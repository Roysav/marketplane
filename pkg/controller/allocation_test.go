package controller_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/postgres"
	redisstorage "github.com/roysav/marketplane/pkg/storage/redis"
	"github.com/roysav/marketplane/tests"
)

const (
	phaseApproved  = "approved"
	phaseRejected  = "rejected"
	testTradespace = "prod"
	testCurrency   = "USD"

	resyncInterval = 500 * time.Millisecond
	pollInterval   = 100 * time.Millisecond
	phaseTimeout   = 5 * time.Second
	watchStartup   = 200 * time.Millisecond
)

// testEnv bundles a running gRPC server with connected clients and the
// controller under test. Call setup() to create one per test.
type testEnv struct {
	records pb.RecordServiceClient
	ledger  pb.LedgerServiceClient
	ctrl    *controller.AllocationController
}

func setup(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()

	pool := tests.Pool(ctx, t)
	rows := postgres.New(pool)
	ledgerStorage := postgres.NewLedgerStorage(pool)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	validator, err := record.NewValidator()
	require.NoError(t, err)

	redisClient, err := redisstorage.NewClient(ctx, redisstorage.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}
	t.Cleanup(func() { redisClient.Close() })
	events := redisstorage.NewEventStorage(redisClient)

	recordSvc := service.New(service.Config{
		Rows:      rows,
		Events:    events,
		Validator: validator,
		Logger:    logger,
	})
	ledgerSvc := service.NewLedgerService(ledgerStorage, *recordSvc, logger)

	// Start gRPC server on a random port.
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	server.New(recordSvc, logger).Register(grpcServer)
	server.NewLedgerServer(ledgerSvc, logger).Register(grpcServer)

	go grpcServer.Serve(lis)
	t.Cleanup(func() { grpcServer.Stop() })

	// Connect client.
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	records := pb.NewRecordServiceClient(conn)
	ledger := pb.NewLedgerServiceClient(conn)

	ctrl := controller.NewAllocationController(controller.AllocationControllerConfig{
		Records:        records,
		Ledger:         ledger,
		Logger:         logger,
		ResyncInterval: resyncInterval,
	})

	return &testEnv{records: records, ledger: ledger, ctrl: ctrl}
}

func createAllocationRecord(t *testing.T, ctx context.Context, client pb.RecordServiceClient, tradespace, name, amount, currency string) {
	t.Helper()
	spec, err := structpb.NewStruct(map[string]any{
		"currency":   currency,
		"amount":     amount,
		"targetType": "polymarket/v1/Order",
		"targetName": "order-1",
	})
	require.NoError(t, err)

	_, err = client.Create(ctx, &pb.CreateRequest{
		Record: &pb.Record{
			Type: "core/v1/Allocation",
			ObjectMeta: &pb.ObjectMeta{
				Tradespace: tradespace,
				Name:       name,
			},
			Spec: spec,
		},
	})
	require.NoError(t, err)
}

func getAllocationPhase(t *testing.T, ctx context.Context, client pb.RecordServiceClient, tradespace, name string) string {
	t.Helper()
	resp, err := client.Get(ctx, &pb.GetRequest{
		Type:       "core/v1/Allocation",
		Tradespace: tradespace,
		Name:       name,
	})
	require.NoError(t, err)

	if resp.Record.Status == nil {
		return ""
	}
	phase, _ := resp.Record.Status.AsMap()["phase"].(string)
	return phase
}

func waitForPhase(t *testing.T, ctx context.Context, client pb.RecordServiceClient, tradespace, name, expected string) {
	t.Helper()
	deadline := time.After(phaseTimeout)
	for {
		phase := getAllocationPhase(t, ctx, client, tradespace, name)
		if phase == expected {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for phase %q on %s/%s, got %q", expected, tradespace, name, phase)
		case <-time.After(pollInterval):
		}
	}
}

func TestController_ApproveDeposit(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createAllocationRecord(t, ctx, env.records, testTradespace, "deposit-1", "100", testCurrency)

	go env.ctrl.Run(ctx)

	waitForPhase(t, ctx, env.records, testTradespace, "deposit-1", phaseApproved)

	// Verify ledger entry was created.
	resp, err := env.ledger.List(ctx, &pb.ListLedgerRequest{Tradespace: testTradespace})
	require.NoError(t, err)
	assert.Len(t, resp.Entries, 1)
	assert.Equal(t, "100", resp.Entries[0].Amount)
}

func TestController_RejectInsufficientBalance(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spend without any prior deposit.
	createAllocationRecord(t, ctx, env.records, testTradespace, "spend-1", "-500", testCurrency)

	go env.ctrl.Run(ctx)

	waitForPhase(t, ctx, env.records, testTradespace, "spend-1", phaseRejected)

	// Verify no ledger entry was created.
	resp, err := env.ledger.List(ctx, &pb.ListLedgerRequest{Tradespace: testTradespace})
	require.NoError(t, err)
	assert.Empty(t, resp.Entries)
}

func TestController_DepositThenSpend(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createAllocationRecord(t, ctx, env.records, testTradespace, "fund-1", "200", testCurrency)

	go env.ctrl.Run(ctx)

	waitForPhase(t, ctx, env.records, testTradespace, "fund-1", phaseApproved)

	// Now spend within balance.
	createAllocationRecord(t, ctx, env.records, testTradespace, "spend-2", "-100", testCurrency)

	waitForPhase(t, ctx, env.records, testTradespace, "spend-2", phaseApproved)

	resp, err := env.ledger.List(ctx, &pb.ListLedgerRequest{Tradespace: testTradespace})
	require.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
}

func TestController_WatchApprovesNewAllocation(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start controller first, then create allocation — exercises the watch path.
	go env.ctrl.Run(ctx)
	time.Sleep(watchStartup)

	createAllocationRecord(t, ctx, env.records, testTradespace, "watch-1", "50", "EUR")

	waitForPhase(t, ctx, env.records, testTradespace, "watch-1", phaseApproved)
}

func TestController_ResyncRetriesRejected(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spend with no deposit → rejected.
	createAllocationRecord(t, ctx, env.records, testTradespace, "retry-spend", "-50", testCurrency)

	go env.ctrl.Run(ctx)

	waitForPhase(t, ctx, env.records, testTradespace, "retry-spend", phaseRejected)

	// Now deposit enough to cover the spend.
	createAllocationRecord(t, ctx, env.records, testTradespace, "retry-deposit", "100", testCurrency)

	waitForPhase(t, ctx, env.records, testTradespace, "retry-deposit", phaseApproved)

	// Resync should retry the rejected allocation and approve it this time.
	waitForPhase(t, ctx, env.records, testTradespace, "retry-spend", phaseApproved)
}

func TestController_ResyncSkipsApproved(t *testing.T) {
	env := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createAllocationRecord(t, ctx, env.records, testTradespace, "skip-1", "100", testCurrency)

	go env.ctrl.Run(ctx)

	waitForPhase(t, ctx, env.records, testTradespace, "skip-1", phaseApproved)

	// Wait for at least one resync cycle.
	time.Sleep(2 * resyncInterval)

	// Still only one ledger entry — resync didn't re-append.
	resp, err := env.ledger.List(ctx, &pb.ListLedgerRequest{Tradespace: testTradespace})
	require.NoError(t, err)
	assert.Len(t, resp.Entries, 1)
}
