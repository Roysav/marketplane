package controller

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/redis"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func setupTestServer(t *testing.T) (pb.RecordServiceClient, pb.LedgerServiceClient, func()) {
	t.Helper()
	ctx := context.Background()

	// Create in-memory storage
	rows, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create row storage: %v", err)
	}

	ledger, err := sqlite.NewLedgerStorage(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create ledger storage: %v", err)
	}

	// Connect to Redis for events (required for Watch)
	redisClient, err := redis.NewClient(ctx, redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Skipf("Redis not available, skipping integration test: %v", err)
	}
	events := redis.NewEventStorage(redisClient)

	// Create services
	svc := service.New(service.Config{
		Rows:   rows,
		Events: events,
	})

	// Create gRPC server
	grpcServer := grpc.NewServer()
	server.New(svc, nil).Register(grpcServer)
	server.NewLedgerServer(ledger, nil).Register(grpcServer)

	// Start server on random port
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go grpcServer.Serve(lis)

	// Create client connection
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		rows.Close()
		ledger.Close()
		redisClient.Close()
	}

	return pb.NewRecordServiceClient(conn), pb.NewLedgerServiceClient(conn), cleanup
}

func TestAllocationController_ApproveDeposit(t *testing.T) {
	records, ledger, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start controller in background
	ctrl := NewAllocationController(records, ledger, nil)
	ctrlCtx, ctrlCancel := context.WithCancel(ctx)
	defer ctrlCancel()

	go ctrl.Run(ctrlCtx)

	// Give controller time to start watching
	time.Sleep(100 * time.Millisecond)

	// Create a deposit allocation
	spec, _ := structpb.NewStruct(map[string]any{
		"currency":   "USD",
		"amount":     "1000.00",
		"targetType": "core/v1/Deposit",
		"targetName": "deposit-001",
	})

	createResp, err := records.Create(ctx, &pb.CreateRequest{
		Record: &pb.Record{
			TypeMeta: &pb.TypeMeta{
				Group:   "core",
				Version: "v1",
				Kind:    "Allocation",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       "alloc-deposit-001",
				Tradespace: "test-ns",
			},
			Spec: spec,
		},
	})
	if err != nil {
		t.Fatalf("failed to create allocation: %v", err)
	}

	// Wait for controller to process
	var finalRecord *pb.Record
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)

		getResp, err := records.Get(ctx, &pb.GetRequest{
			Type:       "core/v1/Allocation",
			Tradespace: "test-ns",
			Name:       "alloc-deposit-001",
		})
		if err != nil {
			t.Fatalf("failed to get allocation: %v", err)
		}

		if getResp.Record.Status != nil {
			if phase, ok := getResp.Record.Status.Fields["phase"]; ok {
				if phase.GetStringValue() != "" {
					finalRecord = getResp.Record
					break
				}
			}
		}
	}

	if finalRecord == nil {
		t.Fatal("allocation was not processed by controller")
	}

	// Verify approved
	phase := finalRecord.Status.Fields["phase"].GetStringValue()
	if phase != PhaseApproved {
		msg := finalRecord.Status.Fields["message"].GetStringValue()
		t.Errorf("expected phase %s, got %s (message: %s)", PhaseApproved, phase, msg)
	}

	// Verify ledger entry exists
	listResp, err := ledger.List(ctx, &pb.LedgerListRequest{Tradespace: "test-ns"})
	if err != nil {
		t.Fatalf("failed to list ledger: %v", err)
	}

	if len(listResp.Entries) != 1 {
		t.Errorf("expected 1 ledger entry, got %d", len(listResp.Entries))
	}

	if listResp.Entries[0].Amount != "1000.00" {
		t.Errorf("expected amount 1000.00, got %s", listResp.Entries[0].Amount)
	}

	_ = createResp // silence unused warning
}

func TestAllocationController_RejectInsufficientBalance(t *testing.T) {
	records, ledger, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start controller
	ctrl := NewAllocationController(records, ledger, nil)
	ctrlCtx, ctrlCancel := context.WithCancel(ctx)
	defer ctrlCancel()

	go ctrl.Run(ctrlCtx)
	time.Sleep(100 * time.Millisecond)

	// Try to spend without any deposit - should be rejected
	spec, _ := structpb.NewStruct(map[string]any{
		"currency":   "USD",
		"amount":     "-500.00",
		"targetType": "polymarket/v1/Order",
		"targetName": "order-001",
	})

	_, err := records.Create(ctx, &pb.CreateRequest{
		Record: &pb.Record{
			TypeMeta: &pb.TypeMeta{
				Group:   "core",
				Version: "v1",
				Kind:    "Allocation",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       "alloc-order-001",
				Tradespace: "test-ns",
			},
			Spec: spec,
		},
	})
	if err != nil {
		t.Fatalf("failed to create allocation: %v", err)
	}

	// Wait for controller to process
	var finalRecord *pb.Record
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)

		getResp, err := records.Get(ctx, &pb.GetRequest{
			Type:       "core/v1/Allocation",
			Tradespace: "test-ns",
			Name:       "alloc-order-001",
		})
		if err != nil {
			t.Fatalf("failed to get allocation: %v", err)
		}

		if getResp.Record.Status != nil {
			if phase, ok := getResp.Record.Status.Fields["phase"]; ok {
				if phase.GetStringValue() != "" {
					finalRecord = getResp.Record
					break
				}
			}
		}
	}

	if finalRecord == nil {
		t.Fatal("allocation was not processed by controller")
	}

	// Verify rejected
	phase := finalRecord.Status.Fields["phase"].GetStringValue()
	if phase != PhaseRejected {
		t.Errorf("expected phase %s, got %s", PhaseRejected, phase)
	}

	msg := finalRecord.Status.Fields["message"].GetStringValue()
	if msg != "insufficient balance" {
		t.Errorf("expected message 'insufficient balance', got %s", msg)
	}

	// Verify no ledger entry
	listResp, err := ledger.List(ctx, &pb.LedgerListRequest{Tradespace: "test-ns"})
	if err != nil {
		t.Fatalf("failed to list ledger: %v", err)
	}

	if len(listResp.Entries) != 0 {
		t.Errorf("expected 0 ledger entries, got %d", len(listResp.Entries))
	}
}

func TestAllocationController_FullFlow(t *testing.T) {
	records, ledger, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Start controller
	ctrl := NewAllocationController(records, ledger, nil)
	ctrlCtx, ctrlCancel := context.WithCancel(ctx)
	defer ctrlCancel()

	go ctrl.Run(ctrlCtx)
	time.Sleep(100 * time.Millisecond)

	// Helper to create allocation and wait for result
	createAndWait := func(name string, spec map[string]any) *pb.Record {
		specPb, _ := structpb.NewStruct(spec)
		_, err := records.Create(ctx, &pb.CreateRequest{
			Record: &pb.Record{
				TypeMeta: &pb.TypeMeta{
					Group:   "core",
					Version: "v1",
					Kind:    "Allocation",
				},
				ObjectMeta: &pb.ObjectMeta{
					Name:       name,
					Tradespace: "test-ns",
				},
				Spec: specPb,
			},
		})
		if err != nil {
			t.Fatalf("failed to create allocation %s: %v", name, err)
		}

		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			getResp, _ := records.Get(ctx, &pb.GetRequest{
				Type:       "core/v1/Allocation",
				Tradespace: "test-ns",
				Name:       name,
			})
			if getResp.Record.Status != nil {
				if phase, ok := getResp.Record.Status.Fields["phase"]; ok {
					if phase.GetStringValue() != "" {
						return getResp.Record
					}
				}
			}
		}
		t.Fatalf("allocation %s was not processed", name)
		return nil
	}

	// 1. Deposit 1000
	rec := createAndWait("deposit-1000", map[string]any{
		"currency":   "USD",
		"amount":     "1000.00",
		"targetType": "core/v1/Deposit",
		"targetName": "deposit-001",
	})
	if rec.Status.Fields["phase"].GetStringValue() != PhaseApproved {
		t.Fatalf("deposit should be approved")
	}

	// 2. Spend 600 - should succeed (balance: 400)
	rec = createAndWait("spend-600", map[string]any{
		"currency":   "USD",
		"amount":     "-600.00",
		"targetType": "polymarket/v1/Order",
		"targetName": "order-001",
	})
	if rec.Status.Fields["phase"].GetStringValue() != PhaseApproved {
		t.Fatalf("spend 600 should be approved, got: %s", rec.Status.Fields["message"].GetStringValue())
	}

	// 3. Spend 500 - should fail (balance: 400, need 500)
	rec = createAndWait("spend-500", map[string]any{
		"currency":   "USD",
		"amount":     "-500.00",
		"targetType": "polymarket/v1/Order",
		"targetName": "order-002",
	})
	if rec.Status.Fields["phase"].GetStringValue() != PhaseRejected {
		t.Fatalf("spend 500 should be rejected")
	}

	// 4. Spend 400 - should succeed (balance: 0)
	rec = createAndWait("spend-400", map[string]any{
		"currency":   "USD",
		"amount":     "-400.00",
		"targetType": "polymarket/v1/Order",
		"targetName": "order-003",
	})
	if rec.Status.Fields["phase"].GetStringValue() != PhaseApproved {
		t.Fatalf("spend 400 should be approved, got: %s", rec.Status.Fields["message"].GetStringValue())
	}

	// Verify final ledger state
	listResp, _ := ledger.List(ctx, &pb.LedgerListRequest{Tradespace: "test-ns"})
	if len(listResp.Entries) != 3 {
		t.Errorf("expected 3 ledger entries, got %d", len(listResp.Entries))
	}
}
