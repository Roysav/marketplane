package controller_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
	"github.com/roysav/marketplane/pkg/polymarket"
	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/postgres"
	redisstorage "github.com/roysav/marketplane/pkg/storage/redis"
	"github.com/roysav/marketplane/tests"
)

// polymarket/v1/Order schema — matches the MetaRecord we register in tests.
var orderSchema = map[string]any{
	"type":     "object",
	"required": []any{"active", "tokenId", "side", "price", "size"},
	"properties": map[string]any{
		"active":     map[string]any{"type": "boolean"},
		"tokenId":    map[string]any{"type": "string"},
		"side":       map[string]any{"type": "string"},
		"price":      map[string]any{"type": "string"},
		"size":       map[string]any{"type": "string"},
		"orderType":  map[string]any{"type": "string"},
		"expiration": map[string]any{"type": "string"},
		"deferExec":  map[string]any{"type": "boolean"},
	},
}

type polyTestEnv struct {
	records pb.RecordServiceClient
	ctrl    *controller.PolymarketOrderController
}

func setupPolymarket(t *testing.T, handler http.Handler) *polyTestEnv {
	t.Helper()
	ctx := context.Background()

	pool := tests.Pool(ctx, t)
	rows := postgres.New(pool)

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
	recordSvc.StartMetaRecordSync(ctx, 500*time.Millisecond)

	// Register polymarket/v1/Order MetaRecord so the schema is available.
	_, err = recordSvc.Create(ctx, &record.Record{
		Type: record.MetaRecordType,
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "polymarket-order",
		},
		Spec: map[string]any{
			"group":   "polymarket",
			"version": "v1",
			"kind":    "Order",
			"schema":  orderSchema,
		},
	})
	require.NoError(t, err)

	// gRPC server
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	server.New(recordSvc, logger).Register(grpcServer)
	go grpcServer.Serve(lis)
	t.Cleanup(func() { grpcServer.Stop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	records := pb.NewRecordServiceClient(conn)

	// Mock Polymarket server
	mockSrv := httptest.NewServer(handler)
	t.Cleanup(mockSrv.Close)

	polyCfg := polymarket.Config{
		Host:          mockSrv.URL,
		ChainID:       137,
		Address:       "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		PrivateKey:    "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
		APIKey:        "test-api-key",
		APISecret:     base64.URLEncoding.EncodeToString([]byte("test-secret-key-32bytes-padding!")),
		APIPassphrase: "test-passphrase",
		SignatureType: 1,
		Funder:        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
	}
	polyClient, err := polymarket.NewClient(polyCfg)
	require.NoError(t, err)

	ctrl := controller.NewPolymarketOrderController(controller.PolymarketOrderControllerConfig{
		Records:        records,
		Poly:           polyClient,
		Logger:         logger,
		ResyncInterval: resyncInterval,
	})

	return &polyTestEnv{records: records, ctrl: ctrl}
}

func createPolymarketOrder(t *testing.T, ctx context.Context, client pb.RecordServiceClient, tradespace, name string, spec map[string]any) {
	t.Helper()
	s, err := structpb.NewStruct(spec)
	require.NoError(t, err)

	_, err = client.Create(ctx, &pb.CreateRequest{
		Record: &pb.Record{
			Type: "polymarket/v1/Order",
			ObjectMeta: &pb.ObjectMeta{
				Tradespace: tradespace,
				Name:       name,
			},
			Spec: s,
		},
	})
	require.NoError(t, err)
}

func getOrderStatus(t *testing.T, ctx context.Context, client pb.RecordServiceClient, tradespace, name string) map[string]any {
	t.Helper()
	resp, err := client.Get(ctx, &pb.GetRequest{
		Type:       "polymarket/v1/Order",
		Tradespace: tradespace,
		Name:       name,
	})
	require.NoError(t, err)
	if resp.Record.Status == nil {
		return nil
	}
	return resp.Record.Status.AsMap()
}

func waitForOrderState(t *testing.T, ctx context.Context, client pb.RecordServiceClient, tradespace, name, expected string) {
	t.Helper()
	deadline := time.After(phaseTimeout)
	for {
		status := getOrderStatus(t, ctx, client, tradespace, name)
		if status != nil {
			if state, _ := status["state"].(string); state == expected {
				return
			}
		}
		select {
		case <-deadline:
			status := getOrderStatus(t, ctx, client, tradespace, name)
			var got string
			if status != nil {
				got, _ = status["state"].(string)
			}
			t.Fatalf("timed out waiting for state %q on %s/%s, got %q", expected, tradespace, name, got)
		case <-time.After(pollInterval):
		}
	}
}

// --- Schema validation ---

func TestPolymarketOrder_SchemaValidation(t *testing.T) {
	nopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not call Polymarket API for invalid record")
	})
	env := setupPolymarket(t, nopHandler)
	ctx := context.Background()

	// Missing required field "side"
	spec, err := structpb.NewStruct(map[string]any{
		"active":  true,
		"tokenId": "12345",
		"price":   "0.57",
		"size":    "10",
	})
	require.NoError(t, err)

	_, err = env.records.Create(ctx, &pb.CreateRequest{
		Record: &pb.Record{
			Type: "polymarket/v1/Order",
			ObjectMeta: &pb.ObjectMeta{
				Tradespace: "prod",
				Name:       "bad-order",
			},
			Spec: spec,
		},
	})
	assert.Error(t, err, "should reject record missing required field 'side'")
}

// --- active=false → Draft ---

func TestPolymarketOrder_ActiveFalse_Draft(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Fatal("should not call Polymarket API when active=false")
	})

	env := setupPolymarket(t, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createPolymarketOrder(t, ctx, env.records, "prod", "draft-1", map[string]any{
		"active":  false,
		"tokenId": "12345",
		"side":    "BUY",
		"price":   "0.57",
		"size":    "10",
	})

	go env.ctrl.Run(ctx)

	waitForOrderState(t, ctx, env.records, "prod", "draft-1", "Draft")

	// Verify no API calls were made.
	assert.Equal(t, int64(0), calls.Load())
}

// --- active=true → submits once → stores polymarketOrderID ---

func TestPolymarketOrder_ActiveTrue_Submits(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(polymarket.SendOrderResponse{
			Success:  true,
			OrderID:  "0xorder123",
			Status:   "live",
			ErrorMsg: "",
		})
	})

	env := setupPolymarket(t, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go env.ctrl.Run(ctx)
	time.Sleep(watchStartup)

	createPolymarketOrder(t, ctx, env.records, "prod", "active-1", map[string]any{
		"active":  true,
		"tokenId": "12345",
		"side":    "BUY",
		"price":   "0.01",
		"size":    "5",
	})

	waitForOrderState(t, ctx, env.records, "prod", "active-1", "Live")

	status := getOrderStatus(t, ctx, env.records, "prod", "active-1")
	assert.Equal(t, "0xorder123", status["polymarketOrderID"])

	// Wait for a resync cycle — should NOT submit again.
	time.Sleep(2 * resyncInterval)
	assert.Equal(t, int64(1), calls.Load(), "should submit exactly once")
}

// --- Already submitted → no resubmit ---

func TestPolymarketOrder_AlreadySubmitted_NoResubmit(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(polymarket.SendOrderResponse{
			Success: true,
			OrderID: "0xalready",
			Status:  "live",
		})
	})

	env := setupPolymarket(t, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createPolymarketOrder(t, ctx, env.records, "prod", "resubmit-1", map[string]any{
		"active":  true,
		"tokenId": "12345",
		"side":    "BUY",
		"price":   "0.50",
		"size":    "1",
	})

	go env.ctrl.Run(ctx)

	waitForOrderState(t, ctx, env.records, "prod", "resubmit-1", "Live")
	assert.Equal(t, int64(1), calls.Load())

	// Wait several resync cycles.
	time.Sleep(3 * resyncInterval)
	assert.Equal(t, int64(1), calls.Load(), "should not resubmit after polymarketOrderID is set")
}

// --- API failure → Failed state ---

func TestPolymarketOrder_APIFailure_Failed(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(polymarket.SendOrderResponse{
			Success:  false,
			OrderID:  "0xfailed",
			Status:   "",
			ErrorMsg: "invalid order payload",
		})
	})

	env := setupPolymarket(t, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createPolymarketOrder(t, ctx, env.records, "prod", "fail-1", map[string]any{
		"active":  true,
		"tokenId": "12345",
		"side":    "BUY",
		"price":   "0.50",
		"size":    "1",
	})

	go env.ctrl.Run(ctx)

	waitForOrderState(t, ctx, env.records, "prod", "fail-1", "Failed")

	status := getOrderStatus(t, ctx, env.records, "prod", "fail-1")
	assert.Contains(t, status["message"], "invalid order payload")
	// polymarketOrderID should NOT be set on failure.
	assert.Empty(t, status["polymarketOrderID"])
}
