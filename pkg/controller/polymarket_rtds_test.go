package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	redisstore "github.com/roysav/marketplane/pkg/storage/redis"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func setupPolymarketTestServer(t *testing.T) (pb.RecordServiceClient, pb.StreamServiceClient, func()) {
	t.Helper()
	ctx := context.Background()

	rows, err := sqlite.New(ctx, fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("failed to create row storage: %v", err)
	}

	redisClient, err := redisstore.NewClient(ctx, redisstore.Options{Addr: "localhost:6379", DB: 13})
	if err != nil {
		rows.Close()
		t.Skipf("Redis not available, skipping integration test: %v", err)
	}
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		rows.Close()
		redisClient.Close()
		t.Fatalf("failed to clear redis state: %v", err)
	}

	events := redisstore.NewEventStorage(redisClient)
	streams := redisstore.NewStreamStorage(redisClient)

	recordSvc := service.New(service.Config{
		Rows:   rows,
		Events: events,
	})
	streamSvc := service.NewStreamService(service.StreamServiceConfig{
		Rows:    rows,
		Streams: streams,
	})

	grpcServer := grpc.NewServer()
	server.New(recordSvc, nil).Register(grpcServer)
	server.NewStreamServer(streamSvc, nil).Register(grpcServer)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		rows.Close()
		redisClient.Close()
		t.Fatalf("failed to listen: %v", err)
	}

	go grpcServer.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		lis.Close()
		grpcServer.Stop()
		rows.Close()
		redisClient.Close()
		t.Fatalf("failed to connect: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		lis.Close()
		rows.Close()
		redisClient.Close()
	}

	return pb.NewRecordServiceClient(conn), pb.NewStreamServiceClient(conn), cleanup
}

func installPolymarketSubscriptionMetaRecord(t *testing.T, records pb.RecordServiceClient) {
	t.Helper()

	spec, err := structpb.NewStruct(map[string]any{
		"group":   "polymarket",
		"version": "v1",
		"kind":    "CryptoSubscription",
		"scope":   "global",
		"schema": map[string]any{
			"type":     "object",
			"required": []any{"venue", "symbol"},
			"properties": map[string]any{
				"venue":   map[string]any{"type": "string", "enum": []any{"binance", "chainlink"}},
				"symbol":  map[string]any{"type": "string"},
				"enabled": map[string]any{"type": "boolean"},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to build MetaRecord spec: %v", err)
	}

	_, err = records.Create(context.Background(), &pb.CreateRequest{
		Record: &pb.Record{
			TypeMeta: &pb.TypeMeta{
				Group:   "core",
				Version: "v1",
				Kind:    "MetaRecord",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       "CryptoSubscription.polymarket",
				Tradespace: "default",
			},
			Spec: spec,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("failed to create MetaRecord: %v", err)
	}
}

func createStreamDefinitionRecord(t *testing.T, records pb.RecordServiceClient, name string) {
	t.Helper()

	spec, err := structpb.NewStruct(map[string]any{
		"group":     "polymarket",
		"version":   "v1",
		"kind":      "CryptoPrice",
		"retention": "24h",
		"schema": map[string]any{
			"type":     "object",
			"required": []any{"venue", "symbol", "value", "source_timestamp_ms", "message_timestamp_ms"},
			"properties": map[string]any{
				"venue":                map[string]any{"type": "string"},
				"symbol":               map[string]any{"type": "string"},
				"value":                map[string]any{"type": "number"},
				"source_timestamp_ms":  map[string]any{"type": "number"},
				"message_timestamp_ms": map[string]any{"type": "number"},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to build StreamDefinition spec: %v", err)
	}

	_, err = records.Create(context.Background(), &pb.CreateRequest{
		Record: &pb.Record{
			TypeMeta: &pb.TypeMeta{
				Group:   "core",
				Version: "v1",
				Kind:    "StreamDefinition",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       name,
				Tradespace: "default",
			},
			Spec: spec,
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("failed to create StreamDefinition %s: %v", name, err)
	}
}

func createCryptoSubscriptionRecord(t *testing.T, records pb.RecordServiceClient, venue, symbol string, enabled bool) string {
	t.Helper()

	streamName, err := streamNameForVenue(venue, symbol)
	if err != nil {
		t.Fatalf("failed to build stream name: %v", err)
	}

	spec, err := structpb.NewStruct(map[string]any{
		"venue":   venue,
		"symbol":  symbol,
		"enabled": enabled,
	})
	if err != nil {
		t.Fatalf("failed to build subscription spec: %v", err)
	}

	_, err = records.Create(context.Background(), &pb.CreateRequest{
		Record: &pb.Record{
			TypeMeta: &pb.TypeMeta{
				Group:   "polymarket",
				Version: "v1",
				Kind:    "CryptoSubscription",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       streamName,
				Tradespace: "default",
			},
			Spec: spec,
		},
	})
	if err != nil {
		t.Fatalf("failed to create subscription %s: %v", streamName, err)
	}

	return streamName
}

type fakeRTDSServer struct {
	server      *httptest.Server
	upgrader    websocket.Upgrader
	subscribeCh chan polymarketSubscribeRequest
	mu          sync.Mutex
	conns       []*websocket.Conn
}

func newFakeRTDSServer(t *testing.T) *fakeRTDSServer {
	t.Helper()

	s := &fakeRTDSServer{
		upgrader:    websocket.Upgrader{},
		subscribeCh: make(chan polymarketSubscribeRequest, 32),
	}

	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()

		go s.readLoop(conn)
	}))

	t.Cleanup(func() {
		s.closeAll()
		s.server.Close()
	})

	return s
}

func (s *fakeRTDSServer) wsURL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func (s *fakeRTDSServer) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if strings.TrimSpace(string(data)) == "PING" {
			continue
		}

		var req polymarketSubscribeRequest
		if err := json.Unmarshal(data, &req); err == nil && req.Action == "subscribe" {
			s.subscribeCh <- req
		}
	}
}

func (s *fakeRTDSServer) waitForSubscribe(t *testing.T) polymarketSubscribeRequest {
	t.Helper()

	select {
	case req := <-s.subscribeCh:
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for RTDS subscribe request")
	}

	return polymarketSubscribeRequest{}
}

func (s *fakeRTDSServer) waitForSubscribeFilters(t *testing.T, want ...string) polymarketSubscribeRequest {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	sort.Strings(want)

	for {
		select {
		case req := <-s.subscribeCh:
			if equalStringSlices(subscriptionFilters(req), want) {
				return req
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for subscribe filters %v", want)
			return polymarketSubscribeRequest{}
		}
	}
}

func subscriptionFilters(req polymarketSubscribeRequest) []string {
	filters := make([]string, 0, len(req.Subscriptions))
	for _, sub := range req.Subscriptions {
		filters = append(filters, sub.Filters)
	}
	sort.Strings(filters)
	return filters
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *fakeRTDSServer) sendJSON(t *testing.T, payload any) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	sent := false
	for _, conn := range s.conns {
		if conn == nil {
			continue
		}
		if err := conn.WriteJSON(payload); err == nil {
			sent = true
		}
	}

	if !sent {
		t.Fatal("no live websocket connection available")
	}
}

func (s *fakeRTDSServer) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, conn := range s.conns {
		if conn != nil {
			conn.Close()
		}
	}
	s.conns = nil
}

func waitForLatestStreamValue(t *testing.T, streams pb.StreamServiceClient, name string) string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := streams.Latest(context.Background(), &pb.LatestRequest{
			Key: &pb.StreamKey{Name: name},
		})
		if err == nil && resp.Entry != nil {
			return resp.Entry.Value
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for latest stream value for %s", name)
	return ""
}

func waitForSubscriptionPhase(t *testing.T, records pb.RecordServiceClient, name, phase string) *pb.Record {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := records.Get(context.Background(), &pb.GetRequest{
			Type:       CryptoSubscriptionTypeStr,
			Tradespace: "default",
			Name:       name,
		})
		if err == nil && resp.Record != nil && resp.Record.Status != nil {
			if got := resp.Record.Status.Fields["phase"].GetStringValue(); got == phase {
				return resp.Record
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for %s to reach phase %s", name, phase)
	return nil
}

func TestPolymarketStreamNameForVenue(t *testing.T) {
	tests := []struct {
		venue    string
		symbol   string
		expected string
		wantErr  bool
	}{
		{venue: "binance", symbol: "BTCUSDT", expected: "polymarket.crypto.binance.btcusdt"},
		{venue: "chainlink", symbol: "ETH/USD", expected: "polymarket.crypto.chainlink.eth_usd"},
		{venue: "chainlink", symbol: "invalid", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.venue+"-"+tt.symbol, func(t *testing.T) {
			got, err := streamNameForVenue(tt.venue, tt.symbol)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Fatalf("streamNameForVenue() = %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestBuildSubscribePayload(t *testing.T) {
	binanceReq, err := buildSubscribePayload(polymarketVenueBinance, []cryptoSubscription{
		{Name: "a", Venue: polymarketVenueBinance, Symbol: "ethusdt"},
		{Name: "b", Venue: polymarketVenueBinance, Symbol: "btcusdt"},
	})
	if err != nil {
		t.Fatalf("buildSubscribePayload(binance) failed: %v", err)
	}
	if len(binanceReq.Subscriptions) != 2 {
		t.Fatalf("unexpected Binance subscribe payload: %+v", binanceReq)
	}
	if got := subscriptionFilters(*binanceReq); !equalStringSlices(got, []string{`{"symbol":"btcusdt"}`, `{"symbol":"ethusdt"}`}) {
		t.Fatalf("unexpected Binance subscribe filters: %v", got)
	}

	chainlinkReq, err := buildSubscribePayload(polymarketVenueChainlink, []cryptoSubscription{
		{Name: "a", Venue: polymarketVenueChainlink, Symbol: "eth/usd"},
	})
	if err != nil {
		t.Fatalf("buildSubscribePayload(chainlink) failed: %v", err)
	}
	if len(chainlinkReq.Subscriptions) != 1 || !strings.Contains(chainlinkReq.Subscriptions[0].Filters, `"symbol":"eth/usd"`) {
		t.Fatalf("unexpected Chainlink subscribe payload: %+v", chainlinkReq)
	}
}

func TestDecodePolymarketRTDSMessage(t *testing.T) {
	msg, err := decodePolymarketRTDSMessage([]byte(`{"topic":"crypto_prices","type":"update","timestamp":1753314088421,"payload":{"symbol":"btcusdt","timestamp":1753314088395,"value":67234.5}}`))
	if err != nil {
		t.Fatalf("decodePolymarketRTDSMessage failed: %v", err)
	}
	if msg.Payload.Symbol != "btcusdt" || msg.Payload.Value != 67234.5 {
		t.Fatalf("unexpected decoded message: %+v", msg)
	}
}

func TestPolymarketRTDSController_BinanceAndChainlinkIngestion(t *testing.T) {
	records, streams, cleanup := setupPolymarketTestServer(t)
	defer cleanup()

	installPolymarketSubscriptionMetaRecord(t, records)

	binanceName, err := streamNameForVenue(polymarketVenueBinance, "btcusdt")
	if err != nil {
		t.Fatal(err)
	}
	chainlinkName, err := streamNameForVenue(polymarketVenueChainlink, "eth/usd")
	if err != nil {
		t.Fatal(err)
	}

	createStreamDefinitionRecord(t, records, binanceName)
	createStreamDefinitionRecord(t, records, chainlinkName)
	createCryptoSubscriptionRecord(t, records, polymarketVenueBinance, "btcusdt", true)
	createCryptoSubscriptionRecord(t, records, polymarketVenueChainlink, "eth/usd", true)

	fakeWS := newFakeRTDSServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctrl := NewPolymarketRTDSController(records, streams, nil)
	ctrl.SetEndpoint(fakeWS.wsURL())
	ctrl.SetPingInterval(50 * time.Millisecond)
	ctrl.SetReconnectDelay(100 * time.Millisecond)
	ctrl.SetStatusWriteInterval(0)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.Run(ctx)
	}()

	subscribeA := fakeWS.waitForSubscribe(t)
	subscribeB := fakeWS.waitForSubscribe(t)
	if subscribeA.Subscriptions[0].Topic == subscribeB.Subscriptions[0].Topic {
		t.Fatalf("expected subscriptions for two venues, got %+v and %+v", subscribeA, subscribeB)
	}

	fakeWS.sendJSON(t, map[string]any{
		"topic":     polymarketTopicBinance,
		"type":      "update",
		"timestamp": 1753314088421,
		"payload": map[string]any{
			"symbol":    "btcusdt",
			"timestamp": 1753314088395,
			"value":     67234.50,
		},
	})
	fakeWS.sendJSON(t, map[string]any{
		"topic":     polymarketTopicChainlink,
		"type":      "update",
		"timestamp": 1753314064237,
		"payload": map[string]any{
			"symbol":    "eth/usd",
			"timestamp": 1753314064213,
			"value":     3456.78,
		},
	})

	binanceValue := waitForLatestStreamValue(t, streams, binanceName)
	chainlinkValue := waitForLatestStreamValue(t, streams, chainlinkName)
	if !strings.Contains(binanceValue, `"venue":"binance"`) {
		t.Fatalf("unexpected Binance stream payload: %s", binanceValue)
	}
	if !strings.Contains(chainlinkValue, `"venue":"chainlink"`) {
		t.Fatalf("unexpected Chainlink stream payload: %s", chainlinkValue)
	}

	binanceRecord := waitForSubscriptionPhase(t, records, binanceName, PhaseActive)
	chainlinkRecord := waitForSubscriptionPhase(t, records, chainlinkName, PhaseActive)
	if binanceRecord.Status.Fields["lastMessageAt"].GetStringValue() == "" {
		t.Fatalf("expected Binance lastMessageAt to be set")
	}
	if chainlinkRecord.Status.Fields["lastMessageAt"].GetStringValue() == "" {
		t.Fatalf("expected Chainlink lastMessageAt to be set")
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("controller returned error: %v", err)
	}
}

func TestPolymarketRTDSController_ResubscribesOnConfigChange(t *testing.T) {
	records, streams, cleanup := setupPolymarketTestServer(t)
	defer cleanup()

	_ = streams
	installPolymarketSubscriptionMetaRecord(t, records)

	btcName, err := streamNameForVenue(polymarketVenueBinance, "btcusdt")
	if err != nil {
		t.Fatal(err)
	}
	ethName, err := streamNameForVenue(polymarketVenueBinance, "ethusdt")
	if err != nil {
		t.Fatal(err)
	}

	createStreamDefinitionRecord(t, records, btcName)
	createStreamDefinitionRecord(t, records, ethName)
	createCryptoSubscriptionRecord(t, records, polymarketVenueBinance, "btcusdt", true)

	fakeWS := newFakeRTDSServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctrl := NewPolymarketRTDSController(records, streams, nil)
	ctrl.SetEndpoint(fakeWS.wsURL())
	ctrl.SetPingInterval(50 * time.Millisecond)
	ctrl.SetReconnectDelay(100 * time.Millisecond)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.Run(ctx)
	}()

	first := fakeWS.waitForSubscribe(t)
	if got := subscriptionFilters(first); !equalStringSlices(got, []string{`{"symbol":"btcusdt"}`}) {
		t.Fatalf("expected first Binance subscribe to contain only btcusdt, got %v", got)
	}

	createCryptoSubscriptionRecord(t, records, polymarketVenueBinance, "ethusdt", true)

	second := fakeWS.waitForSubscribeFilters(t, `{"symbol":"btcusdt"}`, `{"symbol":"ethusdt"}`)
	if got := subscriptionFilters(second); !equalStringSlices(got, []string{`{"symbol":"btcusdt"}`, `{"symbol":"ethusdt"}`}) {
		t.Fatalf("expected resubscribe to contain btcusdt and ethusdt, got %v", got)
	}

	_, err = records.Delete(context.Background(), &pb.DeleteRequest{
		Type:       CryptoSubscriptionTypeStr,
		Tradespace: "default",
		Name:       btcName,
	})
	if err != nil {
		t.Fatalf("failed to delete btc subscription: %v", err)
	}

	third := fakeWS.waitForSubscribeFilters(t, `{"symbol":"ethusdt"}`)
	if got := subscriptionFilters(third); !equalStringSlices(got, []string{`{"symbol":"ethusdt"}`}) {
		t.Fatalf("expected resubscribe to contain only ethusdt, got %v", got)
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("controller returned error: %v", err)
	}
}

func TestPolymarketRTDSController_ReconnectsAfterSocketClose(t *testing.T) {
	records, streams, cleanup := setupPolymarketTestServer(t)
	defer cleanup()

	_ = streams
	installPolymarketSubscriptionMetaRecord(t, records)

	btcName := createCryptoSubscriptionRecord(t, records, polymarketVenueBinance, "btcusdt", true)
	createStreamDefinitionRecord(t, records, btcName)

	fakeWS := newFakeRTDSServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctrl := NewPolymarketRTDSController(records, streams, nil)
	ctrl.SetEndpoint(fakeWS.wsURL())
	ctrl.SetPingInterval(50 * time.Millisecond)
	ctrl.SetReconnectDelay(100 * time.Millisecond)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.Run(ctx)
	}()

	first := fakeWS.waitForSubscribe(t)
	if got := subscriptionFilters(first); !equalStringSlices(got, []string{`{"symbol":"btcusdt"}`}) {
		t.Fatalf("expected initial Binance subscribe to contain btcusdt, got %v", got)
	}

	fakeWS.closeAll()

	second := fakeWS.waitForSubscribeFilters(t, `{"symbol":"btcusdt"}`)
	if got := subscriptionFilters(second); !equalStringSlices(got, []string{`{"symbol":"btcusdt"}`}) {
		t.Fatalf("expected reconnect subscribe to contain btcusdt, got %v", got)
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("controller returned error: %v", err)
	}
}

func TestPolymarketRTDSController_FailsStartupWhenDefinitionMissing(t *testing.T) {
	records, streams, cleanup := setupPolymarketTestServer(t)
	defer cleanup()

	installPolymarketSubscriptionMetaRecord(t, records)
	createCryptoSubscriptionRecord(t, records, polymarketVenueBinance, "btcusdt", true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := NewPolymarketRTDSController(records, streams, nil)
	err := ctrl.Run(ctx)
	if err == nil {
		t.Fatal("expected startup error, got nil")
	}
	if !strings.Contains(err.Error(), "missing StreamDefinition") {
		t.Fatalf("expected missing StreamDefinition error, got %v", err)
	}
}
