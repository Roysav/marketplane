package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/GoPolymarket/polymarket-go-sdk"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/auth"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/clob"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/clob/clobtypes"
)

// NO outcome of market "will-jesus-christ-return-before-2027".
// Looked up once from https://gamma-api.polymarket.com/markets?slug=...
const liveNoTokenID = "51797157743046504218541616681751597845468055908324407922581755135522797852101"

// liveTestOrder is the standard small test order used by every live test:
// 5 shares of NO at 1¢, GTC. Cheap, far from market, safe to leave hanging
// briefly before cancellation.
var liveTestOrder = OrderRequest{
	AssetID:   liveNoTokenID,
	Side:      SideBuy,
	Price:     "0.01",
	Size:      "5.",
	OrderType: OrderTypeGTC,
}

// newLiveClient builds an authenticated SDKClient from the process env and
// registers a cleanup. Tests get a 30s context derived from the test context.
func newLiveClient(t *testing.T) (*SDKClient, context.Context) {
	t.Helper()
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Logger = slog.Default()

	client, err := NewSDKClient(t.Context(), cfg)
	if err != nil {
		t.Fatalf("new sdk client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return client, ctx
}

// placeLiveTestOrder submits liveTestOrder and registers a best-effort cancel
// cleanup so escaped orders don't pile up in the account.
func placeLiveTestOrder(t *testing.T, ctx context.Context, client *SDKClient) *OrderResponse {
	t.Helper()
	resp, err := client.CreateOrder(ctx, liveTestOrder)
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	if resp.OrderID == "" {
		t.Fatalf("create order returned empty id: %+v", resp)
	}
	t.Cleanup(func() { _ = client.CancelOrder(context.Background(), resp.OrderID) })
	return resp
}

func TestSDKDirectly(t *testing.T) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	signer, err := auth.NewPrivateKeySigner(cfg.PrivateKey, cfg.ChainID)
	if err != nil {
		t.Fatal("create private-key signer", "error", err)
	}
	creds := &auth.APIKey{
		Key:        cfg.APIKey,
		Secret:     cfg.APISecret,
		Passphrase: cfg.APIPassphrase,
	}

	client, err := polymarket.NewClientE()
	if err != nil {
		t.Fatal("initialize client", "error", err)
	}
	client.CLOB = client.CLOB.WithAuth(signer, creds)
	client.CLOB = client.CLOB.WithSignatureType(auth.SignatureProxy)
	apiKey, err := client.CLOB.CreateOrDeriveAPIKey(t.Context())
	if err != nil {
		t.Fatal("deriving api key", "error", err)
	}
	client = client.WithAuth(signer, &auth.APIKey{Key: apiKey.APIKey, Secret: apiKey.Secret, Passphrase: apiKey.Passphrase})

	order, err := clob.NewOrderBuilder(client.CLOB, signer).
		TokenID(liveNoTokenID).
		Side(string(SideBuy)).
		Price(0.01).
		Size(5.).
		OrderType(clobtypes.OrderTypeGTC).Build()
	if err != nil {
		t.Fatal("build order:", err)
	}

	resp, err := client.CLOB.CreateOrder(t.Context(), order)
	if err != nil {
		t.Fatal("Order failed:", err)
	}
	fmt.Printf("Order Placed: %s\n", resp.ID)
}

func TestSDKClient_CreateOrder_Live(t *testing.T) {
	client, ctx := newLiveClient(t)
	resp := placeLiveTestOrder(t, ctx, client)

	raw, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	t.Logf("order submitted:\n%s", raw)
}

func TestSDKClient_CancelOrder_Live(t *testing.T) {
	client, ctx := newLiveClient(t)
	resp := placeLiveTestOrder(t, ctx, client)
	t.Logf("created order id=%s status=%s", resp.OrderID, resp.Status)

	if err := client.CancelOrder(ctx, resp.OrderID); err != nil {
		t.Fatalf("cancel order %s: %v", resp.OrderID, err)
	}
	t.Logf("canceled order id=%s", resp.OrderID)
}

func TestSDKClient_ListOpenOrders_Live(t *testing.T) {
	client, ctx := newLiveClient(t)
	created := placeLiveTestOrder(t, ctx, client)

	time.Sleep(5 * time.Second)
	orders, err := client.ListOpenOrders(ctx)
	if err != nil {
		t.Fatalf("list open orders: %v", err)
	}
	t.Logf("open orders: %d", len(orders))

	var found bool
	for _, o := range orders {
		if o.ID == created.OrderID {
			found = true
		}
		t.Logf("  id=%s status=%s side=%s price=%s size=%s/%s asset=%s",
			o.ID, o.Status, o.Side, o.Price, o.SizeMatched, o.OriginalSize, o.AssetID)
	}
	if !found {
		t.Fatalf("just-created order %s not present in ListOpenOrders", created.OrderID)
	}
}

func TestSDKClient_SubscribeUserOrders_Live(t *testing.T) {
	client, ctx := newLiveClient(t)

	stream, err := client.SubscribeUser(ctx)
	if err != nil {
		t.Fatalf("subscribe user: %v", err)
	}
	events := stream.Orders

	// Let the WS handshake settle before placing so we don't race PLACEMENT.
	//time.Sleep(2 * time.Second)

	created := placeLiveTestOrder(t, ctx, client)
	t.Logf("placed order id=%s, waiting for WS event...", created.OrderID)

	deadline := time.After(20 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("WS channel closed before order event arrived")
			}
			raw, _ := json.MarshalIndent(ev, "", "  ")
			t.Logf("WS event:\n%s", raw)
			if ev.ID == created.OrderID {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for WS event for order %s", created.OrderID)
		}
	}
}
