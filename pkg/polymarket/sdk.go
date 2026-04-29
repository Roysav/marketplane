package polymarket

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	polymarketsdk "github.com/GoPolymarket/polymarket-go-sdk"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/auth"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/clob"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/clob/clobtypes"
)

// Client is the surface the polymarket-order-controller depends on. Each
// method maps to one Polymarket REST endpoint or WS stream. Scheduling,
// caching, and diffing are the controller's responsibility, not this
// adapter's.
type Client interface {
	// MakerAddress returns the maker/funder address used to scope REST trade
	// queries. The controller needs this for GET /trades backfill.
	MakerAddress() string

	// CreateOrder signs and submits a new order (POST /order).
	CreateOrder(ctx context.Context, req OrderRequest) (*OrderResponse, error)

	// CancelOrder cancels a single order by ID (DELETE /order).
	CancelOrder(ctx context.Context, orderID string) error

	// GetOrder fetches a single order by ID. Returns (nil, nil) if the order
	// is no longer visible.
	GetOrder(ctx context.Context, orderID string) (*RemoteOrder, error)

	// ListOpenOrders paginates GET /data/orders.
	ListOpenOrders(ctx context.Context) ([]RemoteOrder, error)

	// ListTrades paginates GET /trades for the configured maker address.
	ListTrades(ctx context.Context) ([]RemoteTrade, error)

	// SubscribeUser opens the authenticated user channel. One connection feeds
	// both order and trade event streams; the returned channels are closed
	// when ctx is done or the stream ends.
	SubscribeUser(ctx context.Context) (*UserStream, error)

	// Close releases any resources (HTTP clients, WS connections, heartbeat
	// goroutines) held by the client.
	Close() error
}

// Config is the env-driven configuration documented in
// docs/polymarket-orders-controller.md.
type Config struct {
	Host          string
	WSSURL        string
	ChainID       int64
	Address       string
	MakerAddress  string
	PrivateKey    string
	APIKey        string
	APISecret     string
	APIPassphrase string
	SignatureType int
	Funder        string

	// HeartbeatInterval controls the L2 heartbeat cadence. Zero disables it.
	HeartbeatInterval time.Duration

	Logger *slog.Logger
}

// ConfigFromEnv builds a Config from the documented POLY_* env variables.
// Every listed variable is required; unset or malformed values return an
// error rather than silently falling back to a zero value.
func ConfigFromEnv() (Config, error) {
	cfg := Config{}

	strVars := []struct {
		name string
		dst  *string
	}{
		{"POLY_HOST", &cfg.Host},
		{"POLY_WSS_URL", &cfg.WSSURL},
		{"POLY_ADDRESS", &cfg.Address},
		{"POLY_MAKER_ADDRESS", &cfg.MakerAddress},
		{"POLY_PRIVATE_KEY", &cfg.PrivateKey},
		{"POLY_API_KEY", &cfg.APIKey},
		{"POLY_API_SECRET", &cfg.APISecret},
		{"POLY_API_PASSPHRASE", &cfg.APIPassphrase},
		{"POLY_FUNDER", &cfg.Funder},
	}
	var missing []string
	for _, v := range strVars {
		val, ok := os.LookupEnv(v.name)
		if !ok || val == "" {
			missing = append(missing, v.name)
			continue
		}
		*v.dst = val
	}

	chainRaw, ok := os.LookupEnv("POLY_CHAIN_ID")
	if !ok || chainRaw == "" {
		missing = append(missing, "POLY_CHAIN_ID")
	} else {
		n, err := strconv.ParseInt(chainRaw, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("POLY_CHAIN_ID %q: %w", chainRaw, err)
		}
		cfg.ChainID = n
	}

	sigRaw, ok := os.LookupEnv("POLY_SIGNATURE_TYPE")
	if !ok || sigRaw == "" {
		missing = append(missing, "POLY_SIGNATURE_TYPE")
	} else {
		n, err := strconv.Atoi(sigRaw)
		if err != nil {
			return Config{}, fmt.Errorf("POLY_SIGNATURE_TYPE %q: %w", sigRaw, err)
		}
		cfg.SignatureType = n
	}

	if len(missing) > 0 {
		return Config{}, fmt.Errorf("polymarket: missing required env vars: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}

// SDKClient is the default Client implementation backed by the community
// GoPolymarket SDK.
type SDKClient struct {
	cfg    Config
	logger *slog.Logger
	sdk    *polymarketsdk.Client
	maker  string
	signer *auth.PrivateKeySigner

	userWSMu sync.Mutex
	userWS   *userWSClient
	creds    *auth.APIKey
}

// NewSDKClient constructs an SDKClient. It does not start any goroutines; the
// controller owns the lifecycle via Close.
func NewSDKClient(ctx context.Context, cfg Config) (*SDKClient, error) {
	signer, err := auth.NewPrivateKeySigner(cfg.PrivateKey, cfg.ChainID)
	if err != nil {
		return nil, err
	}
	creds := &auth.APIKey{
		Key:        cfg.APIKey,
		Secret:     cfg.APISecret,
		Passphrase: cfg.APIPassphrase,
	}

	client, err := polymarketsdk.NewClientE()
	if err != nil {
		return nil, err
	}
	client.CLOB = client.CLOB.WithAuth(signer, creds)
	client.CLOB = client.CLOB.WithSignatureType(auth.SignatureProxy)
	apiKeyResponse, err := client.CLOB.CreateOrDeriveAPIKey(ctx)
	apiKey := &auth.APIKey{Key: apiKeyResponse.APIKey, Secret: apiKeyResponse.Secret, Passphrase: apiKeyResponse.Passphrase}
	if err != nil {
		return nil, err
	}
	client = client.WithAuth(signer, apiKey)

	return &SDKClient{
		cfg:    cfg,
		logger: cfg.Logger.With("component", "polymarket-sdk"),
		sdk:    client,
		creds:  apiKey,
		maker:  cfg.MakerAddress,
		signer: signer,
	}, nil
}

// SubscribeUser opens the authenticated user channel and returns the order
// and trade event streams that share the underlying connection.
func (c *SDKClient) SubscribeUser(ctx context.Context) (*UserStream, error) {
	c.userWSMu.Lock()
	defer c.userWSMu.Unlock()
	if c.userWS == nil {
		u, err := dialUserWS(ctx, c.cfg.WSSURL, c.creds.Key, c.creds.Secret, c.creds.Passphrase, c.logger)
		if err != nil {
			return nil, err
		}
		c.userWS = u
	}
	return &UserStream{Orders: c.userWS.orders, Trades: c.userWS.trades}, nil
}

// MakerAddress returns the configured maker/funder address.
func (c *SDKClient) MakerAddress() string { return c.maker }

// CreateOrder builds a signed order via the SDK order builder and submits it.
func (c *SDKClient) CreateOrder(ctx context.Context, req OrderRequest) (*OrderResponse, error) {
	price, err := strconv.ParseFloat(req.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("polymarket: invalid price %q: %w", req.Price, err)
	}
	size, err := strconv.ParseFloat(req.Size, 64)
	if err != nil {
		return nil, fmt.Errorf("polymarket: invalid size %q: %w", req.Size, err)
	}

	builder := clob.NewOrderBuilder(c.sdk.CLOB, c.signer).
		TokenID(req.AssetID).
		Side(string(req.Side)).
		Price(price).
		Size(size).
		OrderType(clobtypes.OrderType(req.OrderType))
	if req.Expiration > 0 {
		builder = builder.ExpirationUnix(req.Expiration)
	}

	order, err := builder.BuildWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("polymarket: build order: %w", err)
	}

	resp, err := c.sdk.CLOB.CreateOrder(ctx, order)
	if err != nil {
		return nil, normalizeSDKError(err)
	}
	return mapOrderResponse(resp), nil
}

// CancelOrder cancels a single order by its Polymarket order ID.
func (c *SDKClient) CancelOrder(ctx context.Context, orderID string) error {
	if _, err := c.sdk.CLOB.CancelOrder(ctx, &clobtypes.CancelOrderRequest{OrderID: orderID}); err != nil {
		return normalizeSDKError(err)
	}
	return nil
}

// GetOrder fetches a single order by its Polymarket order ID.
func (c *SDKClient) GetOrder(ctx context.Context, orderID string) (*RemoteOrder, error) {
	resp, err := c.sdk.CLOB.Order(ctx, orderID)
	if err != nil {
		return nil, normalizeSDKError(err)
	}
	return mapRemoteOrder(resp)
}

// ListOpenOrders paginates GET /data/orders.
func (c *SDKClient) ListOpenOrders(ctx context.Context) ([]RemoteOrder, error) {
	all, err := c.sdk.CLOB.OrdersAll(ctx, &clobtypes.OrdersRequest{})
	if err != nil {
		return nil, normalizeSDKError(err)
	}
	out := make([]RemoteOrder, 0, len(all))
	for _, o := range all {
		ro, err := mapRemoteOrder(o)
		if err != nil {
			return nil, err
		}
		out = append(out, *ro)
	}
	return out, nil
}

// ListTrades paginates GET /trades.
func (c *SDKClient) ListTrades(ctx context.Context) ([]RemoteTrade, error) {
	trades, err := c.sdk.CLOB.TradesAll(ctx, &clobtypes.TradesRequest{})
	if err != nil {
		return nil, normalizeSDKError(err)
	}
	out := make([]RemoteTrade, 0, len(trades))
	for _, t := range trades {
		out = append(out, *mapRemoteTrade(t))
	}
	return out, nil
}

// Close releases any SDK resources.
func (c *SDKClient) Close() error {
	if c.sdk.CLOB != nil {
		c.sdk.CLOB.StopHeartbeats()
	}
	c.userWSMu.Lock()
	u := c.userWS
	c.userWS = nil
	c.userWSMu.Unlock()
	if u != nil {
		_ = u.Close()
	}
	return nil
}

func normalizeSDKError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("polymarket sdk: %w", err)
}
