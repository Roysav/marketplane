package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// OrderWSEvent mirrors the user-channel `order` payload from
// pkg/polymarket/polymarket-wss-reference.md. Numeric fields stay as the raw
// string the server sends; the controller parses what it needs.
type OrderWSEvent struct {
	EventType       string   `json:"event_type"`
	ID              string   `json:"id"`
	Owner           string   `json:"owner"`
	OrderOwner      string   `json:"order_owner,omitempty"`
	Market          string   `json:"market"`
	AssetID         string   `json:"asset_id"`
	Side            string   `json:"side"`
	OriginalSize    string   `json:"original_size"`
	SizeMatched     string   `json:"size_matched"`
	Price           string   `json:"price"`
	AssociateTrades []string `json:"associate_trades,omitempty"`
	Outcome         string   `json:"outcome,omitempty"`
	Type            string   `json:"type"`
	CreatedAt       string   `json:"created_at,omitempty"`
	Expiration      string   `json:"expiration,omitempty"`
	OrderType       string   `json:"order_type,omitempty"`
	Status          string   `json:"status,omitempty"`
	MakerAddress    string   `json:"maker_address,omitempty"`
	Timestamp       string   `json:"timestamp"`
}

// TradeWSEvent mirrors the user-channel `trade` payload.
type TradeWSEvent struct {
	EventType       string            `json:"event_type"`
	Type            string            `json:"type"`
	ID              string            `json:"id"`
	TakerOrderID    string            `json:"taker_order_id"`
	Market          string            `json:"market"`
	AssetID         string            `json:"asset_id"`
	Side            string            `json:"side"`
	Size            string            `json:"size"`
	Price           string            `json:"price"`
	FeeRateBps      string            `json:"fee_rate_bps,omitempty"`
	Status          string            `json:"status"`
	MatchTime       string            `json:"matchtime,omitempty"`
	LastUpdate      string            `json:"last_update,omitempty"`
	Outcome         string            `json:"outcome,omitempty"`
	Owner           string            `json:"owner"`
	TradeOwner      string            `json:"trade_owner,omitempty"`
	MakerAddress    string            `json:"maker_address,omitempty"`
	TransactionHash string            `json:"transaction_hash,omitempty"`
	BucketIndex     int               `json:"bucket_index"`
	MakerOrders     []TradeMakerOrder `json:"maker_orders,omitempty"`
	TraderSide      string            `json:"trader_side,omitempty"`
	Timestamp       string            `json:"timestamp"`
}

// TradeMakerOrder is one entry in a TradeWSEvent's maker_orders list.
type TradeMakerOrder struct {
	OrderID       string `json:"order_id"`
	Owner         string `json:"owner"`
	MakerAddress  string `json:"maker_address,omitempty"`
	MatchedAmount string `json:"matched_amount"`
	Price         string `json:"price"`
	FeeRateBps    string `json:"fee_rate_bps,omitempty"`
	AssetID       string `json:"asset_id"`
	Outcome       string `json:"outcome,omitempty"`
	Side          string `json:"side"`
}

// UserStream bundles the two event channels delivered by a single
// authenticated /ws/user connection.
type UserStream struct {
	Orders <-chan OrderWSEvent
	Trades <-chan TradeWSEvent
}

const userWSPingInterval = 10 * time.Second

// userWSClient owns the authenticated /ws/user connection. One connection
// delivers both order and trade events; consumers read from Orders/Trades.
type userWSClient struct {
	conn   *websocket.Conn
	logger *slog.Logger

	orders chan OrderWSEvent
	trades chan TradeWSEvent

	writeMu   sync.Mutex
	closeOnce sync.Once
	closed    chan struct{}
}

// dialUserWS connects to <baseURL>/ws/user, sends the authenticated subscribe
// frame, and starts the read + ping goroutines. Caller must invoke Close to
// release the connection.
func dialUserWS(ctx context.Context, baseURL, apiKey, secret, passphrase string, logger *slog.Logger) (*userWSClient, error) {
	url := strings.TrimRight(baseURL, "/") + "/ws/user"

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, http.Header{})
	if err != nil {
		return nil, fmt.Errorf("polymarket: dial %s: %w", url, err)
	}

	sub := map[string]any{
		"type": "user",
		"auth": map[string]string{
			"apiKey":     apiKey,
			"secret":     secret,
			"passphrase": passphrase,
		},
		"markets": []string{},
	}
	jsonMessage, err := json.Marshal(sub)
	if err != nil {
		return nil, err
	}
	if err := conn.WriteMessage(websocket.TextMessage, jsonMessage); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("polymarket: send user subscribe: %w", err)
	}
	c := &userWSClient{
		conn:   conn,
		logger: logger.With("component", "polymarket-user-ws"),
		orders: make(chan OrderWSEvent, 64),
		trades: make(chan TradeWSEvent, 64),
		closed: make(chan struct{}),
	}

	go c.readLoop()
	go c.pingLoop()
	return c, nil
}

// Close stops the read/ping goroutines and closes the underlying connection.
// Safe to call multiple times.
func (c *userWSClient) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.conn.Close()
	})
	return nil
}

func (c *userWSClient) readLoop() {
	defer close(c.orders)
	defer close(c.trades)
	for {
		select {
		case <-c.closed:
			return
		default:
		}
		msgType, data, err := c.conn.ReadMessage()
		if err != nil {
			select {
			case <-c.closed:
			default:
				c.logger.Error("user ws read failed", "error", err)
			}
			return
		}
		if msgType == websocket.TextMessage {
			msg := string(data)
			if msg == "PONG" || msg == "PING" {
				continue
			}
		}
		var probe struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			c.logger.Error("user ws decode probe failed", "error", err, "raw", string(data))
			continue
		}
		switch probe.EventType {
		case "order":
			var ev OrderWSEvent
			if err := json.Unmarshal(data, &ev); err != nil {
				c.logger.Error("user ws decode order failed", "error", err)
				continue
			}
			select {
			case c.orders <- ev:
			case <-c.closed:
				return
			}
		case "trade":
			var ev TradeWSEvent
			if err := json.Unmarshal(data, &ev); err != nil {
				c.logger.Error("user ws decode trade failed", "error", err)
				continue
			}
			select {
			case c.trades <- ev:
			case <-c.closed:
				return
			}
		default:
			c.logger.Error("user ws unknown event_type", "event_type", probe.EventType, "raw", string(data))
		}
	}
}

func (c *userWSClient) pingLoop() {
	t := time.NewTicker(userWSPingInterval)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			c.writeMu.Lock()
			err := c.conn.WriteMessage(websocket.TextMessage, []byte("PING"))
			c.writeMu.Unlock()
			if err != nil {
				c.logger.Warn("user ws ping failed", "error", err)
				_ = c.Close()
				return
			}
		}
	}
}
