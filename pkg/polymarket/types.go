// Package polymarket provides a thin adapter around the community
// GoPolymarket/polymarket-go-sdk so Marketplane controllers can depend on a
// small, testable interface rather than the SDK's concrete types.
//
// Everything in this package is intentionally pure data. The SDK adapter
// lives in sdk.go; fakes used by tests live in fake.go.
package polymarket

import "time"

// Side is a Polymarket order side.
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// OrderType mirrors the Polymarket CLOB order types.
type OrderType string

const (
	OrderTypeGTC OrderType = "GTC"
	OrderTypeGTD OrderType = "GTD"
	OrderTypeFOK OrderType = "FOK"
	OrderTypeFAK OrderType = "FAK"
)

// OrderEventKind mirrors the raw Polymarket `event_type` values on the
// user-channel order stream.
type OrderEventKind string

const (
	OrderEventPlacement    OrderEventKind = "PLACEMENT"
	OrderEventUpdate       OrderEventKind = "UPDATE"
	OrderEventCancellation OrderEventKind = "CANCELLATION"
)

// TradeStatus mirrors the raw Polymarket trade lifecycle values.
type TradeStatus string

const (
	TradeMatched   TradeStatus = "MATCHED"
	TradeMined     TradeStatus = "MINED"
	TradeConfirmed TradeStatus = "CONFIRMED"
	TradeRetrying  TradeStatus = "RETRYING"
	TradeFailed    TradeStatus = "FAILED"
)

// OrderRequest is the controller-facing order submission payload. The adapter
// is responsible for filling in fee rate, tick size, nonce, salt, signature
// type, funder, signer, and signing the order before hitting POST /order.
type OrderRequest struct {
	AssetID    string
	Side       Side
	Price      string // decimal string
	Size       string // decimal string
	OrderType  OrderType
	Expiration int64 // unix seconds; 0 for none
}

// OrderResponse is the subset of the POST /order response the controller
// needs. `Raw` carries the original response for observability.
type OrderResponse struct {
	OrderID           string
	Status            string // raw polymarket status, e.g. "live", "matched", "delayed", "unmatched"
	MakingAmount      string
	TakingAmount      string
	TradeIDs          []string
	TransactionHashes []string
	ErrorMsg          string
	Raw               map[string]any
}

// RemoteOrder is a materialized view of GET /data/orders or GET /order.
type RemoteOrder struct {
	ID             string
	Status         string
	Owner          string
	MakerAddress   string
	Market         string
	AssetID        string
	Side           Side
	OriginalSize   string
	SizeMatched    string
	Price          string
	Outcome        string
	OrderType      OrderType
	Expiration     int64
	AssociateTrade []string
	CreatedAt      time.Time
	Raw            map[string]any
}

// RemoteTrade is a materialized view of GET /trades entries or WS trade
// events.
type RemoteTrade struct {
	TradeID         string
	OrderID         string
	Market          string
	AssetID         string
	Side            Side
	Price           string
	Size            string
	MakerAddress    string
	Status          TradeStatus
	TransactionHash string
	MatchTime       time.Time
	Raw             map[string]any
}

// UserEvent is the normalized form of the two kinds of events the controller
// consumes: order-lifecycle events and trade-lifecycle events. Exactly one of
// Order or Trade is set.
type UserEvent struct {
	Kind  OrderEventKind
	Order *RemoteOrder
	Trade *RemoteTrade
	Time  time.Time
}

// MarketMeta carries the metadata required to build a signed order.
type MarketMeta struct {
	TokenID    string
	Market     string  // condition ID
	FeeRateBps int64
	TickSize   string
	NegRisk    bool
}
