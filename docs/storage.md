**Storage types:**
- **Row** - persistent database row, atomic operations, use cases: storing transaction data.
- **Stream** (Redis TimeSeries) - real-time timeseries data, no atomicity premises
- **Event** (Redis Streams) - message queue for change events
- **Ledger** - append-only fund ledger with atomic balance checks

# Row Storage

Atomic stable storage for important things like transactions, allocations, quotas etc.
Row storage is decoupled from business logic - it doesn't know about Spec/Status.

from `./pkg/storage/storage.go`
```go
type Row struct {
	Type       string // e.g. "core/v1/Tradespace"
	Tradespace string
	Name       string
	Labels     map[string]string
	Data       string // JSON blob (contains spec/status)

	ResourceVersion int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
```
Filtering is limited to Type, Tradespace, and Labels. No JSON field filtering - by design.

# Stream (Timeseries)

Handles real-time data like ticker prices, quotes, and other timeseries data. Uses Redis TimeSeries for storage.

from `./pkg/storage/stream.go`
```go
type StreamEntry struct {
	Key       string
	Value     string // JSON blob
	Timestamp time.Time
}

type StreamStorage interface {
	Add(ctx context.Context, key string, ts time.Time, value string) error
	Latest(ctx context.Context, key string) (*StreamEntry, error)
	Range(ctx context.Context, key string, from, to time.Time) ([]*StreamEntry, error)
	Watch(ctx context.Context, prefix string) (<-chan WatchEvent, error)
	Close() error
}
```

**Use cases:** Ticker prices, continuous quota updates, any data that changes frequently and needs historical access.

# Event (Message Queue)

Handles event-driven messaging. Append-only log of events that can be consumed by subscribers. Uses Redis Streams.

from `./pkg/storage/event.go`
```go
type Event struct {
	ID    string // Redis stream ID e.g., "1234567890123-0"
	Topic string
	Data  string // JSON blob
}

type EventStorage interface {
	Publish(ctx context.Context, topic string, data string) (eventID string, err error)
	Subscribe(ctx context.Context, topic string) (<-chan Event, error)
	SubscribeFrom(ctx context.Context, topic string, fromID string) (<-chan Event, error)
	Close() error
}
```

**Use cases:** Record change notifications, order events, system events.

# Ledger Storage

Manages an append-only ledger of approved fund allocations. Provides atomic balance checks to prevent overdrafts.

from `./pkg/storage/ledger.go`
```go
type LedgerEntry struct {
	ID             string
	Tradespace     string
	Currency       string
	Amount         string    // decimal string, positive or negative
	AllocationName string    // references the Allocation record
	TargetType     string    // e.g., "polymarket/v1/Order"
	TargetName     string
	CreatedAt      time.Time
}

type LedgerStorage interface {
	// Append atomically checks balance and inserts a ledger entry.
	// Returns ErrInsufficientBalance if SUM + amount < 0.
	// Returns ErrAlreadyAllocated if target already has an entry.
	Append(ctx context.Context, entry *LedgerEntry) error

	// Balance returns SUM(amount) for tradespace+currency.
	Balance(ctx context.Context, tradespace, currency string) (string, error)

	// GetByTarget returns entry for a target record.
	GetByTarget(ctx context.Context, tradespace, targetType, targetName string) (*LedgerEntry, error)

	// List returns all entries for a tradespace.
	List(ctx context.Context, tradespace string) ([]*LedgerEntry, error)

	Close() error
}
```

**Use cases:** Fund allocation tracking, balance queries, preventing double-spend.

# Infrastructure

```bash
# Start Redis Stack (includes TimeSeries)
docker-compose up -d
```