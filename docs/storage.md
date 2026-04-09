**Two storage types:**
- **Record** - a persistent record, atomic operations, use cases: storing transaction data.
- **Stream** (Redis) - real-time, more fragile - no atomicity premises, use cases: continuous ticker quota for assets, events(?perhaps should be an optionally different storage, but ok for now)

# Record Storage
Should be atomic stable storage for important things, like transactions, allocations, quotas etc...
a records storage implementation is kept de-coupled from business logic as much as possible, but leakage from business logic to implementation is inevitable.
From the scope of a record-storage backend a record look like:

from `./pkg/storage/storage.go`
```go
type Record struct {
	Type       string // e.g. "core/v1/Tradespace"
	Tradespace string
	Name       string
	Labels     map[string]string
	Data       string // JSON blob

	ResourceVersion int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
```
Clear limitations on the ability to filter records in a more complex manner, as there is now way for the caller to filter json fields. this is by design.

# Stream (Timeseries)

Handles real-time data like ticker prices, quotes, and other timeseries data. Uses Redis TimeSeries for storage.

from `./pkg/storage/stream.go`
```go
type StreamEntry struct {
	Key       string
	Value     float64
	Timestamp time.Time
}

type StreamStorage interface {
	Add(ctx context.Context, key string, value float64) error
	AddAt(ctx context.Context, key string, ts time.Time, value float64) error
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

**Use cases:** Entity change notifications, order events, system events.

# Infrastructure

```bash
# Start Redis Stack (includes TimeSeries)
docker-compose up -d
```