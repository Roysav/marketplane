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

# Stream
<!-- I really question this naming decision --->

Handles everything needed to bypass the latency limitations of records, and enables the ability to implement watch mechanisms.