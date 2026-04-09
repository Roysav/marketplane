# CLAUDE.md - Project Context for AI Assistants

## Project: Marketplane

**A Kubernetes-inspired control plane for trading systems.**

Module: `github.com/roysav/marketplane`

## Architecture

```
┌─────────────┐     gRPC      ┌─────────────┐
│   Clients   │◄────────────►│  API Server │  (not yet implemented)
└─────────────┘               └──────┬──────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                                 ▼
              ┌──────────┐                     ┌──────────┐
              │ Postgres │                     │  Redis   │
              │  (Row)   │                     │ (Stream) │
              └──────────┘                     └──────────┘
```

**Storage types:**
- **Row** (PostgreSQL/SQLite) - persistent storage for records
- **Stream** (Redis Streams) - real-time timeseries data (JSON values)
- **Event** (Redis Streams) - message queue for change events

## Key Concepts

### Record
A Record is the core resource type - like an order record, tradespace record, etc.

```go
type Record struct {
    TypeMeta   // group/version/kind
    ObjectMeta // name, tradespace, labels, resourceVersion
    Spec       map[string]any  // desired state
    Status     map[string]any  // current state
}
```

### Record Identification
Type string: `"group/version/kind"` e.g., `"core/v1/Tradespace"`

### Core Records (built-in)
| Type | Description |
|------|-------------|
| `core/v1/MetaRecord` | Defines custom record types (Row storage) |
| `core/v1/StreamDefinition` | Defines custom stream types (Stream storage) |
| `core/v1/Tradespace` | Isolation boundary |
| `core/v1/Quota` | Balance limits per Tradespace |

### Tradespace
- Isolation boundary for records (like K8s namespace)
- Global records use `"default"` tradespace

### MetaRecord
Define custom record types (Row storage):
```json
{
  "group": "polymarket",
  "version": "v1",
  "kind": "Order",
  "schema": { "type": "object", "required": ["marketId"] }
}
```

### StreamDefinition
Define custom stream types (timeseries data):
```json
{
  "group": "binance",
  "version": "v1",
  "kind": "Price",
  "retention": "24h",
  "schema": {
    "type": "object",
    "required": ["symbol", "last_price"],
    "properties": {
      "symbol": { "type": "string" },
      "last_price": { "type": "string" }
    }
  }
}
```

## Current Implementation

```
api/
└── proto/
    └── record.proto       # gRPC service definition
pkg/
├── record/
│   ├── types.go           # Record, TypeMeta, ObjectMeta, GroupVersionKind
│   └── types_test.go
├── service/
│   ├── service.go         # Business logic layer (CRUD, validation, events)
│   └── service_test.go
├── storage/
│   ├── storage.go         # RowStorage interface (Row type)
│   ├── stream.go          # StreamStorage interface (timeseries)
│   ├── event.go           # EventStorage interface (message queue)
│   ├── sqlite/
│   │   ├── sqlite.go      # SQLite RowStorage implementation
│   │   └── sqlite_test.go
│   └── redis/
│       ├── redis.go       # Redis client setup
│       ├── stream.go      # Redis TimeSeries implementation
│       ├── stream_test.go
│       ├── event.go       # Redis Streams implementation
│       └── event_test.go
└── validator/
    ├── validator.go       # Schema validation (gojsonschema)
    └── validator_test.go
```

### RowStorage Interface
```go
type Key struct {
    Type       string  // "core/v1/Tradespace"
    Tradespace string
    Name       string
}

type Row struct {
    Type, Tradespace, Name string
    Labels                 map[string]string
    Data                   string  // JSON blob
    ResourceVersion        int64
    CreatedAt, UpdatedAt   time.Time
}

type RowStorage interface {
    Create(ctx, *Row) (*Row, error)
    Get(ctx, Key) (*Row, error)
    Update(ctx, *Row) (*Row, error)
    Delete(ctx, Key) error
    List(ctx, Query) ([]*Row, error)
    Close() error
}
```

### Design Decisions
1. **Storage is generic** - doesn't know about Record/Spec/Status
2. **No caching** - always hit database, for now
3. **Type = group/version/kind** - single string
4. **No UID** - key is Type+Tradespace+Name
5. **Labels queryable** - stored and indexed
6. **Data is opaque JSON** - storage doesn't parse it
7. **Core schemas hardcoded** - bootstrap problem
8. **Authentication** - For now, only TLS authentication
9. **Migrations** - Record schemas are immutable, schema change = new version


## Not Implemented Yet
- [ ] gRPC API Server
- [x] Redis (Stream storage, watches) ✓
- [x] Service layer ✓
- [ ] RBAC
- [ ] Allocation (cross-tradespace funds)
- [ ] CLI
- [ ] PostgreSQL
- [ ] Intensive Validation

## Commands
```bash
go test ./... -v      # Run tests
go mod tidy           # Tidy dependencies
docker-compose up -d  # Start Redis Stack
```

## Tech Stack
- Go 1.22+
- SQLite (modernc.org/sqlite - pure Go)
- Redis Stack (Streams for timeseries and events)
- gojsonschema (JSON Schema validation)
- Future: PostgreSQL, gRPC, buf
