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
              │ (Record) │                     │ (Stream) │
              └──────────┘                     └──────────┘
```

**Two storage types:**
- **Record** (PostgreSQL/SQLite) - persistent entities
- **Stream** (Redis) - real-time data, watches (not yet implemented)

## Key Concepts

### Entity Identification
Type string: `"group/version/kind"` e.g., `"core/v1/Tradespace"`

### Core Entities (built-in)
| Type | Description |
|------|-------------|
| `core/v1/EntityDefinition` | Defines custom entity types |
| `core/v1/Tradespace` | Isolation boundary |
| `core/v1/Quota` | Balance limits per Tradespace |

### Tradespace
- Isolation boundary for entities (like K8s namespace)
- Global entities use `"default"` tradespace

### EntityDefinition
Define custom types declaratively:
```json
{
  "group": "polymarket",
  "version": "v1",
  "kind": "Order",
  "storage": "record",
  "schema": { "type": "object", "required": ["marketId"] }
}
```

## Current Implementation

```
pkg/
├── entity/
│   ├── types.go           # Entity, TypeMeta, ObjectMeta, GroupVersionKind
│   └── types_test.go
├── storage/
│   ├── storage.go         # RecordStorage interface
│   └── sqlite/
│       ├── sqlite.go      # SQLite implementation
│       └── sqlite_test.go
└── validator/
    ├── validator.go       # Schema validation
    └── validator_test.go
```

### RecordStorage Interface
```go
type Key struct {
    Type       string  // "core/v1/Tradespace"
    Tradespace string
    Name       string
}

type Record struct {
    Type, Tradespace, Name string
    Labels                 map[string]string
    Data                   string  // JSON blob
    ResourceVersion        int64
    CreatedAt, UpdatedAt   time.Time
}

type RecordStorage interface {
    Create(ctx, *Record) (*Record, error)
    Get(ctx, Key) (*Record, error)
    Update(ctx, *Record) (*Record, error)
    Delete(ctx, Key) error
    List(ctx, Query) ([]*Record, error)
    Close() error
}
```

### Design Decisions
1. **Storage is generic** - doesn't know about Entity/Spec/Status
2. **No caching** - always hit database, for now. 
3. **Type = group/version/kind** - single string
4. **No UID** - key is Type+Tradespace+Name
5. **Labels queryable** - stored and indexed
6. **Data is opaque JSON** - storage doesn't parse it
7. **Core schemas hardcoded** - bootstrap problem
8. **Authentication** - For now, only TLS authentication, start the server with a specified (multiple?) certificate authorities. In the future we'll add more options.
9. **Migrations** - Entity schemas are immutable, schema change = new version.


## Not Implemented Yet
- [ ] gRPC API Server
- [ ] Redis (Stream storage, watches)
- [ ] Service layer
- [ ] RBAC
- [ ] Allocation (cross-tradespace funds)
- [ ] CLI
- [ ] PostgreSQL

## Commands
```bash
go test ./... -v    # Run tests
go mod tidy         # Tidy dependencies
```

## Tech Stack
- Go 1.22+
- SQLite (modernc.org/sqlite - pure Go)
- Future: PostgreSQL, Redis, gRPC


TODOS:
  1. Use a 3rd-party library for validation of OpenAPI3 Spec... 
  2. Implement basic interface for stream storage. I think the initial base implementation could also benefit from sqlite, 
       with concurrent reads to the database. This storage however should have much less options, only key-value set-get, prefix list(?maybe), and watch(prefix) 