# Architecture

## High-Level Overview

```
┌─────────────────────────────────────────────────────────┐
│                        Clients                          │
│            (gRPC, grpcurl, controllers, models)         │
└───────────────────────────┬─────────────────────────────┘
                            │ gRPC (port 50051)
                            ▼
┌─────────────────────────────────────────────────────────┐
│                      API Server                         │
│          cmd/server  ─  pkg/server                      │
│   ┌──────────────┬─────────────────┬─────────────────┐  │
│   │ RecordServer │  StreamServer   │  LedgerServer   │  │
│   └──────┬───────┴────────┬────────┴────────┬────────┘  │
│          │                │                 │           │
│   ┌──────▼───────┐ ┌──────▼──────┐         │           │
│   │   Service    │ │StreamService│         │           │
│   │  pkg/service │ │ pkg/service │         │           │
│   └──────┬───────┘ └──────┬──────┘         │           │
└──────────┼────────────────┼────────────────┼───────────┘
           │                │                │
    ┌──────▼──────┐  ┌──────▼──────┐  ┌─────▼──────┐
    │  RowStorage │  │StreamStorage│  │LedgerStorage│
    │  (Row data) │  │ (Timeseries)│  │(Append-only)│
    └──────┬──────┘  └──────┬──────┘  └─────┬───────┘
           │                │               │
    ┌──────▼──────┐  ┌──────▼──────┐  ┌─────▼───────┐
    │ SQLite /    │  │    Redis    │  │ SQLite /    │
    │ PostgreSQL  │  │ TimeSeries  │  │ PostgreSQL  │
    └─────────────┘  └─────────────┘  └─────────────┘

  EventStorage (Redis Streams) is used by Service to publish
  record change events and by the Watch RPC to stream them
  back to clients.

┌─────────────────────────────────────────────────────────┐
│               AllocationController                      │
│                cmd/controller                           │
│   Watches core/v1/Allocation via gRPC Watch             │
│   Calls LedgerService.Append → updates record status    │
└─────────────────────────────────────────────────────────┘
```

## Packages

| Package | Responsibility |
|---------|----------------|
| `pkg/record` | Core Go types: `Record`, `TypeMeta`, `ObjectMeta`, `GroupVersionKind` |
| `pkg/service` | Business logic: CRUD for records, stream operations, schema validation orchestration, event publishing |
| `pkg/server` | gRPC handlers: translates protobuf ↔ domain types, maps errors to gRPC status codes |
| `pkg/controller` | `AllocationController` reconciliation loop (runs out-of-process) |
| `pkg/storage` | Storage interfaces: `RowStorage`, `StreamStorage`, `EventStorage`, `LedgerStorage` |
| `pkg/storage/sqlite` | SQLite implementations of `RowStorage` and `LedgerStorage` |
| `pkg/storage/postgres` | PostgreSQL implementations of `RowStorage` and `LedgerStorage` |
| `pkg/storage/redis` | Redis implementations of `StreamStorage` (TimeSeries) and `EventStorage` (Streams) |
| `pkg/validator` | JSON Schema validation for record specs; scope enforcement |
| `api/proto` | Protobuf service definitions (RecordService, StreamService, LedgerService) |
| `cmd/server` | Binary entry point for the API server |
| `cmd/controller` | Binary entry point for the allocation controller |

## Storage Types

### Row Storage (SQLite / PostgreSQL)

Persistent, atomic storage for records. The storage layer is intentionally generic — it stores raw JSON blobs and does not parse `spec`/`status` fields. All business logic lives in `pkg/service`.

Key properties:
- Filtering by `Type`, `Tradespace`, and `Labels` only (no JSON field queries by design)
- Optimistic concurrency via `ResourceVersion`
- Auto-migration on startup

### Stream Storage (Redis TimeSeries)

Real-time timeseries storage for high-frequency market data. Uses the `TS.ADD`, `TS.GET`, and `TS.RANGE` Redis TimeSeries commands. Data values are JSON strings, allowing structured payloads per entry.

Key properties:
- Time-ordered data points keyed by stream name
- Configurable retention per `StreamDefinition`
- Watch support via keyspace notifications

### Event Storage (Redis Streams)

Durable, ordered publish/subscribe messaging using Redis Streams (`XADD`, `XREAD`). Used internally by `Service` to publish record lifecycle events (`created`, `updated`, `deleted`) and surfaced to clients via the `Watch` RPC.

Key properties:
- Ordered delivery (each event has a monotonic stream ID)
- Consumers can replay from a specific event ID (`SubscribeFrom`)
- Topics follow the pattern `record:<group>/<version>/<kind>`

### Ledger Storage (SQLite / PostgreSQL)

Append-only fund tracking for tradespace balances. `Append` is executed inside a database transaction that atomically reads the current balance sum and inserts the new entry, preventing races that could cause overdrafts.

Key properties:
- Append-only (entries are never updated or deleted)
- `ErrInsufficientBalance` if `SUM(amount) + new_amount < 0`
- `ErrAlreadyAllocated` if the target record already has an entry

## Scope System

Every record type has a **scope** that determines which tradespace it belongs to:

| Scope | Tradespace | Example types |
|-------|------------|---------------|
| `global` | Must be `"default"` or empty | `core/v1/Tradespace`, `core/v1/MetaRecord`, `core/v1/StreamDefinition` |
| `tradespace` | Must be a non-default tradespace name | `core/v1/Quota`, `core/v1/Allocation`, all user-defined types |

The `Validator.ValidateScope` method enforces this before any record is persisted.

## Validation Pipeline

When creating or updating a record, `Service` runs two validation passes:

```
1. ValidateScope  ─── Is the tradespace correct for this type?
        │
        ▼
2. Validate       ─── Does spec satisfy the JSON Schema?
        │
        ├── Core type? → use hardcoded schema from pkg/validator
        └── Custom type? → look up MetaRecord in storage, extract schema field
```

Core type schemas are compiled once at startup (`init()`) to avoid repeated compilation overhead.

## Controller Pattern

Controllers are separate processes that watch the API server for record events and reconcile the desired state. They communicate exclusively via gRPC — they have no direct database access.

The `AllocationController` is the first implemented controller:

```
1. Initial sync ──► list all Allocation records, process pending ones
2. Watch loop   ──► receive "created" events, reconcile immediately
3. Periodic resync (30s) ──► catch any missed events
```

This pattern is inspired by Kubernetes controllers: every controller is a reconciliation loop that makes incremental progress toward the desired state and is safe to run multiple times (idempotent for already-processed records).
