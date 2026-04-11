# Marketplane

**A control plane for enabling declarative algotrading.**

Define custom record types declaratively, persist them, allocate funds safely, and watch for real-time changes. Built for trading across crypto, prediction markets, and traditional finance.

## Features

- **Extensible Record System** — Define custom types via `MetaRecord` and `StreamDefinition`.
- **Dual Storage Model** — Row (persistent) and Stream (real-time timeseries) storage.
- **Tradespace Isolation** — Namespace-like boundaries for resources.
- **Schema Validation** — JSON Schema validation for record specs.
- **Ledger & Allocations** — Append-only fund ledger with atomic balance checks.
- **gRPC API** — Full CRUD + Watch over gRPC with server-side streaming.

## Quick Start

### Prerequisites

- Go 1.22+
- Docker (for Redis Stack)

### 1. Start infrastructure

```bash
docker-compose up -d
```

### 2. Run the API server

```bash
go run ./cmd/server
# Options:
#   -port  int     gRPC listen port (default 50051)
#   -db    string  SQLite path or postgres:// DSN (default "marketplane.db")
#   -redis string  Redis address (default "localhost:6379")
#   -debug         Enable debug logging
```

### 3. Run the allocation controller (optional)

```bash
go run ./cmd/controller
# Options:
#   -server string  API server address (default "localhost:50051")
#   -debug          Enable debug logging
```

### 4. Run tests

```bash
go test ./... -v
```

## Concepts

### Record Types

All records are identified by a type string: `group/version/kind`

| Type | Description |
|------|-------------|
| `core/v1/Tradespace` | Isolation boundary (like a namespace) |
| `core/v1/MetaRecord` | Defines a custom Row-storage record type |
| `core/v1/StreamDefinition` | Defines a custom timeseries stream type |
| `core/v1/Quota` | Starting balance limits per tradespace |
| `core/v1/Allocation` | Fund allocation request (Pending → Approved/Rejected) |
| `polymarket/v1/Order` | Example user-defined type |

### Defining Custom Record Types

Create a `MetaRecord` to register a new type:

```json
{
  "group": "polymarket",
  "version": "v1",
  "kind": "Order",
  "scope": "tradespace",
  "schema": {
    "type": "object",
    "required": ["marketId", "side"],
    "properties": {
      "marketId": {"type": "string"},
      "side": {"type": "string", "enum": ["YES", "NO"]},
      "amount": {"type": "string"}
    }
  }
}
```

### Defining Custom Stream Types

Create a `StreamDefinition` to register a timeseries stream:

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
      "symbol": {"type": "string"},
      "last_price": {"type": "string"}
    }
  }
}
```

## Project Structure

```
cmd/
├── server/          # API server binary
└── controller/      # Allocation controller binary
pkg/
├── record/          # Core types (Record, TypeMeta, ObjectMeta)
├── service/         # Business logic (CRUD, stream ops)
├── server/          # gRPC server implementations
├── controller/      # AllocationController reconciliation loop
├── storage/         # Storage interfaces (Row, Stream, Event, Ledger)
│   ├── sqlite/      # SQLite implementations
│   ├── postgres/    # PostgreSQL implementations
│   └── redis/       # Redis Stream + TimeSeries implementations
└── validator/       # JSON Schema validation
api/
└── proto/           # Protobuf service definitions
```

## Documentation

- [Introduction & Concepts](docs/101.md)
- [Architecture](docs/architecture.md)
- [Storage](docs/storage.md)
- [Ledger & Allocations](docs/ledger.md)
- [Getting Started](docs/getting-started.md)

## Status

🚧 **Early Development**

- [x] Core record types
- [x] RowStorage interface + SQLite implementation
- [x] PostgreSQL implementation
- [x] Schema validation
- [x] gRPC API Server (Record, Stream, Ledger services)
- [x] Redis Stream (timeseries) + Event (pub/sub) storage
- [x] Ledger & Allocation system
- [x] AllocationController
- [ ] CLI tool
- [ ] RBAC

## License

MIT
