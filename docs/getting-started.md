# Getting Started

## Prerequisites

- **Go 1.22+** — [https://go.dev/dl](https://go.dev/dl)
- **Docker** — needed for Redis Stack (TimeSeries + Streams)

## 1. Start Infrastructure

Redis Stack provides both Redis Streams (events) and Redis TimeSeries (stream data):

```bash
docker-compose up -d
```

Redis will be available at `localhost:6379`. The API server falls back gracefully if Redis is unavailable — record CRUD still works, but Watch and stream operations are disabled.

## 2. Start the API Server

```bash
go run ./cmd/server
```

### Server flags

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `50051` | gRPC listen port |
| `-db` | `marketplane.db` | SQLite file path, `:memory:` for in-memory, or a `postgres://` DSN |
| `-redis` | `localhost:6379` | Redis address |
| `-debug` | `false` | Enable debug-level logging |

Examples:

```bash
# SQLite (default)
go run ./cmd/server -port 50051 -db marketplane.db

# In-memory SQLite (for testing/demos)
go run ./cmd/server -db :memory:

# PostgreSQL
go run ./cmd/server -db "postgres://user:pass@localhost:5432/marketplane?sslmode=disable"

# Debug logging
go run ./cmd/server -debug
```

The server starts with gRPC reflection enabled, so you can explore the API with `grpcurl` or `grpcui`:

```bash
grpcurl -plaintext localhost:50051 list
```

## 3. Start the Allocation Controller (optional)

The allocation controller is a separate process that watches for `core/v1/Allocation` records and processes them against the ledger.

```bash
go run ./cmd/controller
```

### Controller flags

| Flag | Default | Description |
|------|---------|-------------|
| `-server` | `localhost:50051` | API server gRPC address |
| `-debug` | `false` | Enable debug-level logging |

## 4. Walkthrough

The following steps show a complete allocation lifecycle using `grpcurl`. All requests use JSON encoding.

### Step 1 — Create a Tradespace

```bash
grpcurl -plaintext -d '{
  "record": {
    "type_meta": {"group": "core", "version": "v1", "kind": "Tradespace"},
    "object_meta": {"name": "algo-ts"},
    "spec": {"description": "My algo trading tradespace"}
  }
}' localhost:50051 marketplane.v1.RecordService/Create
```

### Step 2 — Register a custom record type

Create a `MetaRecord` to register the `polymarket/v1/Order` type:

```bash
grpcurl -plaintext -d '{
  "record": {
    "type_meta": {"group": "core", "version": "v1", "kind": "MetaRecord"},
    "object_meta": {"name": "Order.polymarket"},
    "spec": {
      "group": "polymarket",
      "version": "v1",
      "kind": "Order",
      "scope": "tradespace",
      "schema": {
        "type": "object",
        "required": ["marketId", "side"],
        "properties": {
          "marketId": {"type": "string"},
          "side": {"type": "string", "enum": ["YES", "NO"]}
        }
      }
    }
  }
}' localhost:50051 marketplane.v1.RecordService/Create
```

> **Note:** MetaRecord names follow the convention `Kind.group` (e.g., `Order.polymarket`).

### Step 3 — Create a record of the custom type

```bash
grpcurl -plaintext -d '{
  "record": {
    "type_meta": {"group": "polymarket", "version": "v1", "kind": "Order"},
    "object_meta": {"name": "order-001", "tradespace": "algo-ts"},
    "spec": {"marketId": "market-abc", "side": "YES"}
  }
}' localhost:50051 marketplane.v1.RecordService/Create
```

### Step 4 — Seed a Quota (starting balance)

```bash
grpcurl -plaintext -d '{
  "record": {
    "type_meta": {"group": "core", "version": "v1", "kind": "Quota"},
    "object_meta": {"name": "main-quota", "tradespace": "algo-ts"},
    "spec": {
      "balances": {"USD": "1000.00"}
    }
  }
}' localhost:50051 marketplane.v1.RecordService/Create
```

> **Note:** `core/v1/Quota` records are processed by the ledger as initial balance deposits. A Quota is stored as a `LedgerEntry` with a positive amount.

### Step 5 — Create an Allocation

```bash
grpcurl -plaintext -d '{
  "record": {
    "type_meta": {"group": "core", "version": "v1", "kind": "Allocation"},
    "object_meta": {"name": "alloc-001", "tradespace": "algo-ts"},
    "spec": {
      "currency": "USD",
      "amount": "50.00",
      "targetType": "polymarket/v1/Order",
      "targetName": "order-001"
    }
  }
}' localhost:50051 marketplane.v1.RecordService/Create
```

### Step 6 — Observe the Approved status

Once the allocation controller processes the record (within seconds), its `status` will be updated:

```bash
grpcurl -plaintext -d '{
  "type": "core/v1/Allocation",
  "tradespace": "algo-ts",
  "name": "alloc-001"
}' localhost:50051 marketplane.v1.RecordService/Get
```

You should see:

```json
{
  "record": {
    "status": {
      "phase": "Approved",
      "ledgerEntryID": "...",
      "message": "allocation approved and recorded in ledger"
    }
  }
}
```

If the balance was insufficient you would see `"phase": "Rejected"` instead.

### Step 7 — Watch for changes (optional)

Open a second terminal and start a Watch before creating records:

```bash
grpcurl -plaintext -d '{"type": "polymarket/v1/Order"}' \
  localhost:50051 marketplane.v1.RecordService/Watch
```

Any `Create`, `Update`, or `Delete` on `polymarket/v1/Order` records will be streamed to this terminal.

## 5. Run Tests

```bash
go test ./... -v
```

Some integration tests require Redis and/or PostgreSQL to be running. If those services are unavailable, `go test ./... -v` may fail rather than skip those tests automatically.
