# Ledger & Allocations

## Overview

The ledger is an append-only log of approved fund allocations. It enforces that a tradespace never goes below zero balance and that each target record is only allocated funds once.

Allocations are modelled as a `core/v1/Allocation` record with a simple lifecycle:

```
Pending  →  Approved
         →  Rejected
```

The **AllocationController** runs as a separate process, watches for new `Allocation` records via the gRPC Watch API, and reconciles each one by attempting to append an entry to the ledger.

## core/v1/Allocation

An `Allocation` record is tradespace-scoped. Its `spec` must contain:

| Field | Type | Description |
|-------|------|-------------|
| `currency` | string | Currency code, e.g. `"USD"` |
| `amount` | string | Decimal amount, positive to allocate, negative to reclaim |
| `targetType` | string | The type of the record being funded, e.g. `"polymarket/v1/Order"` |
| `targetName` | string | The name of the target record |

Example:

```json
{
  "group": "core", "version": "v1", "kind": "Allocation",
  "metadata": {"name": "alloc-001", "tradespace": "algo-ts"},
  "spec": {
    "currency": "USD",
    "amount": "50.00",
    "targetType": "polymarket/v1/Order",
    "targetName": "order-001"
  }
}
```

After reconciliation the controller writes back the result to `status`:

**Approved:**
```json
{
  "status": {
    "phase": "Approved",
    "ledgerEntryID": "abc-123",
    "message": "allocation approved and recorded in ledger"
  }
}
```

**Rejected:**
```json
{
  "status": {
    "phase": "Rejected",
    "message": "insufficient balance"
  }
}
```

## LedgerStorage Interface

`LedgerStorage` (defined in `pkg/storage/ledger.go`) is the low-level interface that backs the ledger:

```go
type LedgerStorage interface {
    // Append atomically checks balance and inserts a ledger entry.
    // Returns ErrInsufficientBalance if SUM(amount) + entry.Amount < 0.
    // Returns ErrAlreadyAllocated if the target already has an entry.
    Append(ctx context.Context, entry *LedgerEntry) error

    // Balance returns SUM(amount) for tradespace+currency as a decimal string.
    Balance(ctx context.Context, tradespace, currency string) (string, error)

    // GetByTarget returns the entry for a specific target record.
    GetByTarget(ctx context.Context, tradespace, targetType, targetName string) (*LedgerEntry, error)

    // List returns all entries for a tradespace.
    List(ctx context.Context, tradespace string) ([]*LedgerEntry, error)

    Close() error
}
```

Implementations exist for both SQLite (`pkg/storage/sqlite`) and PostgreSQL (`pkg/storage/postgres`). The atomicity of `Append` is guaranteed by a database transaction that reads the current balance and inserts the new entry in a single operation.

## AllocationController

The `AllocationController` (`pkg/controller/allocation.go`) is a reconciliation loop that keeps `Allocation` records in sync with the ledger.

### Startup sequence

1. **Initial sync** — lists all `Allocation` records and processes any that are still in `Pending` phase.
2. **Watch** — opens a streaming `Watch` call on `core/v1/Allocation` to receive new `created` events.
3. **Periodic resync** — re-runs the full sync every 30 seconds (configurable) to catch any missed events.

### Reconciliation

For each pending `Allocation` the controller:

1. Extracts `currency`, `amount`, `targetType`, `targetName` from `spec`.
2. Calls `LedgerService.Append` via gRPC.
3. On success — updates the `Allocation` status to `Approved` with the new ledger entry ID.
4. On `FailedPrecondition` (insufficient balance) — updates status to `Rejected`.
5. On `AlreadyExists` (target already allocated) — updates status to `Rejected`.
6. On other errors — logs the error and returns it (the next resync will retry).

### Running the controller

```bash
go run ./cmd/controller -server localhost:50051
```

The controller connects to the API server over gRPC (insecure by default). It requires no direct database access — all operations go through the API server.

## Quota

A `core/v1/Quota` record seeds the initial balance for a tradespace. It is tradespace-scoped and its `spec.balances` map holds `currency → decimal-string` starting amounts.

Example:
```json
{
  "group": "core", "version": "v1", "kind": "Quota",
  "metadata": {"name": "main-quota", "tradespace": "algo-ts"},
  "spec": {
    "balances": {"USD": "1000.00", "USDC": "500.00"}
  }
}
```

The ledger balance for a tradespace is computed as the sum of all ledger entries (which includes the positive Quota seed entries and any negative spend entries).

