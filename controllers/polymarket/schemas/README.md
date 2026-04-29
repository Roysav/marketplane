# Polymarket Controller Schemas

JSON Schema fixtures for `core/v1/MetaRecord` specs managed by the
polymarket-order-controller. Load via `schemas.LoadMetaRecords()` at startup
or in tests to register `polymarket/v1/Order` and `polymarket/v1/Trade`.

- `order.json` — user-authored order intent.
- `trade.json` — controller-owned projection of one Polymarket trade keyed by
  external `tradeID`.
