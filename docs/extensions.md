# Extension Schema Installation

Extensions can ship their `MetaRecord` and `StreamDefinition` records as JSON manifests and install them with:

```bash
go run ./cmd/schema -server localhost:50051 -f ./extensions/polymarket/schema.json
```

If an extension keeps multiple schema files in a folder, point the installer at the directory instead:

```bash
go run ./cmd/schema -server localhost:50051 -f ./extensions/polymarket
```

Schema records are immutable:
- missing records are created
- an identical existing record is treated as a no-op
- a different existing record causes the install to fail
- schema records are not updated or deleted in place

If a `MetaRecord` or `StreamDefinition` needs to change, publish a new version instead of editing the existing schema in place.

Supported manifest shapes:
- a single `Record` JSON document
- a JSON array of `Record` documents
- an object with a top-level `records` array

The included Polymarket manifest installs:
- `polymarket/v1/CryptoSubscription`
- Stream definitions for the documented Binance and Chainlink RTDS crypto symbols
