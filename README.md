# Marketplane

**A Kubernetes-inspired control plane for trading systems.**

Define custom entity types declaratively, persist them, and watch for real-time changes. Built for trading across crypto, prediction markets, and traditional finance.

## Features

- **Extensible Entity System** - Define custom types via EntityDefinition (like K8s CRDs)
- **Dual Storage Model** - Record (persistent) and Stream (real-time) storage
- **Tradespace Isolation** - Namespace-like boundaries for resources
- **Schema Validation** - JSON Schema validation for entity specs

## Quick Start

```bash
go test ./... -v
```

## Concepts

### Entity Types

Entities identified by type string: `group/version/kind`

```
core/v1/Tradespace       # Isolation boundary
core/v1/EntityDefinition # Defines custom types
core/v1/Quota            # Balance limits
polymarket/v1/Order      # Custom type (user-defined)
```

### Defining Custom Types

Create an EntityDefinition to register a new type:

```json
{
  "group": "polymarket",
  "version": "v1",
  "kind": "Order",
  "storage": "record",
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

## Project Structure

```
pkg/
├── entity/          # Core types (Entity, TypeMeta, ObjectMeta)
├── storage/         # RecordStorage interface
│   └── sqlite/      # SQLite implementation
└── validator/       # Schema validation
```

## Status

🚧 **Early Development**

- [x] Core entity types
- [x] RecordStorage interface
- [x] SQLite implementation
- [x] Schema validation
- [x] gRPC API Server
- [x] Redis (Stream storage, watches)
- [x] PostgreSQL implementation
- [ ] CLI tool

## License

MIT
