#!/usr/bin/env bash
# gen-certs.sh — Generate a CA, server certificate, and a sample client
# certificate for testing mTLS with Marketplane.
#
# Usage:
#   ./scripts/gen-certs.sh [output-dir]
#
# Output (default dir: certs/):
#   ca.crt / ca.key          — Self-signed CA
#   server.crt / server.key  — Server certificate (signed by CA, SAN=localhost)
#   client.crt / client.key  — Client certificate (signed by CA, CN=alice)
#
# The client CN ("alice") maps to a core/v1/User record named "alice".
#
# Start the server with mTLS:
#   go run ./cmd/server -cert certs/server.crt -key certs/server.key -ca certs/ca.crt
#
# Connect with grpcurl (mTLS):
#   grpcurl -cacert certs/ca.crt \
#           -cert   certs/client.crt \
#           -key    certs/client.key \
#           localhost:50051 list

set -euo pipefail

OUT="${1:-certs}"
mkdir -p "$OUT"

echo "==> Generating CA key and self-signed certificate..."
openssl genrsa -out "$OUT/ca.key" 4096 2>/dev/null
openssl req -new -x509 -days 3650 -key "$OUT/ca.key" \
  -subj "/O=Marketplane/CN=Marketplane CA" \
  -out "$OUT/ca.crt"

echo "==> Generating server key and certificate (SAN: localhost, 127.0.0.1)..."
openssl genrsa -out "$OUT/server.key" 2048 2>/dev/null
openssl req -new -key "$OUT/server.key" \
  -subj "/O=Marketplane/CN=marketplane-server" \
  -out "$OUT/server.csr"
openssl x509 -req -days 365 -in "$OUT/server.csr" \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
  -out "$OUT/server.crt" 2>/dev/null

echo "==> Generating client key and certificate (CN=alice)..."
openssl genrsa -out "$OUT/client.key" 2048 2>/dev/null
openssl req -new -key "$OUT/client.key" \
  -subj "/O=Marketplane/CN=alice" \
  -out "$OUT/client.csr"
openssl x509 -req -days 365 -in "$OUT/client.csr" \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/client.crt" 2>/dev/null

# Clean up CSRs
rm -f "$OUT"/*.csr "$OUT"/*.srl

echo ""
echo "Certificates written to: $OUT/"
echo ""
echo "  ca.crt      — CA certificate (give this to the server via -ca)"
echo "  server.crt  — Server certificate (-cert)"
echo "  server.key  — Server private key (-key)"
echo "  client.crt  — Client certificate (CN=alice)"
echo "  client.key  — Client private key"
echo ""
echo "Start server:"
echo "  go run ./cmd/server -cert $OUT/server.crt -key $OUT/server.key -ca $OUT/ca.crt"
echo ""
echo "Create the matching core/v1/User record (after server is running):"
echo '  grpcurl -cacert '"$OUT"'/ca.crt -cert '"$OUT"'/client.crt -key '"$OUT"'/client.key \'
echo '    -d '"'"'{"record":{"type_meta":{"group":"core","version":"v1","kind":"User"},"object_meta":{"name":"alice","tradespace":"default-tradespace"},"spec":{"commonName":"alice"}}}'"'"' \'
echo '    localhost:50051 marketplane.RecordService/Create'
