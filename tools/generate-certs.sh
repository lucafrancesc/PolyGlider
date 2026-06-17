#!/usr/bin/env bash
# Generates a self-signed CA + server certificate for local TLS development.
# Output goes to certs/ at the repo root. Never commit private keys.
#
# Usage: ./tools/generate-certs.sh
# Requires: openssl
#
# After running, reference the certs in docker-compose.yml or service config.
# For production, replace these with certs from a trusted CA.
set -euo pipefail

OUTDIR="$(cd "$(dirname "$0")/.." && pwd)/certs"
mkdir -p "$OUTDIR"

echo "Generating certs in $OUTDIR ..."

# ── 1. CA ───────────────────────────────────────────────────────────────────
openssl genrsa -out "$OUTDIR/ca.key" 4096 2>/dev/null
openssl req -new -x509 -days 3650 -key "$OUTDIR/ca.key" \
  -subj "/CN=PolyGlider-Dev-CA" \
  -out "$OUTDIR/ca.crt"

# ── 2. Server cert (RabbitMQ / Postgres) ────────────────────────────────────
openssl genrsa -out "$OUTDIR/server.key" 2048 2>/dev/null
openssl req -new -key "$OUTDIR/server.key" \
  -subj "/CN=localhost" \
  -out "$OUTDIR/server.csr"
openssl x509 -req -days 365 \
  -in "$OUTDIR/server.csr" \
  -CA "$OUTDIR/ca.crt" -CAkey "$OUTDIR/ca.key" -CAcreateserial \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
  -out "$OUTDIR/server.crt" 2>/dev/null
rm "$OUTDIR/server.csr"

# ── 3. Client cert (mutual TLS) ─────────────────────────────────────────────
openssl genrsa -out "$OUTDIR/client.key" 2048 2>/dev/null
openssl req -new -key "$OUTDIR/client.key" \
  -subj "/CN=polyglider-client" \
  -out "$OUTDIR/client.csr"
openssl x509 -req -days 365 \
  -in "$OUTDIR/client.csr" \
  -CA "$OUTDIR/ca.crt" -CAkey "$OUTDIR/ca.key" -CAcreateserial \
  -out "$OUTDIR/client.crt" 2>/dev/null
rm "$OUTDIR/client.csr"

chmod 600 "$OUTDIR"/*.key

echo ""
echo "Done. Files written to certs/:"
ls -1 "$OUTDIR"
echo ""
echo "Next steps:"
echo "  RabbitMQ : mount certs/ into the container and set RABBIT_SSL=true"
echo "  Postgres : mount server.crt / server.key and set DB_SSL_MODE=require"
echo "  Gateway  : set RABBITMQ__SSL=true"
echo "  Scala    : set RABBIT_SSL=true DB_SSL_MODE=require"
