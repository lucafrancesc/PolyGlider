#!/usr/bin/env bash
# End-to-end test across the full pipeline:
#   HTTP POST → nginx → C# gateway → RabbitMQ → Scala engine → Postgres → MCP read-back
#
# Brings up the full containerized stack (docker-compose.yml's `containerized` profile,
# the same mode documented in the README), posts a uniquely-tagged order, polls Postgres
# for the resulting ledger upsert, then re-reads it through the MCP server's own query
# function (not just psql) to prove the read-back path works end-to-end too.
#
# Usage:
#   ./tools/e2e-test.sh            # build + start the stack, run the test, leave it running
#   ./tools/e2e-test.sh --down     # also tear the stack down afterwards (pass/fail either way)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEAR_DOWN=false
for arg in "$@"; do [ "$arg" = "--down" ] && TEAR_DOWN=true; done

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; RESET='\033[0m'
log() { echo -e "${YELLOW}[e2e]${RESET} $*"; }
ok()  { echo -e "${GREEN}[e2e]${RESET} $*"; }
die() { echo -e "${RED}[e2e] ERROR:${RESET} $*" >&2; exit 1; }

if $TEAR_DOWN; then
  trap 'docker compose -f "$REPO_ROOT/docker-compose.yml" --profile containerized down' EXIT
fi

log "Building and starting the containerized stack..."
docker compose -f "$REPO_ROOT/docker-compose.yml" --profile containerized up -d --build

log "Waiting for RabbitMQ to be ready..."
for i in $(seq 1 30); do
  if docker exec polyglider_broker rabbitmq-diagnostics -q ping &>/dev/null; then
    ok "RabbitMQ is ready."
    break
  fi
  [ "$i" -eq 30 ] && die "RabbitMQ did not become ready in time."
  sleep 1
done

# `depends_on` (no healthcheck condition) only waits for the container to *start*, not for
# RabbitMQ to actually accept connections, and the engine doesn't retry on startup failure —
# it can race RabbitMQ and exit before it's reachable. Retry-restart it until its log shows a
# successful connection.
log "Ensuring the engine is connected to RabbitMQ (retrying past any startup race)..."
ENGINE_CONNECTED=false
for i in $(seq 1 10); do
  if docker logs polyglider-engine-1 2>&1 | grep -q "Connected to RabbitMQ"; then
    ENGINE_CONNECTED=true
    break
  fi
  docker compose -f "$REPO_ROOT/docker-compose.yml" --profile containerized start engine >/dev/null
  sleep 3
done
$ENGINE_CONNECTED || die "Engine never connected to RabbitMQ after $i restart attempts."
ok "Engine is connected to RabbitMQ."

log "Waiting for the gateway to report healthy (via nginx :80)..."
for i in $(seq 1 60); do
  if curl -sf http://localhost/health >/dev/null 2>&1; then
    ok "Gateway is healthy."
    break
  fi
  [ "$i" -eq 60 ] && die "Gateway did not become healthy in time."
  sleep 2
done

# ── Post a uniquely-tagged order ────────────────────────────────────────────────
SKU="E2E-$(date +%s)-$RANDOM"
QUANTITY=3
CUSTOMER_ID=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)

log "Posting order: sku=$SKU quantity=$QUANTITY customerId=$CUSTOMER_ID"
PAYLOAD=$(printf '{"sku":"%s","quantity":%d,"customerId":"%s"}' "$SKU" "$QUANTITY" "$CUSTOMER_ID")
RESPONSE=$(curl -s -o /tmp/e2e_body -w "%{http_code}" \
  -X POST "http://localhost/api/orders" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

[ "$RESPONSE" -eq 202 ] || die "Expected 202 from gateway, got $RESPONSE: $(cat /tmp/e2e_body)"
EVENT_ID=$(grep -o '"eventId":"[^"]*"' /tmp/e2e_body | cut -d'"' -f4)
ok "Gateway accepted the order (eventId=$EVENT_ID)."

# ── Poll Postgres for the Scala engine's ledger upsert ──────────────────────────
log "Waiting for the Scala engine to process the message and upsert the ledger..."
QTY_IN_LEDGER=""
for i in $(seq 1 30); do
  QTY_IN_LEDGER=$(docker exec polyglider_db psql -U postgres -d polyglider_inventory -tA \
    -c "SELECT qty FROM ledger WHERE sku = '${SKU}'" 2>/dev/null || true)
  [ -n "$QTY_IN_LEDGER" ] && break
  sleep 1
done

[ "$QTY_IN_LEDGER" = "$QUANTITY" ] || die "Expected ledger qty=$QUANTITY for $SKU, got '${QTY_IN_LEDGER:-<not found>}' after 30s"
ok "Postgres ledger upsert confirmed: $SKU → qty=$QTY_IN_LEDGER"

log "Confirming the event landed in processed_events (dedup table)..."
docker exec polyglider_db psql -U postgres -d polyglider_inventory -tA \
  -c "SELECT 1 FROM processed_events WHERE event_id = '${EVENT_ID}'" | grep -q 1 \
  || die "eventId $EVENT_ID not found in processed_events"
ok "processed_events row confirmed."

# ── Read back through the MCP server's own query function ──────────────────────
log "Reading back via the MCP server's get_sku_quantity (not just psql)..."
MCP_DIR="$REPO_ROOT/tools/mcp-server"
if [ ! -d "$MCP_DIR/.venv" ]; then
  log "Creating venv for MCP server..."
  python3 -m venv "$MCP_DIR/.venv"
  "$MCP_DIR/.venv/bin/pip" install -q -r "$MCP_DIR/requirements.txt"
fi

MCP_QTY=$(cd "$MCP_DIR" && POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/polyglider_inventory" \
  .venv/bin/python -c "
import main
print(main.get_sku_quantity('${SKU}')['qty'])
")

[ "$MCP_QTY" = "$QUANTITY" ] || die "MCP read-back returned qty=$MCP_QTY, expected $QUANTITY"
ok "MCP read-back confirmed: $SKU → qty=$MCP_QTY"

echo ""
ok "End-to-end pipeline verified: HTTP → nginx → gateway → RabbitMQ → Scala engine → Postgres → MCP."
