#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

# ── colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; MAGENTA='\033[0;35m'; RESET='\033[0m'

log()     { echo -e "${GREEN}[run-all]${RESET} $*"; }
log_svc() { echo -e "${CYAN}[$1]${RESET} $2"; }
die()     { echo -e "${RED}[run-all] ERROR:${RESET} $*" >&2; exit 1; }

# ── cleanup on exit ────────────────────────────────────────────────────────────
PIDS=()
cleanup() {
  echo ""
  log "Shutting down..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  log "All services stopped."
}
trap cleanup INT TERM EXIT

# ── prefixed log helper (run in background) ────────────────────────────────────
# Usage: prefix_log "LABEL" colour command [args...]
prefix_log() {
  local label="$1" colour="$2"; shift 2
  "$@" 2>&1 | while IFS= read -r line; do
    echo -e "${colour}[${label}]${RESET} ${line}"
  done &
  PIDS+=($!)
}

# ── 1. Infrastructure ──────────────────────────────────────────────────────────
log "Starting infrastructure (RabbitMQ + Postgres)..."
docker compose -f "$REPO_ROOT/docker-compose.yml" up -d

# Wait for RabbitMQ AMQP port
log "Waiting for RabbitMQ to be ready..."
for i in $(seq 1 30); do
  if docker exec polyglider_broker rabbitmq-diagnostics -q ping &>/dev/null; then
    log "RabbitMQ is ready."
    break
  fi
  [ "$i" -eq 30 ] && die "RabbitMQ did not become ready in time."
  sleep 1
done

# Wait for Postgres
log "Waiting for Postgres to be ready..."
for i in $(seq 1 30); do
  if docker exec polyglider_db pg_isready -U postgres -q; then
    log "Postgres is ready."
    break
  fi
  [ "$i" -eq 30 ] && die "Postgres did not become ready in time."
  sleep 1
done

# ── 2. Scala processing engine ─────────────────────────────────────────────────
log "Starting Scala processing engine..."
prefix_log "scala" "$MAGENTA" \
  bash -c "cd '$REPO_ROOT/processing-engine-scala' && sbt run"

# Give sbt a moment to start compiling before launching the gateway
sleep 2

# ── 3. C# gateway ─────────────────────────────────────────────────────────────
log "Starting C# gateway..."
prefix_log "gateway" "$YELLOW" \
  bash -c "cd '$REPO_ROOT/gateway-api-cs' && dotnet run --project gateway-api-cs.csproj"

# ── 4. Wait ───────────────────────────────────────────────────────────────────
log ""
log "All services started. Press ${RED}Ctrl+C${RESET} to stop everything."
log ""
log "  Gateway   → http://localhost:5187"
log "  RabbitMQ  → http://localhost:15672  (guest/guest)"
log "  Postgres  → localhost:5432          (postgres/postgres, db: polyglider_inventory)"
log ""

wait "${PIDS[@]}"
