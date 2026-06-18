#!/usr/bin/env bash
# Injects one failure scenario into the running stack and reports what it did.
#
# Usage:
#   ./chaos.sh <scenario> [duration-seconds]
#   ./chaos.sh random
#
# Scenarios:
#   postgres-kill           stop the Postgres container for DURATION seconds, then restart it
#   broker-delay            pause the RabbitMQ container for DURATION seconds (connections hang), then unpause
#   malformed-payload       publish an unparseable message directly to orders.exchange
#   duplicate-message       publish the same eventId twice directly to orders.exchange
#   dlq-reprocessor-retry   stop Postgres, publish a message pre-tagged to land straight in
#                           dlx.orders.placed with a transient reason, then restart Postgres —
#                           exercises DlqReprocessor's bounded retry-before-escalate path rather
#                           than its immediate-escalate path. DURATION < 10s: the first reprocess
#                           attempt also fails, succeeds on the tier-1 retry. DURATION >= ~70s
#                           (the reprocessor's own 3-tier budget: 10s+20s+40s): exhausts retries
#                           and escalates to needs-attention.orders.placed instead.
#
# Requires: docker, and a Python venv with tools/chaos/requirements.txt installed for the
# message-injection scenarios (see tools/chaos/README.md).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="${1:-random}"
DURATION="${2:-20}"

YELLOW='\033[1;33m'; GREEN='\033[0;32m'; RESET='\033[0m'

if [ "$SCENARIO" = "random" ]; then
  SCENARIOS=(postgres-kill broker-delay malformed-payload duplicate-message)
  SCENARIO="${SCENARIOS[$((RANDOM % ${#SCENARIOS[@]}))]}"
  echo -e "${YELLOW}[chaos] randomly selected: $SCENARIO${RESET}"
fi

case "$SCENARIO" in
  postgres-kill)
    echo -e "${YELLOW}[chaos] stopping polyglider_db for ${DURATION}s${RESET}"
    docker stop polyglider_db
    sleep "$DURATION"
    docker start polyglider_db
    echo -e "${GREEN}[chaos] polyglider_db restarted${RESET}"
    ;;
  broker-delay)
    echo -e "${YELLOW}[chaos] pausing polyglider_broker for ${DURATION}s${RESET}"
    docker pause polyglider_broker
    sleep "$DURATION"
    docker unpause polyglider_broker
    echo -e "${GREEN}[chaos] polyglider_broker unpaused${RESET}"
    ;;
  malformed-payload)
    echo -e "${YELLOW}[chaos] publishing a malformed payload to orders.exchange${RESET}"
    python3 "$SCRIPT_DIR/publish_chaos_message.py" malformed
    ;;
  duplicate-message)
    echo -e "${YELLOW}[chaos] publishing a duplicate eventId to orders.exchange${RESET}"
    python3 "$SCRIPT_DIR/publish_chaos_message.py" duplicate
    ;;
  dlq-reprocessor-retry)
    echo -e "${YELLOW}[chaos] stopping polyglider_db for ${DURATION}s, publishing a transient-reason message straight into dlx.orders.placed${RESET}"
    docker stop polyglider_db
    python3 "$SCRIPT_DIR/publish_chaos_message.py" transient-in-dlx
    sleep "$DURATION"
    docker start polyglider_db
    echo -e "${GREEN}[chaos] polyglider_db restarted${RESET}"
    ;;
  *)
    echo "Unknown scenario: $SCENARIO" >&2
    exit 1
    ;;
esac
