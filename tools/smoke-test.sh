#!/usr/bin/env bash
# Sends a batch of randomised POST /api/orders requests and reports results.
#
# Usage:
#   ./tools/smoke-test.sh [COUNT] [HOST]
#
# Defaults: COUNT=20, HOST=http://localhost:5187
set -euo pipefail

COUNT="${1:-20}"
HOST="${2:-http://localhost:5187}"
ENDPOINT="$HOST/api/orders"

SKUS=("LAPTOP-001" "MOUSE-023" "MONITOR-99" "KEYBOARD-05" "HEADPHONES-12")

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; RESET='\033[0m'

passed=0; failed=0

echo -e "${YELLOW}Sending $COUNT orders to $ENDPOINT${RESET}"
echo ""

for i in $(seq 1 "$COUNT"); do
  sku="${SKUS[$((RANDOM % ${#SKUS[@]}))]}"
  qty=$(( (RANDOM % 5) + 1 ))
  customer=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)

  payload=$(printf '{"sku":"%s","quantity":%d,"customerId":"%s"}' "$sku" "$qty" "$customer")

  response=$(curl -s -o /tmp/smoke_body -w "%{http_code}" \
    -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "$payload")

  body=$(cat /tmp/smoke_body)

  if [ "$response" -eq 202 ]; then
    event_id=$(echo "$body" | grep -o '"eventId":"[^"]*"' | cut -d'"' -f4 || echo "?")
    echo -e "  ${GREEN}✓${RESET} [$i/$COUNT] $sku qty=$qty → 202  eventId=${event_id}"
    passed=$((passed + 1))
  else
    echo -e "  ${RED}✗${RESET} [$i/$COUNT] $sku qty=$qty → $response  body=$body"
    failed=$((failed + 1))
  fi
done

echo ""
echo -e "Results: ${GREEN}$passed passed${RESET}, ${RED}$failed failed${RESET} / $COUNT total"

[ "$failed" -eq 0 ] || exit 1
