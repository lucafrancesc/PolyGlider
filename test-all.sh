#!/usr/bin/env bash
# Run all tests across every component.
# Integration tests require Docker (Testcontainers spins up RabbitMQ automatically).
#
# Usage:
#   ./test-all.sh            # unit + integration
#   ./test-all.sh --no-integration   # unit tests only (no Docker required)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_INTEGRATION=true

for arg in "$@"; do
  [ "$arg" = "--no-integration" ] && RUN_INTEGRATION=false
done

PASS=0; FAIL=0

run_suite() {
  local label="$1"; shift
  echo ""
  echo "══════════════════════════════════════════"
  echo "  $label"
  echo "══════════════════════════════════════════"
  if "$@"; then
    echo "  ✓ $label passed"
    (( ++PASS ))
  else
    echo "  ✗ $label FAILED"
    (( ++FAIL ))
  fi
}

run_suite "Scala unit tests" \
  bash -c "cd '$REPO_ROOT/processing-engine-scala' && sbt test"

run_suite "C# gateway unit tests" \
  dotnet test "$REPO_ROOT/gateway-api-cs-tests/gateway-api-cs-tests.csproj" \
    --filter "Category!=Integration" \
    --logger "console;verbosity=normal"

if $RUN_INTEGRATION; then
  run_suite "C# gateway integration tests (Testcontainers)" \
    dotnet test "$REPO_ROOT/gateway-api-cs-tests/gateway-api-cs-tests.csproj" \
      --filter "Category=Integration" \
      --logger "console;verbosity=normal"
fi

echo ""
echo "══════════════════════════════════════════"
printf "  Results: %d passed, %d failed\n" "$PASS" "$FAIL"
echo "══════════════════════════════════════════"
echo ""

[ "$FAIL" -eq 0 ] || exit 1
