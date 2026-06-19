#!/usr/bin/env bash
# Run all tests across every component.
# Integration tests require Docker (Testcontainers spins up RabbitMQ automatically).
#
# Usage:
#   ./test-all.sh                  # unit + contract + integration
#   ./test-all.sh --no-integration # unit + contract tests only (no Docker required)
#   ./test-all.sh --e2e            # also runs tools/e2e-test.sh (builds + runs the full
#                                   # containerized stack; slow, not run by default)
#
# Contract test ordering: Scala runs first and generates
# contracts/pacts/scala-engine-cs-gateway.json; the C# contract step reads it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_INTEGRATION=true
RUN_E2E=false

for arg in "$@"; do
  [ "$arg" = "--no-integration" ] && RUN_INTEGRATION=false
  [ "$arg" = "--e2e" ] && RUN_E2E=true
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

# Step 1: Scala — unit tests + generates contracts/pacts/scala-engine-cs-gateway.json
run_suite "Scala unit + contract consumer tests" \
  bash -c "cd '$REPO_ROOT/processing-engine-scala' && sbt test"

# Step 2: C# contract tests — must follow Scala so the pact file exists
run_suite "C# contract tests" \
  dotnet test "$REPO_ROOT/gateway-api-cs-tests/gateway-api-cs-tests.csproj" \
    --filter "Category=Contract" \
    --logger "console;verbosity=normal"

# Step 3: C# unit tests (excludes contract + integration to avoid double-running)
run_suite "C# gateway unit tests" \
  dotnet test "$REPO_ROOT/gateway-api-cs-tests/gateway-api-cs-tests.csproj" \
    --filter "Category!=Integration&Category!=Contract" \
    --logger "console;verbosity=normal"

if $RUN_INTEGRATION; then
  run_suite "C# gateway integration tests (Testcontainers)" \
    dotnet test "$REPO_ROOT/gateway-api-cs-tests/gateway-api-cs-tests.csproj" \
      --filter "Category=Integration" \
      --logger "console;verbosity=normal"
fi

if $RUN_E2E; then
  run_suite "End-to-end pipeline test (full containerized stack)" \
    "$REPO_ROOT/tools/e2e-test.sh" --down
fi

echo ""
echo "══════════════════════════════════════════"
printf "  Results: %d passed, %d failed\n" "$PASS" "$FAIL"
echo "══════════════════════════════════════════"
echo ""

[ "$FAIL" -eq 0 ] || exit 1
