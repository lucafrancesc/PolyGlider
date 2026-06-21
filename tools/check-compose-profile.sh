#!/usr/bin/env bash
# Guards against the `containerized` Compose profile silently losing its scoping --
# verifies a plain `docker compose config` excludes gateway/engine/nginx, and that
# `--profile containerized` includes them. Uses `config --services`, which resolves
# profile gating statically without starting any containers.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
CONTAINERIZED_ONLY=(gateway engine nginx)

RED='\033[0;31m'; GREEN='\033[0;32m'; RESET='\033[0m'
die() { echo -e "${RED}[check-compose-profile] ERROR:${RESET} $*" >&2; exit 1; }

default_services=$(docker compose -f "$COMPOSE_FILE" config --services)
containerized_services=$(docker compose -f "$COMPOSE_FILE" --profile containerized config --services)

for service in "${CONTAINERIZED_ONLY[@]}"; do
  if echo "$default_services" | grep -qx "$service"; then
    die "'$service' appears in the default profile -- it must stay scoped to 'profiles: [\"containerized\"]'"
  fi
  if ! echo "$containerized_services" | grep -qx "$service"; then
    die "'$service' is missing from the 'containerized' profile"
  fi
done

echo -e "${GREEN}[check-compose-profile]${RESET} OK -- ${CONTAINERIZED_ONLY[*]} are containerized-only, as expected."
