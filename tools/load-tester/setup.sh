#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

echo "Setup complete. Activate with: source tools/load-tester/.venv/bin/activate"
