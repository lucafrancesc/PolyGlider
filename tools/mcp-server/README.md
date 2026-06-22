# MCP Server — PolyGlider Inventory

Exposes the PolyGlider inventory state as [MCP](https://modelcontextprotocol.io) tools so an AI assistant (Claude Code, Claude Desktop, or any MCP-compatible client) can query orders and inventory without writing SQL.

The system has no read API — the C# gateway only accepts `POST /api/orders`. This server fills that gap by connecting directly to Postgres and exposing four tools over the stdio transport.

---

## Tools

| Tool | Description |
|------|-------------|
| `list_inventory` | All SKUs and their current quantities from the `ledger` table |
| `get_sku_quantity(sku)` | Current quantity for a single SKU |
| `list_recent_events(limit?)` | Most recent processed order events (default: 20) |
| `place_order(sku, quantity, customer_id)` | Place an order via the C# gateway |

---

## Setup

```bash
# From the repo root — infrastructure must be running
docker compose up -d

cd tools/mcp-server
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy the example env file if you need non-default credentials:

```bash
cp .env.example .env
# edit .env if your Postgres or gateway URLs differ from the defaults
```

Default values (used when no `.env` is present):

| Variable | Default |
|----------|---------|
| `POSTGRES_URL` | `postgresql://postgres:postgres@localhost:5432/polyglider_inventory` |
| `GATEWAY_URL` | `http://localhost:5187` |
| `GATEWAY_API_KEY` | unset — sent as `X-Api-Key` on `place_order` requests when present |

---

## Register with Claude Code

Run this once from anywhere:

```bash
claude mcp add polyglider-inventory \
  /path/to/PolyGlider/tools/mcp-server/.venv/bin/python \
  /path/to/PolyGlider/tools/mcp-server/main.py
```

Use the absolute path to the `.venv` Python so Claude Code uses the right interpreter with all dependencies installed.

Verify it connected:

```bash
claude mcp list
# polyglider-inventory: ... ✔ Connected
```

---

## Use it

Once registered, ask Claude anything about your inventory in natural language:

```
What's the current inventory?
How many LAPTOP-001 units do we have?
Show me the last 10 processed orders.
Place an order for 3 units of MONITOR-99 for customer 11111111-1111-4111-8111-111111111111.
```

Claude will call the appropriate tool and return the result.

---

## Test interactively (without Claude)

The `mcp[cli]` package includes a local inspector:

```bash
source .venv/bin/activate
mcp dev main.py
# Opens http://localhost:6274 (with an MCP_PROXY_AUTH_TOKEN query param) — call tools directly from the browser UI
```

### How it works

`mcp dev` launches the [MCP Inspector](https://github.com/modelcontextprotocol/inspector), which starts two local processes:

- A **proxy server** on port `6277` — speaks MCP to `main.py` over stdio (the same transport Claude Code uses), bridging it to HTTP/WebSocket for the browser
- A **client UI** on port `6274` — a web app that connects to the proxy and lets you browse this server's tools, fill in arguments via a generated form, and inspect raw request/response JSON

A session token (printed in the terminal output, also embedded in the URL `mcp dev` prints) authenticates the browser to the proxy — without it, requests are rejected. This is the same `main.py` process that `claude mcp add` would otherwise launch for Claude Code, so anything you can verify here behaves identically when called from Claude.

### Inspector CLI mode

For scripting or quick one-off checks without a browser, the inspector also has a non-interactive `--cli` mode:

```bash
# List all tools this server exposes, with their input schemas
npx @modelcontextprotocol/inspector --cli .venv/bin/python main.py --method tools/list

# Call a tool with arguments
npx @modelcontextprotocol/inspector --cli .venv/bin/python main.py --method tools/call \
  --tool-name list_inventory

npx @modelcontextprotocol/inspector --cli .venv/bin/python main.py --method tools/call \
  --tool-name get_sku_quantity --tool-arg sku=LAPTOP-001

npx @modelcontextprotocol/inspector --cli .venv/bin/python main.py --method tools/call \
  --tool-name list_recent_events --tool-arg limit=5

npx @modelcontextprotocol/inspector --cli .venv/bin/python main.py --method tools/call \
  --tool-name place_order --tool-arg sku=LAPTOP-001 --tool-arg quantity=2 \
  --tool-arg customer_id=22222222-2222-4222-8222-222222222222
```

Each call starts a fresh `main.py` process, runs the one request, prints the JSON result, and exits — no UI, no long-running session. Useful in shell scripts or CI smoke checks where you want to assert a tool's output without going through Claude.

---

## Run tests

```bash
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

Tests mock `_get_conn`/`_put_conn` and `httpx.Client` directly, so no Postgres or gateway needs to be running — `conftest.py` stubs out the module-level connection pool before `main.py` is imported.

---

## Verify

- `list_inventory` should return rows if the Scala engine has processed at least one order
- `place_order` requires the C# gateway to be running (`cd gateway-api-cs && dotnet run --project gateway-api-cs.csproj`)
- If Postgres is down, tools return `{"error": "Database unavailable: ..."}` rather than crashing the session
- `place_order` does **not** currently send an `X-Api-Key` header — if `Gateway__ApiKey` is set on the gateway (see the repo root README's [Security](../../README.md#security) section), `place_order` will get `401` with no way to configure a key on this side yet. Tracked in [#114](https://github.com/lucafrancesc/PolyGlider/issues/114).
