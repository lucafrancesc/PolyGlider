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
# Opens http://localhost:5173 — call tools directly from the browser UI
```

---

## Verify

- `list_inventory` should return rows if the Scala engine has processed at least one order
- `place_order` requires the C# gateway to be running (`cd gateway-api-cs && dotnet run`)
- If Postgres is down, tools return `{"error": "Database unavailable: ..."}` rather than crashing the session
