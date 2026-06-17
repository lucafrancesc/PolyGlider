"""
MCP server for the PolyGlider polyglot inventory system.

Exposes four tools:
  list_inventory        — all SKUs + quantities from ledger
  get_sku_quantity      — single SKU lookup
  list_recent_events    — recent rows from processed_events
  place_order           — POST to the C# gateway

Environment variables (all optional, defaults shown):
  POSTGRES_URL   postgresql://postgres:postgres@localhost:5432/polyglider_inventory
  GATEWAY_URL    http://localhost:5187
"""

import os

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://postgres:postgres@localhost:5432/polyglider_inventory",
)
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:5187")

mcp = FastMCP("polyglider-inventory")


def _get_conn():
    return psycopg2.connect(POSTGRES_URL)


@mcp.tool()
def list_inventory() -> list[dict]:
    """Return all SKUs and their current quantities from the inventory ledger."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT sku, qty FROM ledger ORDER BY sku")
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    except psycopg2.OperationalError as e:
        return [{"error": f"Database unavailable: {e}"}]
    finally:
        if conn:
            conn.close()


@mcp.tool()
def get_sku_quantity(sku: str) -> dict:
    """Return the current quantity for a single SKU."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT sku, qty FROM ledger WHERE sku = %s", (sku,))
            row = cur.fetchone()
        if row is None:
            return {"error": f"SKU '{sku}' not found"}
        return dict(row)
    except psycopg2.OperationalError as e:
        return {"error": f"Database unavailable: {e}"}
    finally:
        if conn:
            conn.close()


@mcp.tool()
def list_recent_events(limit: int = 20) -> list[dict]:
    """Return the most recent processed order events. `limit` is clamped to [1, 1000]."""
    conn = None
    limit = max(1, min(limit, 1000))
    try:
        conn = _get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT event_id, processed_at FROM processed_events "
                "ORDER BY processed_at DESC LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
        return [{"event_id": r["event_id"], "processed_at": str(r["processed_at"])} for r in rows]
    except psycopg2.OperationalError as e:
        return [{"error": f"Database unavailable: {e}"}]
    finally:
        if conn:
            conn.close()


@mcp.tool()
def place_order(sku: str, quantity: int, customer_id: str) -> dict:
    """Place a new order through the C# gateway (POST /api/orders)."""
    payload = {
        "sku": sku,
        "quantity": quantity,
        "customerId": customer_id,
    }
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(f"{GATEWAY_URL}/api/orders", json=payload)
        if response.status_code == 202:
            return response.json()
        return {"error": f"Gateway returned {response.status_code}", "body": response.text}
    except httpx.ConnectError as e:
        return {"error": f"Gateway unreachable: {e}"}
    except httpx.TimeoutException as e:
        return {"error": f"Gateway timed out: {e}"}


if __name__ == "__main__":
    mcp.run()
