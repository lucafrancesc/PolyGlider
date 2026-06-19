"""
MCP server for the PolyGlider polyglot inventory system.

Exposes four tools:
  list_inventory        — all SKUs + quantities from ledger
  get_sku_quantity      — single SKU lookup
  list_recent_events    — recent rows from processed_events
  place_order           — POST to the C# gateway

Environment variables (all optional, defaults shown):
  POSTGRES_URL      postgresql://postgres:postgres@localhost:5432/polyglider_inventory
  GATEWAY_URL       http://localhost:5187
  POSTGRES_POOL_MAX 10
  METRICS_PORT      9101
"""

import atexit
import logging
import os
import time
from functools import wraps

import httpx
import psycopg2
import psycopg2.extras
import psycopg2.pool
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("polyglider-inventory")

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://postgres:postgres@localhost:5432/polyglider_inventory",
)
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:5187")
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX", "10"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))

mcp = FastMCP("polyglider-inventory")

TOOL_CALLS = Counter(
    "mcp_tool_calls_total", "MCP tool invocations", ["tool", "outcome"]
)
TOOL_CALL_DURATION = Histogram(
    "mcp_tool_call_duration_seconds", "MCP tool call duration in seconds", ["tool"]
)


def track_tool_call(func):
    """Records call count (by outcome) and duration for an MCP tool function.

    Applied as the innermost decorator (under @mcp.tool()) so it wraps the same plain
    function FastMCP registers -- @mcp.tool() returns its argument unchanged, it doesn't
    wrap it, so decoration order here doesn't affect what gets registered.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        outcome = "success"
        try:
            return func(*args, **kwargs)
        except Exception:
            outcome = "error"
            raise
        finally:
            TOOL_CALLS.labels(tool=func.__name__, outcome=outcome).inc()
            TOOL_CALL_DURATION.labels(tool=func.__name__).observe(time.monotonic() - start)

    return wrapper

# psycopg2's pool only keeps up to `minconn` idle connections around for reuse — with
# minconn=0 every putconn() would close the connection and the next call would open a fresh
# one, which is no pooling at all. minconn=1 means the server needs Postgres reachable at
# startup (this server has nothing useful to do without it anyway), but actually reuses
# connections instead of opening one per call.
_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=POSTGRES_POOL_MAX, dsn=POSTGRES_URL)

def _close_pool() -> None:
    # closeall() raises PoolError if the pool is already closed -- guard so this is safe to
    # call more than once (e.g. an explicit shutdown call plus atexit both firing).
    if _pool.closed:
        return
    logger.info("Shutting down: closing pooled Postgres connections")
    _pool.closeall()


# atexit (not a signal handler) so this runs on every clean interpreter exit -- SIGINT raising
# KeyboardInterrupt and unwinding to process exit, SIGTERM's default handler raising SystemExit,
# or main() returning normally all go through this, without needing to duplicate cleanup logic
# in each path.
atexit.register(_close_pool)


def _get_conn():
    return _pool.getconn()


def _put_conn(conn, *, broken: bool = False) -> None:
    # A connection that errored mid-use may be in an unknown state (e.g. an aborted
    # transaction); discard it instead of returning it to the pool for reuse.
    _pool.putconn(conn, close=broken)


@mcp.tool()
@track_tool_call
def list_inventory() -> list[dict]:
    """Return all SKUs and their current quantities from the inventory ledger."""
    conn = None
    broken = False
    try:
        conn = _get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT sku, qty FROM ledger ORDER BY sku")
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    except psycopg2.Error as e:
        broken = True
        logger.error("list_inventory failed: %s", e)
        raise RuntimeError(f"Database error: {e}") from e
    finally:
        if conn:
            _put_conn(conn, broken=broken)


@mcp.tool()
@track_tool_call
def get_sku_quantity(sku: str) -> dict:
    """Return the current quantity for a single SKU."""
    conn = None
    broken = False
    try:
        conn = _get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT sku, qty FROM ledger WHERE sku = %s", (sku,))
            row = cur.fetchone()
        if row is None:
            return {"error": f"SKU '{sku}' not found"}
        return dict(row)
    except psycopg2.Error as e:
        broken = True
        logger.error("get_sku_quantity failed for sku=%s: %s", sku, e)
        raise RuntimeError(f"Database error: {e}") from e
    finally:
        if conn:
            _put_conn(conn, broken=broken)


@mcp.tool()
@track_tool_call
def list_recent_events(limit: int = 20) -> list[dict]:
    """Return the most recent processed order events. `limit` is clamped to [1, 1000]."""
    conn = None
    broken = False
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
    except psycopg2.Error as e:
        broken = True
        logger.error("list_recent_events failed: %s", e)
        raise RuntimeError(f"Database error: {e}") from e
    finally:
        if conn:
            _put_conn(conn, broken=broken)


@mcp.tool()
@track_tool_call
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
            body = response.json()
            # eventId is generated once by the gateway and carried unchanged through the
            # RabbitMQ event, retries, and the DLQ/reprocessor — logging it here lets an
            # operator grep the same id across gateway and Scala consumer logs.
            logger.info("place_order succeeded: eventId=%s sku=%s quantity=%d", body.get("eventId"), sku, quantity)
            return body
        logger.warning("place_order failed: gateway returned %d for sku=%s", response.status_code, sku)
        return {"error": f"Gateway returned {response.status_code}", "body": response.text}
    except httpx.ConnectError as e:
        logger.error("place_order failed: gateway unreachable for sku=%s: %s", sku, e)
        return {"error": f"Gateway unreachable: {e}"}
    except httpx.TimeoutException as e:
        logger.error("place_order failed: gateway timed out for sku=%s: %s", sku, e)
        return {"error": f"Gateway timed out: {e}"}


if __name__ == "__main__":
    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics exposed on :%d/metrics", METRICS_PORT)
    mcp.run()
