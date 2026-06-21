from unittest.mock import MagicMock

import httpx
import psycopg2
import pytest

import main


def _fake_conn(rows):
    """A connection whose cursor() context manager returns canned rows from fetchall/fetchone."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.fetchone.return_value = rows[0] if rows else None
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


@pytest.fixture
def patch_conn(monkeypatch):
    """Returns a function that wires main._get_conn/_put_conn to a fake connection."""

    def _patch(rows=None):
        conn, cursor = _fake_conn(rows or [])
        monkeypatch.setattr(main, "_get_conn", lambda: conn)
        put_calls = []
        monkeypatch.setattr(main, "_put_conn", lambda c, broken=False: put_calls.append(broken))
        return conn, cursor, put_calls

    return _patch


class TestListInventory:
    def test_happy_path(self, patch_conn):
        _, cursor, put_calls = patch_conn([{"sku": "SKU-1", "qty": 5}, {"sku": "SKU-2", "qty": 0}])
        result = main.list_inventory()
        assert result == [{"sku": "SKU-1", "qty": 5}, {"sku": "SKU-2", "qty": 0}]
        assert put_calls == [False]

    def test_db_error_raises_runtime_error_and_marks_connection_broken(self, patch_conn):
        conn, cursor, put_calls = patch_conn()
        cursor.execute.side_effect = psycopg2.Error("boom")
        with pytest.raises(RuntimeError, match="Database error"):
            main.list_inventory()
        assert put_calls == [True]


class TestGetSkuQuantity:
    def test_happy_path(self, patch_conn):
        patch_conn([{"sku": "SKU-1", "qty": 5}])
        result = main.get_sku_quantity("SKU-1")
        assert result == {"sku": "SKU-1", "qty": 5}

    def test_not_found_returns_error_dict(self, patch_conn):
        patch_conn([])
        result = main.get_sku_quantity("MISSING")
        assert result == {"error": "SKU 'MISSING' not found"}

    def test_db_error_raises_runtime_error(self, patch_conn):
        _, cursor, put_calls = patch_conn()
        cursor.execute.side_effect = psycopg2.Error("boom")
        with pytest.raises(RuntimeError, match="Database error"):
            main.get_sku_quantity("SKU-1")
        assert put_calls == [True]


class TestListRecentEvents:
    def test_happy_path(self, patch_conn):
        import datetime

        ts = datetime.datetime(2026, 6, 21, 12, 0, 0)
        patch_conn([{"event_id": "abc", "processed_at": ts}])
        result = main.list_recent_events(limit=5)
        assert result == [{"event_id": "abc", "processed_at": str(ts)}]

    @pytest.mark.parametrize(
        "requested,expected",
        [(0, 1), (-5, 1), (1, 1), (1000, 1000), (5000, 1000), (50, 50)],
    )
    def test_limit_is_clamped_to_1_1000(self, patch_conn, requested, expected):
        _, cursor, _ = patch_conn([])
        main.list_recent_events(limit=requested)
        _, params = cursor.execute.call_args
        assert cursor.execute.call_args[0][1] == (expected,)

    def test_db_error_raises_runtime_error(self, patch_conn):
        _, cursor, put_calls = patch_conn()
        cursor.execute.side_effect = psycopg2.Error("boom")
        with pytest.raises(RuntimeError, match="Database error"):
            main.list_recent_events()
        assert put_calls == [True]


class TestPlaceOrder:
    def _patch_client(self, monkeypatch, response=None, raise_exc=None):
        client = MagicMock()
        if raise_exc is not None:
            client.post.side_effect = raise_exc
        else:
            client.post.return_value = response
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        monkeypatch.setattr(main.httpx, "Client", lambda timeout=None: client)
        return client

    def test_202_returns_gateway_body(self, monkeypatch):
        response = MagicMock(status_code=202)
        response.json.return_value = {"eventId": "evt-1"}
        self._patch_client(monkeypatch, response=response)
        result = main.place_order("SKU-1", 3, "cust-1")
        assert result == {"eventId": "evt-1"}

    def test_non_202_returns_error_dict(self, monkeypatch):
        response = MagicMock(status_code=429, text="rate limited")
        self._patch_client(monkeypatch, response=response)
        result = main.place_order("SKU-1", 3, "cust-1")
        assert result == {"error": "Gateway returned 429", "body": "rate limited"}

    def test_connect_error_returns_error_dict(self, monkeypatch):
        self._patch_client(monkeypatch, raise_exc=httpx.ConnectError("refused"))
        result = main.place_order("SKU-1", 3, "cust-1")
        assert "Gateway unreachable" in result["error"]

    def test_timeout_returns_error_dict(self, monkeypatch):
        self._patch_client(monkeypatch, raise_exc=httpx.TimeoutException("timed out"))
        result = main.place_order("SKU-1", 3, "cust-1")
        assert "Gateway timed out" in result["error"]

    def test_sends_x_api_key_header_when_configured(self, monkeypatch):
        monkeypatch.setattr(main, "GATEWAY_API_KEY", "secret-key")
        response = MagicMock(status_code=202)
        response.json.return_value = {"eventId": "evt-1"}
        client = self._patch_client(monkeypatch, response=response)
        main.place_order("SKU-1", 3, "cust-1")
        _, kwargs = client.post.call_args
        assert kwargs["headers"] == {"X-Api-Key": "secret-key"}

    def test_omits_x_api_key_header_when_unset(self, monkeypatch):
        monkeypatch.setattr(main, "GATEWAY_API_KEY", None)
        response = MagicMock(status_code=202)
        response.json.return_value = {"eventId": "evt-1"}
        client = self._patch_client(monkeypatch, response=response)
        main.place_order("SKU-1", 3, "cust-1")
        _, kwargs = client.post.call_args
        assert kwargs["headers"] == {}


class TestTrackToolCall:
    def test_increments_success_counter_and_returns_value(self):
        @main.track_tool_call
        def ok():
            return "result"

        before = main.TOOL_CALLS.labels(tool="ok", outcome="success")._value.get()
        assert ok() == "result"
        after = main.TOOL_CALLS.labels(tool="ok", outcome="success")._value.get()
        assert after == before + 1

    def test_increments_error_counter_and_reraises(self):
        @main.track_tool_call
        def boom():
            raise ValueError("nope")

        before = main.TOOL_CALLS.labels(tool="boom", outcome="error")._value.get()
        with pytest.raises(ValueError):
            boom()
        after = main.TOOL_CALLS.labels(tool="boom", outcome="error")._value.get()
        assert after == before + 1
