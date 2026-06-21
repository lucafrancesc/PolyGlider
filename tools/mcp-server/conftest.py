"""Patches the Postgres connection pool before main.py is imported.

main.py opens a real psycopg2 connection pool (minconn=1) as a module-level side effect of
import, so importing it for tests would otherwise require a live Postgres instance. Tests
never exercise the real pool directly -- they monkeypatch main._get_conn/_put_conn -- so a
dummy stand-in here is enough to make the import safe in a Postgres-less test environment.
"""

from unittest.mock import MagicMock

import psycopg2.pool

psycopg2.pool.SimpleConnectionPool = MagicMock(return_value=MagicMock(closed=False))
