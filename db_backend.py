"""Dual SQL backend: local SQLite by default, managed Postgres in the cloud.

Selection is driven entirely by the presence of `DATABASE_URL` -- the same
standard connection-string variable used by Railway, Heroku, Render, Fly.io,
and most other platforms. That keeps the choice of cloud provider a config
change, never a code change, and means the app runs on local SQLite with zero
extra setup whenever `DATABASE_URL` is absent.

This module only adapts the small sqlite3.Connection surface the rest of the
codebase already relies on (`.execute()` returning a fetchable cursor with
dict-like rows, `.executemany()`, `.commit()`, context-manager semantics, and
`?` placeholders) so call sites in classification_store.py do not need two
copies of every query.
"""

from __future__ import annotations

import os
import re
import sys
import threading
import time
from typing import Any, Iterable


_POOLS: dict[tuple[str, str, bool], Any] = {}
_POOLS_LOCK = threading.Lock()


def using_postgres(env_var: str = "DATABASE_URL") -> bool:
    """Each logical database gets its own env-var gate.

    ai_search and zone_universe deliberately do NOT share the classification
    store's `DATABASE_URL` -- that variable is already set in production, so
    reusing it here would silently switch their search corpora over to an
    empty Postgres database on the next deploy. Pass e.g.
    `AI_SEARCH_DATABASE_URL` for those instead.
    """
    return bool(os.getenv(env_var, "").strip())


class PostgresConnection:
    """Adapts a psycopg connection to the sqlite3.Connection calling style."""

    def __init__(self, raw, release=None) -> None:
        self._raw = raw
        self._release = release
        self._released = False

    @staticmethod
    def _translate(sql: str) -> str:
        # The codebase writes SQLite-style positional `?` placeholders
        # everywhere; psycopg expects `%s`. No query text contains a literal
        # `?` character otherwise, so a blind replace is safe here.
        return sql.replace("?", "%s")

    @staticmethod
    def _clean_param(value: Any) -> Any:
        """Remove NUL bytes that PostgreSQL cannot store in text fields.

        Website text is untrusted input and malformed pages occasionally
        contain literal ``0x00`` characters. SQLite accepts them, but psycopg
        rejects the entire statement—and previously the crawler's whole
        concurrent batch—with DataError. Bytes/blob values are deliberately
        left untouched because NUL is valid binary data.
        """
        if isinstance(value, str) and "\x00" in value:
            return value.replace("\x00", "")
        return value

    @classmethod
    def _clean_params(cls, params: Iterable[Any]) -> tuple[Any, ...]:
        return tuple(cls._clean_param(value) for value in params)

    def execute(self, sql: str, params: Iterable[Any] = ()):
        cursor = self._raw.cursor()
        cursor.execute(self._translate(sql), self._clean_params(params))
        return cursor

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
        cursor = self._raw.cursor()
        cursor.executemany(
            self._translate(sql),
            [self._clean_params(params) for params in seq_of_params],
        )

    def executescript(self, sql: str) -> None:
        with self._raw.cursor() as cursor:
            cursor.execute(sql)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        if self._released:
            return
        self._released = True
        if self._release is not None:
            # A SELECT opens a transaction under psycopg's normal DB-API
            # semantics. Callers that explicitly close after reads expect a
            # rollback; do it here so the pool receives an IDLE connection
            # instead of logging and repairing an INTRANS session itself.
            try:
                self._raw.rollback()
            except Exception:
                self._raw.close()
            self._release(self._raw)
        else:
            self._raw.close()

    def copy_out(self, sql: str) -> bytes:
        """Run a `COPY ... TO STDOUT` and return the raw bytes produced.

        Used for logical backups: streaming a table out via COPY needs no
        external `pg_dump` binary, so it works identically on a bare Railway
        container, in CI, and on a developer's Mac.
        """
        chunks = bytearray()
        with self._raw.cursor() as cursor, cursor.copy(sql) as copy:
            for data in copy:
                chunks += bytes(data)
        return bytes(chunks)

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._raw.commit()
        else:
            self._raw.rollback()
        self.close()


def _connection_pool(env_var: str, read_only: bool):
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    url = os.environ[env_var]
    key = (env_var, url, read_only)
    with _POOLS_LOCK:
        pool = _POOLS.get(key)
        if pool is None:
            def configure(raw) -> None:
                if read_only:
                    with raw.cursor() as cursor:
                        cursor.execute("SET default_transaction_read_only = on")
                    raw.commit()

            pool = ConnectionPool(
                conninfo=url,
                min_size=0,
                max_size=max(1, int(os.getenv("GLASSWING_DB_POOL_SIZE", "8"))),
                timeout=10,
                max_idle=300,
                kwargs={
                    "row_factory": dict_row,
                    "autocommit": False,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 3,
                },
                configure=configure,
            )
            _POOLS[key] = pool
        return pool


def connect_postgres(
    *, read_only: bool = False, env_var: str = "DATABASE_URL"
) -> PostgresConnection:
    # A separate read-only pool prevents session settings from leaking into
    # write requests. Returning the wrapper closes the transaction normally,
    # then releases the live socket back to the bounded process-local pool.
    pool = _connection_pool(env_var, read_only)
    raw = pool.getconn()
    return PostgresConnection(raw, release=pool.putconn)


def begin_immediate(conn) -> None:
    """Start a write transaction. `BEGIN IMMEDIATE` is SQLite-only syntax
    (it asks SQLite to take the write lock up front rather than lazily);
    Postgres has no such modifier and doesn't need one for correctness here.
    """
    if isinstance(conn, PostgresConnection):
        conn.execute("BEGIN")
    else:
        conn.execute("BEGIN IMMEDIATE")


def acquire_advisory_lock_with_timeout(
    conn,
    key: str,
    *,
    timeout_seconds: float = 10.0,
    poll_seconds: float = 0.5,
    transactional: bool = False,
) -> bool:
    """pg_advisory_lock() blocks forever if never released -- and every
    startup migration path in this app (continuous_pipeline, ai_search,
    zone_universe, runtime_control) took exactly one of these, unconditionally,
    with no timeout. A process killed abruptly (an interrupted deploy, an OOM,
    a force-stopped container) while holding one leaves it stuck: Postgres
    only releases a session-level advisory lock when that connection's own
    socket closes, and a connection pooler in front of the database can keep
    that socket alive well after the client process is gone. Every future
    restart of that same service then hung forever waiting on a lock that
    would never be released -- confirmed live: three different workers each
    went silent immediately at startup after routine redeploys.

    Polls pg_try_advisory_lock() instead of blocking. On timeout, proceeds
    without the lock rather than hanging forever -- safe here because every
    caller only uses this to guard `CREATE TABLE IF NOT EXISTS`-style DDL,
    which is idempotent; the lock was always just uncontended-case race
    protection for the very first deploy, never something correctness
    actually depends on afterward.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        function = (
            "pg_try_advisory_xact_lock" if transactional
            else "pg_try_advisory_lock"
        )
        row = conn.execute(
            f"SELECT {function}(hashtext(?)) AS locked", (key,)
        ).fetchone()
        if bool(row["locked"]):
            return True
        time.sleep(poll_seconds)
    print(
        f"WARNING: advisory lock {key!r} still held after {timeout_seconds}s "
        "-- proceeding without it (likely a stale lock from a killed "
        "process; the guarded schema DDL is idempotent, so this is safe).",
        file=sys.stderr,
    )
    return False


def table_columns_postgres(conn: PostgresConnection, table: str) -> set[str]:
    cursor = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        (table,),
    )
    return {str(row["column_name"]) for row in cursor.fetchall()}


_DDL_OBJECT_PATTERN = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)(?:\s+IF\s+NOT\s+EXISTS)?\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
_DDL_CREATE_COUNT_PATTERN = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)\b", re.IGNORECASE
)


def schema_objects_in_script(script: str) -> set[str]:
    """Names of the tables and indexes a CREATE-only DDL script would define."""
    return {match.lower() for match in _DDL_OBJECT_PATTERN.findall(script)}


def postgres_schema_is_current(
    conn: PostgresConnection,
    schema: str,
    script: str,
) -> bool:
    """Whether every object a DDL script defines already exists in `schema`.

    `CREATE TABLE IF NOT EXISTS` still takes a conflicting lock on an
    already-existing table, so re-running a full schema script on every
    connect serializes against live writers. Confirmed live: the ai_search
    script ran on each of the ~5 connections the screening loop opened per
    batch and deadlocked against the crawler's own `INSERT INTO pages`,
    holding the migration advisory lock for 38 minutes and freezing screening
    entirely. Reading the catalog first takes only ACCESS SHARE, so the
    common case (schema already built) never reaches the DDL at all.
    """
    expected = schema_objects_in_script(script)
    # Fail safe: if any CREATE statement's name could not be parsed, fall back
    # to running the script rather than risk skipping an object that is
    # genuinely missing.
    if len(expected) != len(_DDL_CREATE_COUNT_PATTERN.findall(script)):
        return False
    if not expected:
        return True
    rows = conn.execute(
        """
        SELECT tablename AS name FROM pg_tables WHERE schemaname = ?
        UNION ALL
        SELECT indexname AS name FROM pg_indexes WHERE schemaname = ?
        """,
        (schema, schema),
    ).fetchall()
    present = {str(row["name"]).lower() for row in rows}
    return expected.issubset(present)
