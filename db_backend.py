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
from typing import Any, Iterable


def using_postgres() -> bool:
    return bool(os.getenv("DATABASE_URL", "").strip())


class PostgresConnection:
    """Adapts a psycopg connection to the sqlite3.Connection calling style."""

    def __init__(self, raw) -> None:
        self._raw = raw

    @staticmethod
    def _translate(sql: str) -> str:
        # The codebase writes SQLite-style positional `?` placeholders
        # everywhere; psycopg expects `%s`. No query text contains a literal
        # `?` character otherwise, so a blind replace is safe here.
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: Iterable[Any] = ()):
        cursor = self._raw.cursor()
        cursor.execute(self._translate(sql), tuple(params))
        return cursor

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
        cursor = self._raw.cursor()
        cursor.executemany(
            self._translate(sql),
            [tuple(params) for params in seq_of_params],
        )

    def executescript(self, sql: str) -> None:
        with self._raw.cursor() as cursor:
            cursor.execute(sql)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._raw.commit()
        else:
            self._raw.rollback()
        self._raw.close()


def connect_postgres(*, read_only: bool = False) -> PostgresConnection:
    import psycopg
    from psycopg.rows import dict_row

    raw = psycopg.connect(
        os.environ["DATABASE_URL"],
        row_factory=dict_row,
        autocommit=False,
    )
    if read_only:
        with raw.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
        raw.commit()
    return PostgresConnection(raw)


def begin_immediate(conn) -> None:
    """Start a write transaction. `BEGIN IMMEDIATE` is SQLite-only syntax
    (it asks SQLite to take the write lock up front rather than lazily);
    Postgres has no such modifier and doesn't need one for correctness here.
    """
    if isinstance(conn, PostgresConnection):
        conn.execute("BEGIN")
    else:
        conn.execute("BEGIN IMMEDIATE")


def table_columns_postgres(conn: PostgresConnection, table: str) -> set[str]:
    cursor = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        (table,),
    )
    return {str(row["column_name"]) for row in cursor.fetchall()}
