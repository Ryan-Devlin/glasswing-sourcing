"""Copy the real local pipeline queue into its Postgres dual-backend.

Read-only against the local SQLite file -- this never modifies or deletes
the source. Requires PIPELINE_DATABASE_URL to be set (see db_backend.py);
defaults to --dry-run (count rows only, write nothing) so the destination
is never touched by accident. Pass --execute to actually copy.

This does NOT flip the live app over to Postgres -- that only happens if
PIPELINE_DATABASE_URL is set in the actual Railway deployment, which is a
separate, deliberate step. This script exists to prove the copy itself
works correctly against real data before that step is ever considered.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from urllib.parse import urlparse

from common import QUEUE_DB
from continuous_pipeline import PIPELINE_DATABASE_ENV_VAR, _connect_db_postgres
from db_backend import using_postgres

TABLES = ("domains", "claude_calls", "registry_meta")
BATCH_SIZE = 5000


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]


def migrate(*, execute: bool = False, progress: bool = False) -> dict[str, int]:
    if not using_postgres(PIPELINE_DATABASE_ENV_VAR):
        raise RuntimeError(
            f"Set {PIPELINE_DATABASE_ENV_VAR} before running this "
            "(it must point at the destination Postgres)."
        )
    if not QUEUE_DB.exists():
        raise FileNotFoundError(f"No local queue database at {QUEUE_DB}")

    sqlite_conn = sqlite3.connect(f"file:{QUEUE_DB}?mode=ro", uri=True)
    sqlite_conn.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        pg_conn = _connect_db_postgres()
        try:
            for table in TABLES:
                columns = _sqlite_columns(sqlite_conn, table)
                rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
                counts[table] = len(rows)
                if not execute or not rows:
                    continue
                column_list = ",".join(columns)
                placeholders = ",".join("?" for _ in columns)
                batch: list[tuple] = []
                copied = 0
                for row in rows:
                    batch.append(tuple(row[c] for c in columns))
                    if len(batch) >= BATCH_SIZE:
                        pg_conn.executemany(
                            f"INSERT INTO {table}({column_list}) "
                            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                            batch,
                        )
                        pg_conn.commit()
                        copied += len(batch)
                        batch = []
                        if progress:
                            print(f"  {table}: {copied}/{len(rows)}", file=sys.stderr)
                if batch:
                    pg_conn.executemany(
                        f"INSERT INTO {table}({column_list}) "
                        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                        batch,
                    )
                    pg_conn.commit()
                    copied += len(batch)
                if progress:
                    print(f"  {table}: {copied}/{len(rows)} (done)", file=sys.stderr)
        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()
    return counts


def _confirm(env_var: str) -> None:
    url = os.environ[env_var]
    parsed = urlparse(url)
    host = parsed.hostname or "unknown-host"
    dbname = (parsed.path or "").lstrip("/") or "unknown-db"
    print(f"About to WRITE real data into Postgres at {host}/{dbname}.")
    if input("Type 'yes' to continue: ").strip().lower() != "yes":
        print("Aborted -- nothing written.")
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually copy rows into Postgres (default: dry run, count only)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation prompt (for automation)",
    )
    args = parser.parse_args()
    if args.execute and not args.yes:
        _confirm(PIPELINE_DATABASE_ENV_VAR)
    result = migrate(execute=args.execute, progress=args.execute)
    mode = "copied" if args.execute else "would copy (dry run)"
    print()
    for table, count in result.items():
        print(f"{table}: {mode} {count} rows")
