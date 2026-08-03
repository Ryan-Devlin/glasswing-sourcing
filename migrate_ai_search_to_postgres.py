"""Copy the real local ai_search corpus into its Postgres dual-backend.

Read-only against the local SQLite file -- this never modifies or deletes
the source. Requires AI_SEARCH_DATABASE_URL to be set; defaults to
--dry-run (count rows only, write nothing). Pass --execute to actually copy.

Explicit primary keys are preserved (pages.domain_id and friends reference
them), so each SERIAL sequence is reset to MAX(id) afterward -- otherwise
the next real INSERT would collide with a copied row. After copying `pages`,
search_vector is (re)computed per row via index_page(), since SQLite's FTS5
index has no direct Postgres equivalent to copy.

This does NOT flip the live app over to Postgres -- that only happens if
AI_SEARCH_DATABASE_URL is set in the actual Railway deployment, a separate,
deliberate step this script does not take.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from urllib.parse import urlparse

from ai_search.database import AI_SEARCH_DATABASE_ENV_VAR, connect, index_page
from common import AI_SEARCH_DB
from db_backend import using_postgres

# Parent tables before the tables that reference them via foreign key.
TABLES_IN_FK_ORDER = (
    "domains", "watch_queries", "pages", "crawl_attempts", "shortlists",
    "query_discoveries", "worker_state", "html_blobs",
)
SEQUENCES = ("domains", "pages", "crawl_attempts", "watch_queries")
BATCH_SIZE = 2000
# Smaller than BATCH_SIZE: each search_vector row does 5 to_tsvector() calls
# (slower than a plain row copy), so commit more often -- less redone work,
# more frequent progress output if this step is ever interrupted.
SEARCH_VECTOR_BATCH_SIZE = 500


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]


def _sanitize(value):
    # Real crawled HTML/text occasionally contains embedded NUL (0x00)
    # bytes -- SQLite's TEXT columns tolerate them, Postgres's do not
    # ("PostgreSQL text fields cannot contain NUL (0x00) bytes"). Stripping
    # them is lossless in any practical sense: a NUL byte in page text is
    # encoding noise, never meaningful content.
    if isinstance(value, str) and "\x00" in value:
        return value.replace("\x00", "")
    return value


def _copy_table(
    sqlite_conn, pg_conn, table: str, *, execute: bool, progress: bool
) -> int:
    columns = _sqlite_columns(sqlite_conn, table)
    rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
    if not execute or not rows:
        return len(rows)
    column_list = ",".join(columns)
    placeholders = ",".join("?" for _ in columns)
    batch: list[tuple] = []
    copied = 0
    for row in rows:
        batch.append(tuple(_sanitize(row[c]) for c in columns))
        if len(batch) >= BATCH_SIZE:
            pg_conn.executemany(
                f"INSERT INTO {table}({column_list}) VALUES ({placeholders}) "
                f"ON CONFLICT DO NOTHING",
                batch,
            )
            pg_conn.commit()
            copied += len(batch)
            batch = []
            if progress:
                print(f"  {table}: {copied}/{len(rows)}", file=sys.stderr)
    if batch:
        pg_conn.executemany(
            f"INSERT INTO {table}({column_list}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING",
            batch,
        )
        pg_conn.commit()
        copied += len(batch)
    if progress:
        print(f"  {table}: {copied}/{len(rows)} (done)", file=sys.stderr)
    return len(rows)


def migrate(*, execute: bool = False, progress: bool = False) -> dict[str, int]:
    if not using_postgres(AI_SEARCH_DATABASE_ENV_VAR):
        raise RuntimeError(
            f"Set {AI_SEARCH_DATABASE_ENV_VAR} before running this "
            "(it must point at the destination Postgres)."
        )
    if not AI_SEARCH_DB.exists():
        raise FileNotFoundError(f"No local ai_search database at {AI_SEARCH_DB}")

    sqlite_conn = sqlite3.connect(f"file:{AI_SEARCH_DB}?mode=ro", uri=True)
    sqlite_conn.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        pg_conn = connect("")
        try:
            for table in TABLES_IN_FK_ORDER:
                counts[table] = _copy_table(
                    sqlite_conn, pg_conn, table, execute=execute, progress=progress
                )
            if execute:
                for table in SEQUENCES:
                    pg_conn.execute(f"""
                        SELECT setval(
                            pg_get_serial_sequence('{table}', 'id'),
                            coalesce((SELECT max(id) FROM {table}), 1)
                        )
                    """)
                pg_conn.commit()

                page_ids = [
                    row["id"] for row in pg_conn.execute("SELECT id FROM pages").fetchall()
                ]
                indexed = 0
                for page_id in page_ids:
                    index_page(pg_conn, page_id)
                    indexed += 1
                    if indexed % SEARCH_VECTOR_BATCH_SIZE == 0:
                        pg_conn.commit()
                        if progress:
                            print(
                                f"  search_vector: {indexed}/{len(page_ids)}",
                                file=sys.stderr,
                            )
                pg_conn.commit()
                if progress:
                    print(
                        f"  search_vector: {indexed}/{len(page_ids)} (done)",
                        file=sys.stderr,
                    )
                counts["search_vector_populated"] = indexed
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
        _confirm(AI_SEARCH_DATABASE_ENV_VAR)
    result = migrate(execute=args.execute, progress=args.execute)
    mode = "copied" if args.execute else "would copy (dry run)"
    print()
    for table, count in result.items():
        print(f"{table}: {mode} {count} rows")
