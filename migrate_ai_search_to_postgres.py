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
import sqlite3

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


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]


def _copy_table(sqlite_conn, pg_conn, table: str, *, execute: bool) -> int:
    columns = _sqlite_columns(sqlite_conn, table)
    rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
    if not execute or not rows:
        return len(rows)
    column_list = ",".join(columns)
    placeholders = ",".join("?" for _ in columns)
    batch: list[tuple] = []
    for row in rows:
        batch.append(tuple(row[c] for c in columns))
        if len(batch) >= BATCH_SIZE:
            pg_conn.executemany(
                f"INSERT INTO {table}({column_list}) VALUES ({placeholders}) "
                f"ON CONFLICT DO NOTHING",
                batch,
            )
            pg_conn.commit()
            batch = []
    if batch:
        pg_conn.executemany(
            f"INSERT INTO {table}({column_list}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING",
            batch,
        )
        pg_conn.commit()
    return len(rows)


def migrate(*, execute: bool = False) -> dict[str, int]:
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
                    sqlite_conn, pg_conn, table, execute=execute
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

                indexed = 0
                for row in pg_conn.execute("SELECT id FROM pages").fetchall():
                    index_page(pg_conn, row["id"])
                    indexed += 1
                    if indexed % BATCH_SIZE == 0:
                        pg_conn.commit()
                pg_conn.commit()
                counts["search_vector_populated"] = indexed
        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually copy rows into Postgres (default: dry run, count only)",
    )
    args = parser.parse_args()
    result = migrate(execute=args.execute)
    mode = "copied" if args.execute else "would copy (dry run)"
    for table, count in result.items():
        print(f"{table}: {mode} {count} rows")
