"""Copy the real local zone_universe corpus into its Postgres dual-backend.

Read-only against the local SQLite file -- this never modifies or deletes
the source. Requires ZONE_UNIVERSE_DATABASE_URL to be set; defaults to
--dry-run (count rows only, write nothing). Pass --execute to actually copy.

pages.id is explicitly preserved and its SERIAL sequence reset afterward.
After copying `pages`, search_vector is (re)computed per row directly
(zone_universe has no standalone index_page() -- that logic lives inline in
crawler.save_success(), which also does the actual crawl-saving work this
migration doesn't need), since SQLite's FTS5 index has no direct Postgres
equivalent to copy.

This does NOT flip the live app over to Postgres -- that only happens if
ZONE_UNIVERSE_DATABASE_URL is set in the actual Railway deployment, a
separate, deliberate step this script does not take.
"""

from __future__ import annotations

import argparse
import sqlite3

from common import ZONE_UNIVERSE_DB
from db_backend import using_postgres
from zone_universe.database import ZONE_UNIVERSE_DATABASE_ENV_VAR, connect

TABLES = (
    "zones", "shards", "pages", "worker_state", "event_log",
    "crawl_retries", "domain_registration", "registration_queue", "html_blobs",
)
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


def _populate_search_vector(pg_conn) -> int:
    rows = pg_conn.execute(
        "SELECT id, domain, title, meta_description, headings, cleaned_text "
        "FROM pages"
    ).fetchall()
    for i, row in enumerate(rows, 1):
        pg_conn.execute("""
            UPDATE pages SET search_vector =
                setweight(to_tsvector('english', coalesce(?, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'C') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'D') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'D')
            WHERE id = ?
        """, (
            row["domain"], row["title"], row["meta_description"],
            row["headings"], row["cleaned_text"], row["id"],
        ))
        if i % BATCH_SIZE == 0:
            pg_conn.commit()
    pg_conn.commit()
    return len(rows)


def migrate(*, execute: bool = False) -> dict[str, int]:
    if not using_postgres(ZONE_UNIVERSE_DATABASE_ENV_VAR):
        raise RuntimeError(
            f"Set {ZONE_UNIVERSE_DATABASE_ENV_VAR} before running this "
            "(it must point at the destination Postgres)."
        )
    if not ZONE_UNIVERSE_DB.exists():
        raise FileNotFoundError(
            f"No local zone_universe database at {ZONE_UNIVERSE_DB}"
        )

    sqlite_conn = sqlite3.connect(f"file:{ZONE_UNIVERSE_DB}?mode=ro", uri=True)
    sqlite_conn.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        pg_conn = connect("")
        try:
            for table in TABLES:
                counts[table] = _copy_table(
                    sqlite_conn, pg_conn, table, execute=execute
                )
            if execute:
                pg_conn.execute("""
                    SELECT setval(
                        pg_get_serial_sequence('pages', 'id'),
                        coalesce((SELECT max(id) FROM pages), 1)
                    )
                """)
                pg_conn.commit()
                counts["search_vector_populated"] = _populate_search_vector(pg_conn)
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
