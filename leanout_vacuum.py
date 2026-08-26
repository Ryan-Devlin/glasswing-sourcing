#!/usr/bin/env python3
"""Return the space the lean-out freed, one table at a time.

DELETE marks tuples dead; it does not shrink the file. Until something
rewrites the table, a store that just lost 450,000 of 452,000 rows still
occupies every byte it did before, and the volume is no emptier. VACUUM FULL
is the rewrite.

Run this only *after* leanout_store.py. VACUUM FULL builds a fresh copy of the
table before swapping it in, so its peak requirement is the size of what
survives -- a few megabytes here, against the 2.4 GB the table was. Running it
first, on the full table, would need 2.4 GB of free space and is exactly the
kind of thing that filled this volume on 2026-08-25.

Table at a time, largest last, with the free space re-checked between each,
so a surprise stops the run instead of the disk.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(os.environ.get("BACKFILL_REPO", ".")).resolve()
sys.path.insert(0, str(REPO))
os.chdir(REPO)

import classification_store  # noqa: E402
from db_backend import PostgresConnection, connect_postgres  # noqa: E402

CAPACITY_MB = float(os.environ.get("POSTGRES_VOLUME_MB", "10000"))
MIN_FREE_MB = float(os.environ.get("MIN_FREE_MB", "500"))
DRY = os.environ.get("DRY_RUN", "1") == "1"

# Smallest first: each one that completes frees space for the next, so the
# biggest table faces the most headroom rather than the least.
TABLES = (
    "movement_log",
    "hidden_domains",
    "claude_review_queue",
    "daily_digest_items",
    "training_labels",
    "manual_decisions",
    "tracked_domains",
    "claude_diligence",
    "revisit_queue",
    "crawl_attempts",
    "classification_run_membership",
    "automated_exclusions",
    "pages",
    "classifications",
    "domains",
)


def table_bytes(conn, table: str) -> int:
    try:
        return int(
            conn.execute(
                "SELECT pg_total_relation_size(?) AS n", (table,)
            ).fetchone()["n"]
        )
    except Exception:
        return 0


def free_mb(conn) -> float:
    database_mb = int(
        conn.execute(
            "SELECT pg_database_size(current_database()) AS n"
        ).fetchone()["n"]
    ) / 1024 ** 2
    try:
        wal_mb = float(
            conn.execute(
                "SELECT coalesce(sum(size),0) AS n FROM pg_ls_waldir()"
            ).fetchone()["n"]
        ) / 1024 ** 2
    except Exception:
        wal_mb = max(0.0, CAPACITY_MB - database_mb)
    return CAPACITY_MB - database_mb - wal_mb


def main() -> None:
    if not classification_store.using_classification_postgres():
        print("VACUUM FULL is a Postgres operation; nothing to do on sqlite.")
        return

    # VACUUM FULL cannot run inside a transaction block, so it needs a raw
    # autocommit connection rather than the pooled wrapper the app uses.
    conn = connect_postgres()
    raw = conn._raw
    previous_autocommit = raw.autocommit
    raw.autocommit = True
    try:
        start = free_mb(conn)
        print(f"free before: {start:,.0f} MB of {CAPACITY_MB:,.0f} MB")
        for table in TABLES:
            before = table_bytes(conn, table)
            if not before:
                print(f"skip {table} (absent or empty)")
                continue
            room = free_mb(conn)
            if room < MIN_FREE_MB:
                print(f"STOPPING before {table}: only {room:,.0f} MB free",
                      file=sys.stderr)
                raise SystemExit(1)
            if DRY:
                print(f"would vacuum {table}: {before / 1024**2:,.1f} MB")
                continue
            conn.execute(f"VACUUM (FULL, ANALYZE) {table}")
            after = table_bytes(conn, table)
            print(
                f"{table}: {before / 1024**2:,.1f} MB -> {after / 1024**2:,.1f} MB",
                flush=True,
            )
        end = free_mb(conn)
        print(f"\nfree after: {end:,.0f} MB of {CAPACITY_MB:,.0f} MB "
              f"({end - start:+,.0f} MB)")
        if DRY:
            print("dry run -- nothing vacuumed")
    finally:
        raw.autocommit = previous_autocommit
        if isinstance(conn, PostgresConnection):
            conn.close()


if __name__ == "__main__":
    main()
