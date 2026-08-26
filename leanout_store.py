#!/usr/bin/env python3
"""Reduce the store to the records worth keeping, in small committed batches.

WHAT IS KEPT, and nothing else:

  * every ``priority`` classification -- the investable bucket, and the only
    one discard_uninvestable_classifications has ever treated as durable
  * every ``comp`` classification -- 760 rows of comparables, kept because it
    is bucketed work and costs almost nothing
  * every domain in ``claude_diligence`` -- every company that ever cost a
    Claude call, whatever the verdict was
  * every domain in ``tracked_domains`` -- explicit human tracking
  * every domain in ``manual_decisions`` and ``training_labels`` -- human
    truth, which is protected here exactly as it is everywhere else

Everything else in ``classifications`` goes: reject, insufficient and review,
plus the queue, exclusion and run-membership rows that hang off them. Python
scorer output is a signal and not truth, so a review row with no Claude call
and no human decision is a machine opinion about a domain we can screen again.

WAL, NOT JUST TABLE SIZE. On 2026-08-25 the compactor filled this volume and
took Postgres down. Its guard watched pg_database_size, which does not count
pg_wal -- so the one check meant to prevent that could not see the thing doing
it. This watches both, and it refuses to start at all unless there is real
headroom, rather than trusting a ratio.

DELETE leaves dead tuples behind, so the files do not shrink until VACUUM FULL
runs. That is the point of doing this in two phases: the delete pass makes the
surviving set small, and VACUUM FULL then rewrites each table to a file sized
for what survived. It needs room for a copy of the *kept* rows only, which
after this is a few megabytes.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

REPO = Path(os.environ.get("BACKFILL_REPO", ".")).resolve()
sys.path.insert(0, str(REPO))
os.chdir(REPO)

import classification_store  # noqa: E402
from db_backend import PostgresConnection  # noqa: E402


def store(*, read_only: bool = False):
    """Open the store, resolving its location at call time.

    Never ``connect_store()`` bare. Its ``path`` default is bound when the
    function is defined, so pointing CLASSIFICATION_DB somewhere else has no
    effect on it -- which meant an early version of the test for this file
    silently ran against the real local store and cleared three of its tables.
    Passing the attribute explicitly is also correct in cloud mode, where
    connect_store ignores the path and uses the configured Postgres URL.
    """
    return classification_store.connect_store(
        classification_store.CLASSIFICATION_DB, read_only=read_only
    )

DRY = os.environ.get("DRY_RUN", "1") == "1"
BATCH = max(100, int(os.environ.get("BATCH", "2000")))
PAUSE = float(os.environ.get("PAUSE", "0.3"))
# Refuse to run without this much room. The failure this exists to prevent
# needed only 16 MB to avoid, and it still happened, because nothing checked.
MIN_FREE_MB = float(os.environ.get("MIN_FREE_MB", "1500"))
CAPACITY_MB = float(os.environ.get("POSTGRES_VOLUME_MB", "10000"))

# Bucketed work that survives on its own merit.
KEEP_BUCKETS = tuple(
    b.strip() for b in os.environ.get("KEEP_BUCKETS", "priority,comp").split(",")
    if b.strip()
)
# Tables whose rows are only meaningful next to a classification we kept.
# Discovered at runtime rather than hardcoded: these live in several modules'
# schemas and a missing one must be skipped, not crash the run.
DOMAIN_SCOPED = (
    "classifications",
    "automated_exclusions",
    "classification_run_membership",
    "claude_review_queue",
    "hidden_domains",
    "movement_log",
    "daily_digest_items",
    "domains",
    "pages",
    "revisit_queue",
)
# Pure transient state: queue mechanics and logs, meaningless once stopped.
TRUNCATE = (
    "crawl_attempts",
    "crawl_retries",
    "event_log",
    "admin_audit_log",
    "competitive_landscape_jobs",
    "daily_digest_runs",
)


def sizes(conn) -> tuple[float, float]:
    """Database megabytes and WAL megabytes, which share one volume.

    WAL is the half the last guard could not see. pg_database_size excludes
    pg_wal entirely, so a rewrite can fill the disk while the number the guard
    watches barely moves -- which is precisely what happened on 2026-08-25.
    """
    if not isinstance(conn, PostgresConnection):
        # Local sqlite: one file, no WAL volume to exhaust.
        database = Path(classification_store.CLASSIFICATION_DB)
        return (database.stat().st_size / 1024 ** 2 if database.exists() else 0.0), 0.0
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
        # pg_ls_waldir needs pg_monitor on some installs. Not knowing the WAL
        # size is not a licence to ignore it, so assume the worst: that it
        # accounts for every megabyte unaccounted for.
        wal_mb = max(0.0, CAPACITY_MB - database_mb)
    return database_mb, wal_mb


def has_table(conn, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def columns(conn, table: str) -> set[str]:
    if isinstance(conn, PostgresConnection):
        rows = conn.execute(
            "SELECT column_name AS c FROM information_schema.columns "
            "WHERE table_name=?",
            (table,),
        ).fetchall()
        return {str(r["c"]) for r in rows}
    return {
        str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")
    }


KEEP_SQL = """
    domain IN (SELECT domain FROM claude_diligence)
 OR domain IN (SELECT domain FROM tracked_domains)
 OR domain IN (SELECT domain FROM manual_decisions)
 OR domain IN (SELECT domain FROM training_labels)
 OR domain IN (SELECT domain FROM classifications WHERE routing_bucket IN ({marks}))
"""


def keep_clause() -> str:
    return KEEP_SQL.format(marks=",".join("?" for _ in KEEP_BUCKETS))


def main() -> None:
    with store(read_only=True) as conn:
        database_mb, wal_mb = sizes(conn)
        free_mb = CAPACITY_MB - database_mb - wal_mb
        print(f"database {database_mb:,.0f} MB  wal {wal_mb:,.0f} MB  "
              f"free {free_mb:,.0f} MB of {CAPACITY_MB:,.0f} MB")
        if free_mb < MIN_FREE_MB:
            print(
                f"REFUSING TO RUN: {free_mb:,.0f} MB free is under the "
                f"{MIN_FREE_MB:,.0f} MB floor. Deleting rows writes WAL before "
                "it frees anything, and filling this volume is what took the "
                "database down once already.",
                file=sys.stderr,
            )
            raise SystemExit(1)

        keep = keep_clause()
        kept = int(conn.execute(
            f"SELECT count(*) AS n FROM classifications WHERE {keep}",
            list(KEEP_BUCKETS),
        ).fetchone()["n"])
        total = int(conn.execute(
            "SELECT count(*) AS n FROM classifications"
        ).fetchone()["n"])
        print(f"classifications: {total:,} total, {kept:,} kept, "
              f"{total - kept:,} to delete")
        for bucket in KEEP_BUCKETS:
            n = int(conn.execute(
                "SELECT count(*) AS n FROM classifications WHERE routing_bucket=?",
                (bucket,),
            ).fetchone()["n"])
            print(f"   bucket {bucket}: {n:,}")
        for table in ("claude_diligence", "tracked_domains", "manual_decisions",
                      "training_labels"):
            if has_table(conn, table):
                n = int(conn.execute(
                    f"SELECT count(*) AS n FROM {table}"
                ).fetchone()["n"])
                print(f"   {table}: {n:,} (all kept)")

    if DRY:
        print("\ndry run -- nothing deleted")
        return

    for table in DOMAIN_SCOPED:
        with store(read_only=True) as conn:
            if not has_table(conn, table) or "domain" not in columns(conn, table):
                print(f"skip {table} (absent, or no domain column)")
                continue
        removed = 0
        while True:
            # Batched by primary-key-ish slice rather than one huge statement:
            # a single DELETE of half a million rows is one transaction's worth
            # of WAL, which is the shape of the last failure.
            with store() as conn:
                cursor = conn.execute(
                    f"DELETE FROM {table} WHERE domain IN ("
                    f"  SELECT domain FROM {table} WHERE NOT ({keep_clause()}) "
                    f"  LIMIT {BATCH})",
                    list(KEEP_BUCKETS),
                )
                conn.commit()
            n = max(0, int(cursor.rowcount or 0))
            removed += n
            if n == 0:
                break
            time.sleep(PAUSE)
            with store(read_only=True) as conn:
                database_mb, wal_mb = sizes(conn)
            if CAPACITY_MB - database_mb - wal_mb < MIN_FREE_MB / 3:
                print(f"\nSTOPPING on low free space while clearing {table}",
                      file=sys.stderr)
                raise SystemExit(1)
            print(f"  {table}: removed {removed:,}", flush=True)
        print(f"{table}: {removed:,} rows removed")

    for table in TRUNCATE:
        with store() as conn:
            if not has_table(conn, table):
                print(f"skip {table} (absent)")
                continue
            conn.execute(f"DELETE FROM {table}")
            conn.commit()
        print(f"{table}: cleared")

    with store(read_only=True) as conn:
        database_mb, wal_mb = sizes(conn)
    print(f"\nafter delete: database {database_mb:,.0f} MB  wal {wal_mb:,.0f} MB")
    print("run leanout_vacuum.py next -- DELETE does not shrink the files")


if __name__ == "__main__":
    main()
