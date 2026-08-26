#!/usr/bin/env python3
"""Copy the data worth keeping out of Postgres into a file you can open.

Writes a single portable SQLite database plus a CSV per table, so the corpus
survives without Railway, without Postgres, and without this repo.

    OUT_DIR=~/Desktop/glasswing-export \\
    CLASSIFICATION_DATABASE_URL="$DATABASE_PUBLIC_URL" \\
    python3 export_keepers.py

Read-only on the source. Nothing here deletes anything -- run it, check the
counts it prints, open the file, and only then run leanout_store.py.

WHAT COMES OUT, and why each one:

  classifications   the priority (investable) and comp buckets, plus any row
                    that had a Claude call, is tracked, or carries a human
                    decision -- the same predicate leanout_store keeps, so the
                    export and the deletion cannot disagree
  claude_diligence  every Claude call ever made, verdict included. The
                    expensive, irreplaceable table
  tracked_domains   explicit human tracking
  manual_decisions  human truth
  training_labels   human truth
  founder_*         the GitHub talent corpus. Kept because GitHub's events API
                    only returns recent activity, so accumulated history is the
                    one thing here that genuinely cannot be re-fetched
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(os.environ.get("BACKFILL_REPO", ".")).resolve()
sys.path.insert(0, str(REPO))
os.chdir(REPO)

import classification_store  # noqa: E402
import keeper_rules  # noqa: E402
from db_backend import PostgresConnection  # noqa: E402

OUT_DIR = Path(os.environ.get("OUT_DIR", "~/Desktop/glasswing-export")).expanduser()
# Imported, not restated: leanout_store.py deletes the complement of this, and
# if the two ever disagreed the export would be a promise about rows that had
# already gone.
KEEP_BUCKETS = keeper_rules.KEEP_BUCKETS
KEEP_PREDICATE = keeper_rules.keep_clause(KEEP_BUCKETS)

# table -> the WHERE clause that selects what to keep, or "" for everything.
EXPORTS = {
    "classifications": KEEP_PREDICATE,
    "claude_diligence": "",
    "claude_review_queue": "",
    "tracked_domains": "",
    "manual_decisions": "",
    "training_labels": "",
    "movement_log": "",
    "founder_candidates": "",
    "verified_github_identities": "",
    "founder_signals": "",
    "founder_github_events": "",
    "founder_github_repositories": "",
    "founder_analyst_reviews": "",
    "founder_score_snapshots": "",
    "github_identity_candidates": "",
}


def store(read_only: bool = True):
    # Never bare connect_store(): its path default binds at definition time.
    return classification_store.connect_store(
        classification_store.CLASSIFICATION_DB, read_only=read_only
    )


def table_columns(conn, table: str) -> list[str]:
    if isinstance(conn, PostgresConnection):
        rows = conn.execute(
            "SELECT column_name AS c FROM information_schema.columns "
            "WHERE table_name=? ORDER BY ordinal_position",
            (table,),
        ).fetchall()
        return [str(r["c"]) for r in rows]
    return [
        str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")
    ]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    target = OUT_DIR / f"glasswing_keepers_{stamp}.sqlite3"
    csv_dir = OUT_DIR / "csv"
    csv_dir.mkdir(exist_ok=True)
    if target.exists():
        target.unlink()
    out = sqlite3.connect(target)

    summary: list[tuple[str, int]] = []
    for table, predicate in EXPORTS.items():
        with store() as conn:
            columns = table_columns(conn, table)
            if not columns:
                print(f"skip {table} (not present)")
                continue
            where = ""
            params: list[object] = []
            if predicate:
                where = " WHERE " + predicate
                params = keeper_rules.keep_params(KEEP_BUCKETS)
            quoted = ",".join(f'"{c}"' for c in columns)
            rows = conn.execute(
                f"SELECT {quoted} FROM {table}{where}", params
            ).fetchall()

        # Everything lands as TEXT. This file is for reading and for reloading
        # later, not for running the pipeline against, and dodging type
        # mapping between two engines is worth more here than column affinity.
        definition = ", ".join(f'"{c}" TEXT' for c in columns)
        out.execute(f'CREATE TABLE "{table}" ({definition})')
        if rows:
            marks = ",".join("?" for _ in columns)
            out.executemany(
                f'INSERT INTO "{table}" VALUES ({marks})',
                [tuple(None if r[c] is None else str(r[c]) for c in columns)
                 for r in rows],
            )
        out.commit()

        with (csv_dir / f"{table}.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            for r in rows:
                writer.writerow(["" if r[c] is None else r[c] for c in columns])

        summary.append((table, len(rows)))
        print(f"{table}: {len(rows):,} rows", flush=True)

    out.execute("VACUUM")
    out.close()

    total = sum(n for _, n in summary)
    size_mb = target.stat().st_size / 1024 ** 2
    print(f"\n{total:,} rows across {len(summary)} tables")
    print(f"{target}  ({size_mb:,.1f} MB)")
    print(f"{csv_dir}  (one CSV per table)")
    print("\nCheck these counts before running leanout_store.py.")


if __name__ == "__main__":
    main()
