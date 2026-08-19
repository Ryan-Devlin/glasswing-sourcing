#!/usr/bin/env python3
"""Write the Claude diligence candidate CSV in a short-lived child process.

This exists purely for memory isolation. Building the candidate file means
loading every classification row -- each carrying a full `record_json` website
copy payload -- into a DataFrame, because claude_classifier's eligibility gate
re-scores that copy live (see its prepare_input). At ~99k rows that is ~1.7 GB
resident.

Doing it inside the always-on pipeline worker pushed that worker past its own
GLASSWING_PIPELINE_RECYCLE_RSS_MB ceiling (1200 MB), so the post-batch check in
continuous_pipeline's worker loop os.execv'd the process after essentially
every batch. Confirmed live: the dashboard counted ~24 domains -- exactly one
surge wave -- then reset to zero, over and over, because every per-run counter
resets when the process is replaced.

Run as a subprocess, the peak lives in a process that exits immediately after
writing the file, so the worker's own RSS never grows.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


_RESCUE_CURSOR_KEY = "claude_rescue_scan_offset"


def pool_domains(limit: int) -> list[str]:
    """The best un-diligenced candidates, straight from SQL.

    Three things happen here that used to happen after loading the corpus:

    * **Already-diligenced rows are excluded.** ``prepare_input`` drops them
      anyway, so carrying them through the CSV is pure weight.
    * **Ordered by score.** The classifier only ever calls at most a hundred,
      so it needs the top of the pool, not all of it.
    * **Bounded.** Passing every candidate in history produced a 454 MB CSV
      that the classifier then loaded with pandas -- putting memory back where
      the OOM fix had just moved it away from.

    Manual overrides are kept regardless of score or diligence state, since an
    operator asking for a domain outranks all of this.
    """
    from classification_store import CLASSIFICATION_DB, connect_store

    with connect_store(CLASSIFICATION_DB, read_only=True) as conn:
        rows = conn.execute(
            "SELECT c.domain FROM classifications c "
            "LEFT JOIN claude_diligence g ON g.domain = c.domain "
            "WHERE (c.routing_bucket IN ('review','priority','comp') "
            "       OR COALESCE(c.manual_override, 0) <> 0) "
            "  AND (g.domain IS NULL OR COALESCE(c.manual_override, 0) <> 0) "
            "ORDER BY COALESCE(c.priority_score, 0) DESC, "
            "         COALESCE(c.fit_score, 0) DESC "
            f"LIMIT {int(limit)}"
        ).fetchall()
    return [str(row["domain"]) for row in rows if row["domain"]]


def eligible_domains(
    minimum_score: int, rescue_limit: int, pool_limit: int
) -> list[str]:
    """Domains that could clear the diligence bar, found in one cheap pass.

    Deliberately permissive. ``prepare_input`` re-derives everything from the
    rows this returns and applies the real gate, so the only failure that
    matters here is dropping a row it would have kept. Anything already in the
    candidate pool passes regardless of score, as does any manual override.

    The stored ``diligence_rank_score`` is not consulted, because it is not a
    column -- it lives inside ``record_json``, the blob this pass exists to
    avoid. What replaces it is the live rescore, which was already the
    load-bearing half of the rule: the frozen score goes stale as scoring
    improves, and the live one reflects current logic against the same copy.
    ``fit_score`` and ``priority_score`` are real columns and are taken as
    well, so a row clears on the highest of the three.
    """
    from classification_store import (
        count_eligible_for_diligence,
        iter_eligible_scoring_rows,
        read_store_meta,
        write_store_meta,
    )
    from filter import score_text

    keep: list[str] = list(pool_domains(pool_limit))
    if rescue_limit <= 0:
        return keep
    seen = set(keep)

    # The scan window rotates. It used to be the newest `rescue_limit` rows of
    # any bucket, which measured live meant 25,000 row-scores spent to look at
    # 5,996 eligible rows while 91,286 eligible rows sat permanently outside
    # the window -- a company whose stored score went stale could never be
    # rescued, which is the exact bug the live rescore exists to prevent.
    #
    # Ordering on the primary key and advancing an offset each run covers the
    # whole eligible set every few passes at the same cost per pass.
    eligible_total = count_eligible_for_diligence()
    try:
        cursor = int(read_store_meta(_RESCUE_CURSOR_KEY, "0") or 0)
    except (TypeError, ValueError):
        cursor = 0
    if cursor < 0 or (eligible_total and cursor >= eligible_total):
        cursor = 0
    for row in iter_eligible_scoring_rows(limit=rescue_limit, offset=cursor):
        domain = str(row.get("domain") or "").strip()
        if not domain or domain in seen:
            continue
        stored = 0.0
        for column in ("fit_score", "priority_score"):
            try:
                stored = max(stored, float(row.get(column) or 0))
            except (TypeError, ValueError):
                continue
        if stored >= minimum_score:
            keep.append(domain)
            continue
        copy = " ".join(
            str(row.get(column) or "")
            for column in ("company_name", "description", "excerpt")
        ).strip()
        if copy and score_text(copy)[0] >= minimum_score:
            keep.append(domain)
    # Advance even when the window found nothing: the point is coverage, and
    # a window that only moves on success would stick on a barren stretch.
    try:
        write_store_meta(_RESCUE_CURSOR_KEY, str(cursor + rescue_limit))
    except Exception as exc:
        # A cursor that cannot be saved costs coverage, not correctness --
        # the next run simply rescans the same window.
        print(f"Rescue cursor not saved: {exc}", file=sys.stderr)
    return keep


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output",
        type=Path,
        help="CSV path to write the candidate frame to.",
    )
    parser.add_argument(
        "--pool-limit",
        type=int,
        default=int(os.getenv("CLAUDE_POOL_ROWS", "2000")),
        help=(
            "How many top-scoring un-diligenced candidates to hand the "
            "classifier. It calls at most a hundred, so this only needs to be "
            "comfortably above that."
        ),
    )
    parser.add_argument(
        "--rescue-limit",
        type=int,
        default=int(os.getenv("CLAUDE_RESCUE_SCAN_ROWS", "25000")),
        help=(
            "How many recently-updated non-pool rows to rescore looking for "
            "companies a stale stored score has buried. 0 disables the scan."
        ),
    )
    parser.add_argument(
        "--minimum-score",
        type=int,
        default=int(os.getenv("CLAUDE_MINIMUM_SCORE", "60")),
        help="Eligibility floor; must match the classifier's own.",
    )
    args = parser.parse_args()

    from classification_store import frame_from_store

    # Two passes. The first streams a few columns to decide who is eligible;
    # the second loads full rows -- record_json and all -- for only those.
    # Loading everything to choose at most ten candidates is what got this
    # process OOM-killed once the corpus passed a quarter of a million rows.
    domains = eligible_domains(
        args.minimum_score, args.rescue_limit, args.pool_limit
    )
    if not domains:
        args.output.unlink(missing_ok=True)
        return 2

    frame = frame_from_store(domains=domains)
    if frame.empty:
        args.output.unlink(missing_ok=True)
        return 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)
    print(f"{len(frame)}", file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
