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


def eligible_domains(minimum_score: int) -> list[str]:
    """Domains that could clear the diligence bar, found in one cheap pass.

    Applies the same eligibility rule ``prepare_input`` does -- the higher of
    the stored rank score and a live rescore of the stored page copy -- but
    streams the few columns it needs instead of the whole corpus. The live
    rescore is the load-bearing half: the frozen stored score goes stale as
    scoring improves, and filtering on it alone silently buries real companies.

    A manually overridden row is always eligible regardless of score, matching
    the forced path downstream.
    """
    from classification_store import iter_scoring_rows
    from filter import score_text

    keep: list[str] = []
    for row in iter_scoring_rows():
        domain = str(row.get("domain") or "").strip()
        if not domain:
            continue
        if str(row.get("manual_override") or "").strip():
            keep.append(domain)
            continue
        stored = 0.0
        for column in ("diligence_rank_score", "fit_score", "priority_score"):
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
    return keep


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output",
        type=Path,
        help="CSV path to write the candidate frame to.",
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
    domains = eligible_domains(args.minimum_score)
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
