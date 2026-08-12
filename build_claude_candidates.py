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
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output",
        type=Path,
        help="CSV path to write the candidate frame to.",
    )
    args = parser.parse_args()

    from classification_store import frame_from_store

    frame = frame_from_store()
    if frame.empty:
        # Communicate "nothing to do" without writing a misleading empty file
        # that a later run could mistake for real candidates.
        args.output.unlink(missing_ok=True)
        return 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)
    print(f"{len(frame)}", file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
