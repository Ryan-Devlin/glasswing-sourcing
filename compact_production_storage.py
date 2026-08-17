#!/usr/bin/env python3
"""Compact canonical records and remove reproducible cloud CSV mirrors."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from classification_store import compact_classification_storage
from common import FILTERED, LLM


REPRODUCIBLE_EXPORTS = (
    FILTERED / "all_screened_live.csv",
    FILTERED / "all_screened.csv",
    FILTERED / "screened_out_master.csv",
    FILTERED / "master_investable_claude.csv",
    FILTERED / "master_hard_denied_claude.csv",
    FILTERED / "crawled_investable_claude.csv",
    FILTERED / "crawled_hard_denied_claude.csv",
    FILTERED / "zone_crawled_investable_claude.csv",
    FILTERED / "zone_crawled_hard_denied_claude.csv",
    FILTERED / "certstream_investable_claude.csv",
    FILTERED / "certstream_hard_denied_claude.csv",
    LLM / "llm_approved_master.csv",
    LLM / "llm_denied_master.csv",
    LLM / "ranked_master.csv",
    LLM / "rejected_master.csv",
)


def removable_exports(*, execute: bool) -> dict[str, int]:
    files = [path for path in REPRODUCIBLE_EXPORTS if path.exists()]
    total = sum(path.stat().st_size for path in files)
    if execute:
        for path in files:
            path.unlink(missing_ok=True)
    return {"files": len(files), "bytes": total}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    before = removable_exports(execute=False)
    if not args.execute:
        print(json.dumps({"dry_run": True, "reproducible_exports": before}, indent=2))
        return
    result = compact_classification_storage()
    result["reproducible_exports"] = removable_exports(execute=True)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
