#!/usr/bin/env python3
"""Safe maintenance commands for screening and training data."""

from __future__ import annotations

import argparse
import json

from archive_gc import compact_html_archive
from rejection_cleanup import (
    AI_SEARCH_DB,
    ZONE_DB,
    automated_rejected_domains,
    purge_automated_rejections,
    rebuild_full_text_indexes,
)
from training_data import write_training_exports
from training_truth import rebuild_human_ground_truth


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "rebuild-ground-truth",
        help="Keep only human and verified portfolio training labels.",
    )
    purge = commands.add_parser(
        "purge-python-rejections",
        help="Remove derived payloads for non-manual hard denials.",
    )
    purge.add_argument("--vacuum", action="store_true")
    purge.add_argument(
        "--dry-run",
        action="store_true",
        help="Count protected automatic discards without deleting anything.",
    )
    commands.add_parser(
        "clean-all",
        help="Rebuild ground truth, purge automatic denials, and export training data.",
    )
    commands.add_parser(
        "compact-html",
        help="Physically remove orphaned blocks from both HTML archives.",
    )
    commands.add_parser(
        "repair-indexes",
        help="Rebuild both full-text indexes from live page rows.",
    )
    args = parser.parse_args()

    if args.command == "rebuild-ground-truth":
        result = rebuild_human_ground_truth()
        _, _, manifest = write_training_exports()
        result["training_export"] = manifest
    elif args.command == "purge-python-rejections":
        if args.dry_run:
            domains = automated_rejected_domains()
            result = {
                "dry_run": True,
                "purge_candidates": len(domains),
            }
        else:
            result = purge_automated_rejections(vacuum=args.vacuum)
    elif args.command == "compact-html":
        result = {
            "ai": compact_html_archive(
                AI_SEARCH_DB,
                AI_SEARCH_DB.parent / "html",
                shard_count=64,
            ),
            "zone": compact_html_archive(
                ZONE_DB,
                ZONE_DB.parent / "html",
                shard_count=128,
            ),
        }
    elif args.command == "repair-indexes":
        result = rebuild_full_text_indexes()
    else:
        result = {
            "ground_truth": rebuild_human_ground_truth(),
            "purge": purge_automated_rejections(vacuum=True),
        }
        result["html_archive"] = {
            "ai": compact_html_archive(
                AI_SEARCH_DB,
                AI_SEARCH_DB.parent / "html",
                shard_count=64,
            ),
            "zone": compact_html_archive(
                ZONE_DB,
                ZONE_DB.parent / "html",
                shard_count=128,
            ),
        }
        result["search_indexes"] = rebuild_full_text_indexes()
        _, _, manifest = write_training_exports()
        result["training_export"] = manifest
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
