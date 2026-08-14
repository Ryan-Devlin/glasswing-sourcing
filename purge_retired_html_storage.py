#!/usr/bin/env python3
"""Permanently remove retired crawler HTML/search payloads.

Domain tombstones, metadata, classifications, scores, tags, and human labels
are preserved. The command is deliberately inert without ``--execute``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
for source in (
    ROOT / "ai-domain-search-engine" / "src",
    ROOT / "zone-universe" / "src",
):
    sys.path.insert(0, str(source))

from ai_search.database import connect as connect_ai
from ai_search.config import load_settings as load_ai_settings
from db_backend import PostgresConnection
from zone_universe.database import connect as connect_zone
from zone_universe.config import load_settings as load_zone_settings


def _counts(conn) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT count(*) AS pages,
               coalesce(sum(length(cleaned_text)),0) AS cleaned_text_bytes,
               coalesce(sum(compressed_html_bytes),0) AS archive_bytes,
               (SELECT count(*) FROM html_blobs) AS html_blobs
        FROM pages
        """
    ).fetchone()
    return {key: int(row[key] or 0) for key in row.keys()}


def _purge(conn, *, zone: bool) -> dict[str, int]:
    before = _counts(conn)
    search_vector_reset = ",search_vector=NULL" if isinstance(
        conn, PostgresConnection
    ) else ""
    if zone:
        conn.execute(
            f"""
            UPDATE pages SET cleaned_text='',raw_html_zlib=NULL,
                raw_html_hash='',raw_html_bytes=0,html_archive_path='',
                html_archive_offset=0,html_archive_length=0,
                html_archive_codec='',compressed_html_bytes=0
                {search_vector_reset}
            """
        )
    else:
        conn.execute(
            f"""
            UPDATE pages SET visible_text='',cleaned_text='',raw_html='',
                raw_html_hash='',raw_html_bytes=0,html_archive_path='',
                html_archive_offset=0,html_archive_length=0,
                html_archive_codec='',compressed_html_bytes=0
                {search_vector_reset}
            """
        )
    conn.execute("DELETE FROM html_blobs")
    if not isinstance(conn, PostgresConnection):
        conn.execute("UPDATE page_search SET body=''")
    conn.commit()
    return {"before": before, "after": _counts(conn)}


def _delete_archives(root: Path) -> dict[str, int]:
    files = list(root.rglob("*.htmlpack")) if root.exists() else []
    byte_count = sum(path.stat().st_size for path in files)
    for path in files:
        path.unlink()
    return {"files_deleted": len(files), "bytes_deleted": byte_count}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    ai = connect_ai(
        "" if os.getenv("AI_SEARCH_DATABASE_URL")
        else load_ai_settings().database
    )
    zone = connect_zone(
        "" if os.getenv("ZONE_UNIVERSE_DATABASE_URL")
        else load_zone_settings().database
    )
    try:
        if not args.execute:
            result = {
                "dry_run": True,
                "ai": _counts(ai),
                "zone": _counts(zone),
            }
        else:
            result = {
                "dry_run": False,
                "ai": _purge(ai, zone=False),
                "zone": _purge(zone, zone=True),
                "ai_archive": _delete_archives(
                    ROOT / "ai-domain-search-engine" / "data" / "html"
                ),
                "zone_archive": _delete_archives(
                    ROOT / "zone-universe" / "data" / "html"
                ),
            }
    finally:
        ai.close()
        zone.close()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
