#!/usr/bin/env python3
"""Move live legacy HTML-pack blocks to configured S3-compatible storage.

The crawler databases can contain absolute archive pointers created on an
older machine. This migration reads only blocks still referenced by live
page rows, writes deterministic objects, verifies every upload, and then
updates the database pointer. It is idempotent and safe to resume.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zlib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
for source_root in (
    ROOT / "ai-domain-search-engine" / "src",
    ROOT / "zone-universe" / "src",
):
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))

from ai_search.config import load_settings as load_ai_settings
from ai_search.database import connect as connect_ai_search
from object_storage import (
    exists,
    read_bytes,
    using_object_storage,
    write_bytes,
)
from zone_universe.config import load_settings as load_zone_settings
from zone_universe.database import connect as connect_zone_universe


OBJECT_PREFIX = "html-archive/migrated"


def is_object_key(value: object) -> bool:
    return str(value or "").startswith("html-archive/")


def resolve_legacy_path(pointer: object, archive_root: Path) -> Path:
    """Map a historical absolute pointer onto its current mounted root."""
    value = str(pointer or "")
    path = Path(value)
    if path.exists():
        return path
    marker = "/data/html/"
    if marker in value:
        relative = value.split(marker, 1)[1]
        candidate = archive_root / relative
        if candidate.exists():
            return candidate
    candidate = archive_root / path.name
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Legacy HTML pack is unavailable: {value}")


def read_legacy_block(
    pointer: object,
    *,
    archive_root: Path,
    offset: int,
    length: int,
    codec: str,
    raw_hash: str,
) -> bytes:
    source_path = resolve_legacy_path(pointer, archive_root)
    with source_path.open("rb") as source:
        source.seek(max(0, int(offset)))
        payload = source.read(max(0, int(length)))
    if len(payload) != int(length):
        raise RuntimeError(
            f"Truncated legacy block: {source_path}:{offset}+{length}"
        )
    if codec == "zlib" and raw_hash:
        actual = hashlib.sha256(zlib.decompress(payload)).hexdigest()
        if actual != raw_hash:
            raise RuntimeError(
                f"Legacy block checksum mismatch: {source_path}:{raw_hash}"
            )
    return payload


def _rows(conn: Any, subsystem: str) -> list[Any]:
    fields = "id,raw_html_hash,html_archive_path,html_archive_offset,html_archive_length,html_archive_codec,raw_html_bytes"
    if subsystem == "zone":
        fields = "id,tld," + fields.split("id,", 1)[1]
    return conn.execute(
        f"""
        SELECT {fields} FROM pages
        WHERE html_archive_path<>''
          AND left(html_archive_path,13)<>'html-archive/'
        ORDER BY id
        """
    ).fetchall()


def migrate_subsystem(
    subsystem: str,
    *,
    dry_run: bool = False,
    limit: int = 0,
    verify_uploads: bool = True,
) -> dict[str, int]:
    if subsystem == "ai":
        conn = connect_ai_search("")
        archive_root = load_ai_settings().html_archive
    elif subsystem == "zone":
        conn = connect_zone_universe("")
        archive_root = load_zone_settings().html_archive
    else:
        raise ValueError(f"Unknown subsystem: {subsystem}")

    result = {
        "candidates": 0,
        "migrated_pages": 0,
        "uploaded_objects": 0,
        "reused_objects": 0,
        "bytes": 0,
    }
    try:
        rows = _rows(conn, subsystem)
        if limit > 0:
            rows = rows[:limit]
        result["candidates"] = len(rows)
        checked_keys: set[str] = set()
        for position, row in enumerate(rows, start=1):
            raw_hash = str(row["raw_html_hash"] or "")
            if not raw_hash:
                raise RuntimeError(f"Page {row['id']} has no raw HTML hash")
            payload = read_legacy_block(
                row["html_archive_path"],
                archive_root=archive_root,
                offset=int(row["html_archive_offset"] or 0),
                length=int(row["html_archive_length"] or 0),
                codec=str(row["html_archive_codec"] or ""),
                raw_hash=raw_hash,
            )
            tld = str(row["tld"] or "unknown") if subsystem == "zone" else "ai"
            key = f"{OBJECT_PREFIX}/{subsystem}/{tld}/{raw_hash[:2]}/{raw_hash}.zlib"
            result["bytes"] += len(payload)
            if dry_run:
                continue
            if key not in checked_keys:
                if exists(key):
                    result["reused_objects"] += 1
                else:
                    write_bytes(key, payload)
                    result["uploaded_objects"] += 1
                if verify_uploads and read_bytes(key) != payload:
                    raise RuntimeError(f"Object verification failed: {key}")
                checked_keys.add(key)
            conn.execute(
                """
                UPDATE pages SET html_archive_path=?,html_archive_offset=0,
                    html_archive_length=?,compressed_html_bytes=?
                WHERE id=?
                """,
                (key, len(payload), len(payload), int(row["id"])),
            )
            conn.execute(
                """
                UPDATE html_blobs SET archive_path=?,archive_offset=0,
                    archive_length=?
                WHERE raw_html_hash=?
                """,
                (key, len(payload), raw_hash),
            )
            result["migrated_pages"] += 1
            if position % 100 == 0:
                conn.commit()
        if not dry_run:
            conn.commit()
    except Exception:
        if not dry_run:
            conn.rollback()
        raise
    finally:
        conn.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--subsystem", choices=("all", "ai", "zone"), default="all"
    )
    parser.add_argument("--no-verify", action="store_true")
    args = parser.parse_args()
    if not using_object_storage():
        raise SystemExit("S3_BUCKET is not configured; refusing migration.")
    subsystems = (
        ("ai", "zone") if args.subsystem == "all" else (args.subsystem,)
    )
    result = {
        subsystem: migrate_subsystem(
            subsystem,
            dry_run=args.dry_run,
            limit=max(0, args.limit),
            verify_uploads=not args.no_verify,
        )
        for subsystem in subsystems
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
