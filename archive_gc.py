"""Rewrite HTML packs using only blocks referenced by live page rows."""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def compact_html_archive(
    database: Path | str,
    archive_root: Path | str,
    *,
    shard_count: int = 64,
) -> dict[str, int]:
    """Atomically replace append-only packs and discard orphaned blocks."""
    database = Path(database)
    archive_root = Path(archive_root)
    if not database.exists() or not archive_root.exists():
        return {
            "live_blobs": 0,
            "bytes_before": 0,
            "bytes_after": 0,
            "bytes_reclaimed": 0,
        }
    shard_count = max(1, int(shard_count))
    bytes_before = sum(
        path.stat().st_size
        for path in archive_root.rglob("*")
        if path.is_file()
    )
    temporary_root = Path(tempfile.mkdtemp(
        prefix=f".{archive_root.name}.rebuild-",
        dir=archive_root.parent,
    ))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    old_root = archive_root.with_name(f".{archive_root.name}.old-{stamp}")
    pointers: dict[str, tuple[str, int, int, str, int]] = {}

    conn = sqlite3.connect(database, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT raw_html_hash,html_archive_path,html_archive_offset,
                   html_archive_length,html_archive_codec,
                   max(raw_html_bytes) AS raw_html_bytes
            FROM pages
            WHERE html_archive_path<>'' AND html_archive_length>0
              AND raw_html_hash<>''
            GROUP BY raw_html_hash
            ORDER BY raw_html_hash
            """
        ).fetchall()
        handles: dict[Path, object] = {}
        try:
            for row in rows:
                raw_hash = str(row["raw_html_hash"])
                source_path = Path(str(row["html_archive_path"]))
                with source_path.open("rb") as source:
                    source.seek(int(row["html_archive_offset"]))
                    block = source.read(int(row["html_archive_length"]))
                if len(block) != int(row["html_archive_length"]):
                    raise RuntimeError(
                        f"archive_block_truncated:{source_path}:{raw_hash}"
                    )
                shard = int(raw_hash[:8], 16) % shard_count
                destination = temporary_root / f"{shard:03d}.htmlpack"
                handle = handles.get(destination)
                if handle is None:
                    handle = destination.open("ab")
                    handles[destination] = handle
                offset = handle.tell()
                handle.write(block)
                pointers[raw_hash] = (
                    str(archive_root / destination.name),
                    offset,
                    len(block),
                    str(row["html_archive_codec"]),
                    int(row["raw_html_bytes"] or 0),
                )
            for handle in handles.values():
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            for handle in handles.values():
                handle.close()

        os.replace(archive_root, old_root)
        os.replace(temporary_root, archive_root)
        try:
            conn.execute("BEGIN IMMEDIATE")
            for raw_hash, pointer in pointers.items():
                path, offset, length, codec, raw_bytes = pointer
                conn.execute(
                    """
                    UPDATE pages SET
                        html_archive_path=?,
                        html_archive_offset=?,
                        html_archive_length=?,
                        html_archive_codec=?,
                        compressed_html_bytes=?
                    WHERE raw_html_hash=?
                    """,
                    (path, offset, length, codec, length, raw_hash),
                )
            conn.execute("DELETE FROM html_blobs")
            now = datetime.now(timezone.utc).isoformat()
            conn.executemany(
                """
                INSERT INTO html_blobs(
                    raw_html_hash,archive_path,archive_offset,
                    archive_length,archive_codec,raw_bytes,created_at
                ) VALUES (?,?,?,?,?,?,?)
                """,
                [
                    (raw_hash, *pointer, now)
                    for raw_hash, pointer in pointers.items()
                ],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            failed_root = archive_root.with_name(
                f".{archive_root.name}.failed-{stamp}"
            )
            os.replace(archive_root, failed_root)
            os.replace(old_root, archive_root)
            shutil.rmtree(failed_root, ignore_errors=True)
            raise
        shutil.rmtree(old_root)
    except Exception:
        shutil.rmtree(temporary_root, ignore_errors=True)
        raise
    finally:
        conn.close()

    bytes_after = sum(
        path.stat().st_size
        for path in archive_root.rglob("*")
        if path.is_file()
    )
    return {
        "live_blobs": len(pointers),
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "bytes_reclaimed": max(0, bytes_before - bytes_after),
    }
