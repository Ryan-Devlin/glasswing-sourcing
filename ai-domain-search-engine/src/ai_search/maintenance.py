from __future__ import annotations

import gzip
import hashlib
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .database import utc_now
from .html_archive import ArchivePointer, append_html, read_html


def compressed_backup(
    conn: sqlite3.Connection,
    database: Path,
) -> Path:
    """Create a consistent compressed SQLite backup before compaction."""
    backup_root = database.parent / "backups"
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    temporary = backup_root / f"{database.stem}-before-compact-{stamp}.sqlite3"
    destination = temporary.with_suffix(temporary.suffix + ".gz")
    backup_conn = sqlite3.connect(temporary)
    try:
        conn.backup(backup_conn)
    finally:
        backup_conn.close()
    with temporary.open("rb") as source, gzip.open(
        destination,
        "wb",
        compresslevel=6,
    ) as target:
        shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
    temporary.unlink(missing_ok=True)
    return destination


def _existing_pointer(
    conn: sqlite3.Connection,
    raw_hash: str,
) -> ArchivePointer | None:
    row = conn.execute(
        """
        SELECT archive_path,archive_offset,archive_length,
               archive_codec,raw_bytes
        FROM html_blobs
        WHERE raw_html_hash=?
        """,
        (raw_hash,),
    ).fetchone()
    if row is None:
        return None
    return ArchivePointer(
        path=str(row["archive_path"]),
        offset=int(row["archive_offset"]),
        length=int(row["archive_length"]),
        codec=str(row["archive_codec"]),
        raw_bytes=int(row["raw_bytes"]),
    )


def compact_existing_pages(
    conn: sqlite3.Connection,
    *,
    database: Path,
    archive_root: Path,
    make_backup: bool = True,
    vacuum: bool = True,
    batch_size: int = 100,
    progress: Callable[[str], None] | None = None,
) -> dict[str, int | str]:
    """Move raw HTML out of SQLite while preserving search and evidence text."""
    report = progress or (lambda _message: None)
    backup_path = ""
    if make_backup:
        report("Creating compressed safety backup...")
        backup_path = str(compressed_backup(conn, database))
        report(f"Backup complete: {backup_path}")

    total = int(
        conn.execute(
            "SELECT count(*) AS count FROM pages WHERE length(raw_html)>0"
        ).fetchone()["count"]
    )
    archived = discarded_templates = deduplicated = processed = 0
    raw_bytes = compressed_bytes = 0

    while True:
        rows = conn.execute(
            """
            SELECT p.id,p.raw_html,d.domain,d.is_parked,d.is_coming_soon
            FROM pages p
            JOIN domains d ON d.id=p.domain_id
            WHERE length(p.raw_html)>0
            ORDER BY p.id
            LIMIT ?
            """,
            (max(1, int(batch_size)),),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            html = str(row["raw_html"] or "")
            raw = html.encode("utf-8", errors="replace")
            raw_hash = hashlib.sha256(raw).hexdigest()
            pointer: ArchivePointer | None = None
            retain = not bool(row["is_parked"] or row["is_coming_soon"])
            if retain:
                pointer = _existing_pointer(conn, raw_hash)
                if pointer is not None:
                    deduplicated += 1
                else:
                    pointer = append_html(
                        archive_root,
                        domain=str(row["domain"]),
                        html=html,
                    )
                    # Verify every newly written block before deleting its
                    # database copy. Interrupted runs are safe to resume.
                    restored = read_html(
                        pointer.path,
                        offset=pointer.offset,
                        length=pointer.length,
                        codec=pointer.codec,
                    )
                    if hashlib.sha256(
                        restored.encode("utf-8", errors="replace")
                    ).hexdigest() != raw_hash:
                        raise RuntimeError(
                            f"archive_verification_failed:{row['domain']}"
                        )
                    conn.execute(
                        """
                        INSERT INTO html_blobs(
                            raw_html_hash,archive_path,archive_offset,
                            archive_length,archive_codec,raw_bytes,created_at
                        ) VALUES (?,?,?,?,?,?,?)
                        """,
                        (
                            raw_hash,
                            pointer.path,
                            pointer.offset,
                            pointer.length,
                            pointer.codec,
                            pointer.raw_bytes,
                            utc_now(),
                        ),
                    )
                archived += 1
                compressed_bytes += pointer.length
            else:
                discarded_templates += 1
            raw_bytes += len(raw)
            conn.execute(
                """
                UPDATE pages SET
                    visible_text='',
                    raw_html='',
                    raw_html_hash=?,
                    html_archive_path=?,
                    html_archive_offset=?,
                    html_archive_length=?,
                    html_archive_codec=?,
                    raw_html_bytes=?,
                    compressed_html_bytes=?,
                    fetch_method='compact_archive'
                WHERE id=?
                """,
                (
                    raw_hash,
                    pointer.path if pointer else "",
                    pointer.offset if pointer else 0,
                    pointer.length if pointer else 0,
                    pointer.codec if pointer else "",
                    len(raw),
                    pointer.length if pointer else 0,
                    int(row["id"]),
                ),
            )
            processed += 1
        conn.commit()
        report(f"Compacted {processed:,}/{total:,} pages")

    if vacuum and processed:
        report("Rebuilding SQLite to release empty pages...")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
        conn.execute("PRAGMA optimize")

    return {
        "pages_processed": processed,
        "pages_archived": archived,
        "templates_discarded": discarded_templates,
        "duplicates_reused": deduplicated,
        "raw_bytes_removed": raw_bytes,
        "archive_bytes_written_or_reused": compressed_bytes,
        "backup_path": backup_path,
    }
