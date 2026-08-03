"""Purge payloads produced for automatically rejected domains.

The immutable discovery inventories and one minimal queue deduplication row are
retained so a rejected domain is not downloaded repeatedly. Search documents,
HTML/page metadata, classifications, exports, registration lookups, and retry
records are removed.
"""

from __future__ import annotations

import csv
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable

from ai_search.database import AI_SEARCH_DATABASE_ENV_VAR
from ai_search.database import connect as connect_ai_search
from ai_search.database import index_page as index_ai_search_page
from classification_store import CLASSIFICATION_DB, connect_store
from common import (
    AI_SEARCH_DB,
    DATA,
    QUEUE_DB,
    ZONE_UNIVERSE_DB,
    stable_domain,
)
from db_backend import begin_immediate, using_postgres
from training_truth import protected_training_domains
from zone_universe.database import ZONE_UNIVERSE_DATABASE_ENV_VAR
from zone_universe.database import connect as connect_zone_universe


ZONE_DB = ZONE_UNIVERSE_DB


def automated_rejected_domains(
    path: Path | str = CLASSIFICATION_DB,
) -> set[str]:
    database = Path(path)
    if not using_postgres() and not database.exists():
        return set()
    protected = protected_training_domains()
    with connect_store(database, read_only=True) as conn:
        domains = {
            stable_domain(row["domain"])
            for row in conn.execute(
                """
                SELECT domain FROM classifications
                WHERE routing_bucket='reject'
                """
            )
        }
    return {domain for domain in domains if domain and domain not in protected}


def _rebuild_ai_search_index_postgres() -> int:
    conn = connect_ai_search("")
    try:
        page_ids = [
            row["id"] for row in conn.execute("SELECT id FROM pages").fetchall()
        ]
        for page_id in page_ids:
            index_ai_search_page(conn, page_id)
        conn.commit()
        return len(page_ids)
    finally:
        conn.close()


def _rebuild_zone_universe_index_postgres() -> int:
    conn = connect_zone_universe("")
    try:
        rows = conn.execute(
            "SELECT id, domain, title, meta_description, headings, cleaned_text "
            "FROM pages"
        ).fetchall()
        for row in rows:
            conn.execute("""
                UPDATE pages SET search_vector =
                    setweight(to_tsvector('english', coalesce(?, '')), 'A') ||
                    setweight(to_tsvector('english', coalesce(?, '')), 'B') ||
                    setweight(to_tsvector('english', coalesce(?, '')), 'C') ||
                    setweight(to_tsvector('english', coalesce(?, '')), 'D') ||
                    setweight(to_tsvector('english', coalesce(?, '')), 'D')
                WHERE id = ?
            """, (
                row["domain"], row["title"], row["meta_description"],
                row["headings"], row["cleaned_text"], row["id"],
            ))
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def rebuild_full_text_indexes(
    *,
    ai_search_db: Path | str = AI_SEARCH_DB,
    zone_db: Path | str = ZONE_DB,
) -> dict[str, int]:
    """Rebuild both full-text indexes from their canonical live page tables."""
    result = {"ai_indexed_pages": 0, "zone_indexed_pages": 0}

    if using_postgres(AI_SEARCH_DATABASE_ENV_VAR):
        result["ai_indexed_pages"] = _rebuild_ai_search_index_postgres()
    else:
        ai_path = Path(ai_search_db)
        if ai_path.exists():
            with sqlite3.connect(ai_path, timeout=30) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM page_search")
                conn.execute(
                    """
                    INSERT INTO page_search(
                        page_id,domain,title,description,headings,body
                    )
                    SELECT p.id,d.domain,p.title,p.meta_description,
                           p.headings,p.cleaned_text
                    FROM pages p
                    JOIN domains d ON d.id=p.domain_id
                    """
                )
                conn.execute(
                    "INSERT INTO page_search(page_search) VALUES ('optimize')"
                )
                conn.commit()
                result["ai_indexed_pages"] = int(
                    conn.execute("SELECT count(*) FROM page_search").fetchone()[0]
                )

    if using_postgres(ZONE_UNIVERSE_DATABASE_ENV_VAR):
        result["zone_indexed_pages"] = _rebuild_zone_universe_index_postgres()
    else:
        zone_path = Path(zone_db)
        if zone_path.exists():
            with sqlite3.connect(zone_path, timeout=30) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM page_search")
                conn.execute(
                    """
                    INSERT INTO page_search(
                        page_id,domain,title,description,headings,body
                    )
                    SELECT id,domain,title,meta_description,headings,cleaned_text
                    FROM pages
                    """
                )
                conn.execute(
                    "INSERT INTO page_search(page_search) VALUES ('optimize')"
                )
                conn.commit()
                result["zone_indexed_pages"] = int(
                    conn.execute("SELECT count(*) FROM page_search").fetchone()[0]
                )
    return result


def _chunks(values: list, size: int = 700):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _delete_where(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    domains: list[str],
) -> int:
    removed = 0
    for batch in _chunks(domains):
        marks = ",".join("?" for _ in batch)
        cursor = conn.execute(
            f"DELETE FROM {table} WHERE {column} IN ({marks})",
            batch,
        )
        removed += max(0, int(cursor.rowcount))
    return removed


def _filter_csv(path: Path, domains: set[str]) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    temporary_name = ""
    removed = 0
    try:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames:
                return 0
            domain_column = next(
                (
                    column
                    for column in ("domain", "domain_url", "registrable")
                    if column in reader.fieldnames
                ),
                "",
            )
            if not domain_column:
                return 0
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as target:
                temporary_name = target.name
                writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
                writer.writeheader()
                for row in reader:
                    if stable_domain(row.get(domain_column, "")) in domains:
                        removed += 1
                        continue
                    writer.writerow(row)
                target.flush()
                os.fsync(target.fileno())
        if removed:
            os.replace(temporary_name, path)
            temporary_name = ""
        return removed
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def purge_automated_rejections(
    domains: Iterable[str] | None = None,
    *,
    classification_db: Path | str = CLASSIFICATION_DB,
    queue_db: Path | str = QUEUE_DB,
    ai_search_db: Path | str = AI_SEARCH_DB,
    zone_db: Path | str = ZONE_DB,
    data_root: Path = DATA,
    vacuum: bool = False,
    clean_csv: bool = True,
) -> dict[str, int]:
    """Remove every derived payload while protecting explicit human labels."""
    requested = (
        automated_rejected_domains(classification_db)
        if domains is None
        else {
            stable_domain(domain)
            for domain in domains
            if stable_domain(domain)
        }
    )
    protected = protected_training_domains()
    selected = sorted(requested - protected)
    result = {
        "requested": len(requested),
        "protected": len(requested & protected),
        "purged_domains": len(selected),
        "classification_rows": 0,
        "movement_rows": 0,
        "queue_payloads_scrubbed": 0,
        "ai_domain_rows": 0,
        "ai_page_rows": 0,
        "zone_page_rows": 0,
        "zone_auxiliary_rows": 0,
        "csv_rows": 0,
    }
    if not selected:
        return result

    classification_path = Path(classification_db)
    if using_postgres() or classification_path.exists():
        with connect_store(classification_path) as conn:
            begin_immediate(conn)
            result["classification_rows"] += _delete_where(
                conn, "classifications", "domain", selected
            )
            result["movement_rows"] += _delete_where(
                conn, "movement_log", "domain", selected
            )
            # No selected domain can have a protected manual decision, but
            # remove historical automated misuse of this table.
            _delete_where(conn, "manual_decisions", "domain", selected)
            conn.commit()
            # Postgres autovacuums on its own and can't run VACUUM inside a
            # transaction block; this manual pass is SQLite-only maintenance.
            if vacuum and not using_postgres():
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.execute("VACUUM")

    queue_path = Path(queue_db)
    if queue_path.exists():
        with sqlite3.connect(queue_path, timeout=30) as conn:
            for batch in _chunks(selected):
                marks = ",".join("?" for _ in batch)
                cursor = conn.execute(
                    """
                    UPDATE domains SET
                        status='discarded',
                        classification='',
                        last_error='',
                        next_attempt_at=NULL
                    WHERE domain IN (""" + marks + ")",
                    batch,
                )
                result["queue_payloads_scrubbed"] += max(
                    0, int(cursor.rowcount)
                )
            conn.commit()

    ai_path = Path(ai_search_db)
    if ai_path.exists():
        with sqlite3.connect(ai_path, timeout=30) as conn:
            conn.execute("PRAGMA foreign_keys=ON")
            for batch in _chunks(selected):
                marks = ",".join("?" for _ in batch)
                page_ids = [
                    int(row[0])
                    for row in conn.execute(
                        "SELECT id FROM pages WHERE domain_id IN "
                        f"(SELECT id FROM domains WHERE domain IN ({marks}))",
                        batch,
                    )
                ]
                result["ai_page_rows"] += len(page_ids)
                for id_batch in _chunks(page_ids):
                    id_marks = ",".join("?" for _ in id_batch)
                    conn.execute(
                        f"DELETE FROM page_search "
                        f"WHERE page_id IN ({id_marks})",
                        id_batch,
                    )
                cursor = conn.execute(
                    f"DELETE FROM domains WHERE domain IN ({marks})",
                    batch,
                )
                result["ai_domain_rows"] += max(0, int(cursor.rowcount))
            conn.commit()
            if vacuum:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.execute("VACUUM")

    zone_path = Path(zone_db)
    if zone_path.exists():
        with sqlite3.connect(zone_path, timeout=30) as conn:
            for batch in _chunks(selected):
                marks = ",".join("?" for _ in batch)
                page_ids = [
                    int(row[0])
                    for row in conn.execute(
                        f"SELECT id FROM pages WHERE domain IN ({marks})",
                        batch,
                    )
                ]
                for id_batch in _chunks(page_ids):
                    id_marks = ",".join("?" for _ in id_batch)
                    conn.execute(
                        f"DELETE FROM page_search WHERE page_id IN ({id_marks})",
                        id_batch,
                    )
                result["zone_page_rows"] += _delete_where(
                    conn, "pages", "domain", batch
                )
                for table in (
                    "crawl_retries",
                    "registration_queue",
                    "domain_registration",
                ):
                    result["zone_auxiliary_rows"] += _delete_where(
                        conn, table, "domain", batch
                    )
            conn.commit()
            if vacuum:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.execute("VACUUM")

    if clean_csv:
        for folder_name in ("filtered", "llm", "stream_batches"):
            folder = data_root / folder_name
            if not folder.exists():
                continue
            for path in folder.glob("*.csv"):
                result["csv_rows"] += _filter_csv(path, set(selected))

    return result
