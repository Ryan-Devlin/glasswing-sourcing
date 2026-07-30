from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .config import DATABASE


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def connect(
    path: Path | str = DATABASE,
    *,
    read_only: bool = False,
) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if read_only and path.exists():
        conn = sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            timeout=30,
        )
    else:
        conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    if read_only:
        conn.execute("PRAGMA query_only=ON")
        return conn
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS zones (
            tld TEXT PRIMARY KEY,
            zone_path TEXT NOT NULL DEFAULT '',
            source_size INTEGER NOT NULL DEFAULT 0,
            source_modified_at TEXT NOT NULL DEFAULT '',
            source_fingerprint TEXT NOT NULL DEFAULT '',
            import_status TEXT NOT NULL DEFAULT 'not_imported',
            imported_at TEXT,
            discovered_domains INTEGER NOT NULL DEFAULT 0,
            shard_count INTEGER NOT NULL DEFAULT 0,
            active_queue_version TEXT NOT NULL DEFAULT '',
            import_error TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS shards (
            tld TEXT NOT NULL,
            queue_version TEXT NOT NULL,
            shard INTEGER NOT NULL,
            priority INTEGER NOT NULL DEFAULT 1,
            path TEXT NOT NULL,
            byte_offset INTEGER NOT NULL DEFAULT 0,
            total_bytes INTEGER NOT NULL DEFAULT 0,
            queued_domains INTEGER NOT NULL DEFAULT 0,
            attempted INTEGER NOT NULL DEFAULT 0,
            success INTEGER NOT NULL DEFAULT 0,
            dns_error INTEGER NOT NULL DEFAULT 0,
            timeout INTEGER NOT NULL DEFAULT 0,
            blocked INTEGER NOT NULL DEFAULT 0,
            http_error INTEGER NOT NULL DEFAULT 0,
            empty INTEGER NOT NULL DEFAULT 0,
            other_error INTEGER NOT NULL DEFAULT 0,
            skipped_existing INTEGER NOT NULL DEFAULT 0,
            skipped_low_priority INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(tld, queue_version, shard)
        );
        CREATE INDEX IF NOT EXISTS idx_shards_pending
            ON shards(
                tld, queue_version, priority, byte_offset, total_bytes
            );

        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY,
            domain TEXT NOT NULL UNIQUE,
            tld TEXT NOT NULL,
            requested_url TEXT NOT NULL DEFAULT '',
            final_url TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            meta_description TEXT NOT NULL DEFAULT '',
            headings TEXT NOT NULL DEFAULT '',
            cleaned_text TEXT NOT NULL DEFAULT '',
            raw_html_zlib BLOB,
            raw_html_hash TEXT NOT NULL DEFAULT '',
            html_archive_path TEXT NOT NULL DEFAULT '',
            html_archive_offset INTEGER NOT NULL DEFAULT 0,
            html_archive_length INTEGER NOT NULL DEFAULT 0,
            html_archive_codec TEXT NOT NULL DEFAULT '',
            document_count INTEGER NOT NULL DEFAULT 1,
            raw_html_bytes INTEGER NOT NULL DEFAULT 0,
            compressed_html_bytes INTEGER NOT NULL DEFAULT 0,
            http_status INTEGER,
            word_count INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT NOT NULL DEFAULT '',
            language TEXT NOT NULL DEFAULT '',
            is_parked INTEGER NOT NULL DEFAULT 0,
            is_coming_soon INTEGER NOT NULL DEFAULT 0,
            crawl_duration_ms INTEGER NOT NULL DEFAULT 0,
            enrichment_reason TEXT NOT NULL DEFAULT '',
            crawled_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pages_tld_crawled
            ON pages(tld, crawled_at DESC);
        CREATE INDEX IF NOT EXISTS idx_pages_word_count
            ON pages(word_count DESC);
        CREATE VIRTUAL TABLE IF NOT EXISTS page_search USING fts5(
            page_id UNINDEXED,
            domain,
            title,
            description,
            headings,
            body,
            tokenize='unicode61 remove_diacritics 2'
        );

        CREATE TABLE IF NOT EXISTS worker_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            tld TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_event_log_created
            ON event_log(created_at DESC);

        CREATE TABLE IF NOT EXISTS crawl_retries (
            domain TEXT PRIMARY KEY,
            tld TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT NOT NULL,
            last_status TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_crawl_retries_due
            ON crawl_retries(tld,next_attempt_at,attempts);

        CREATE TABLE IF NOT EXISTS domain_registration (
            domain TEXT PRIMARY KEY,
            tld TEXT NOT NULL,
            registration_at TEXT NOT NULL DEFAULT '',
            expiration_at TEXT NOT NULL DEFAULT '',
            changed_at TEXT NOT NULL DEFAULT '',
            registrar TEXT NOT NULL DEFAULT '',
            rdap_status TEXT NOT NULL DEFAULT '',
            lookup_status TEXT NOT NULL DEFAULT '',
            lookup_error TEXT NOT NULL DEFAULT '',
            rdap_url TEXT NOT NULL DEFAULT '',
            checked_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_domain_registration_registered
            ON domain_registration(registration_at);

        CREATE TABLE IF NOT EXISTS registration_queue (
            domain TEXT PRIMARY KEY,
            tld TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            queued_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_registration_queue_next
            ON registration_queue(status,next_attempt_at,priority DESC,queued_at);

        CREATE TABLE IF NOT EXISTS html_blobs (
            raw_html_hash TEXT PRIMARY KEY,
            archive_path TEXT NOT NULL,
            archive_offset INTEGER NOT NULL DEFAULT 0,
            archive_length INTEGER NOT NULL DEFAULT 0,
            archive_codec TEXT NOT NULL DEFAULT '',
            raw_bytes INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        """
    )
    zone_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(zones)")
    }
    if "rejected_gibberish" not in zone_columns:
        conn.execute(
            "ALTER TABLE zones ADD COLUMN "
            "rejected_gibberish INTEGER NOT NULL DEFAULT 0"
        )
    page_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(pages)")
    }
    page_additions = {
        "raw_html_hash": "TEXT NOT NULL DEFAULT ''",
        "html_archive_path": "TEXT NOT NULL DEFAULT ''",
        "html_archive_offset": "INTEGER NOT NULL DEFAULT 0",
        "html_archive_length": "INTEGER NOT NULL DEFAULT 0",
        "html_archive_codec": "TEXT NOT NULL DEFAULT ''",
        "document_count": "INTEGER NOT NULL DEFAULT 1",
        "enrichment_reason": "TEXT NOT NULL DEFAULT ''",
    }
    for column, definition in page_additions.items():
        if column not in page_columns:
            conn.execute(
                f"ALTER TABLE pages ADD COLUMN {column} {definition}"
            )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pages_raw_html_hash "
        "ON pages(raw_html_hash)"
    )
    shard_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(shards)")
    }
    if "skipped_existing" not in shard_columns:
        conn.execute(
            "ALTER TABLE shards ADD COLUMN "
            "skipped_existing INTEGER NOT NULL DEFAULT 0"
        )
    if "priority" not in shard_columns:
        conn.execute(
            "ALTER TABLE shards ADD COLUMN "
            "priority INTEGER NOT NULL DEFAULT 1"
        )
    if "skipped_low_priority" not in shard_columns:
        conn.execute(
            "ALTER TABLE shards ADD COLUMN "
            "skipped_low_priority INTEGER NOT NULL DEFAULT 0"
        )
    zone_additions = {
        "snapshot_path": "TEXT NOT NULL DEFAULT ''",
        "added_domains": "INTEGER NOT NULL DEFAULT 0",
        "removed_domains": "INTEGER NOT NULL DEFAULT 0",
        "unchanged_domains": "INTEGER NOT NULL DEFAULT 0",
        "delta_added_path": "TEXT NOT NULL DEFAULT ''",
        "delta_removed_path": "TEXT NOT NULL DEFAULT ''",
    }
    for column, definition in zone_additions.items():
        if column not in zone_columns:
            conn.execute(
                f"ALTER TABLE zones ADD COLUMN {column} {definition}"
            )
    conn.execute(
        """
        CREATE VIEW IF NOT EXISTS domains AS
        SELECT domain,crawled_at AS last_crawled_at
        FROM pages
        """
    )
    conn.commit()


def set_state(conn: sqlite3.Connection, key: str, value: object) -> None:
    conn.execute(
        """
        INSERT INTO worker_state(key,value,updated_at) VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,
        (key, str(value), utc_now()),
    )
    conn.commit()


def get_state(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row["key"]): str(row["value"])
        for row in conn.execute(
            "SELECT key,value FROM worker_state"
        )
    }


def log_event(
    conn: sqlite3.Connection,
    event_type: str,
    message: str,
    *,
    tld: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO event_log(event_type,tld,message,created_at)
        VALUES (?,?,?,?)
        """,
        (event_type, tld, str(message)[:1000], utc_now()),
    )
    conn.commit()


def diagnostics(conn: sqlite3.Connection) -> dict[str, int]:
    values = {
        "zones": "SELECT count(*) FROM zones",
        "queued_domains": (
            "SELECT coalesce(sum(queued_domains),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "attempted": (
            "SELECT coalesce(sum(attempted),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "low_priority_quarantined": (
            "SELECT coalesce(sum(skipped_low_priority),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "success": "SELECT count(*) FROM pages",
        "searchable_pages": "SELECT count(*) FROM page_search",
        "documents": (
            "SELECT coalesce(sum(document_count),0) FROM pages"
        ),
        "homepage_only": (
            "SELECT count(*) FROM pages WHERE document_count=1"
        ),
        "evidence_enriched": (
            "SELECT count(*) FROM pages WHERE document_count>1"
        ),
        "raw_html_bytes": (
            "SELECT coalesce(sum(raw_html_bytes),0) FROM pages"
        ),
        "compressed_html_bytes": (
            "SELECT "
            "coalesce((SELECT sum(length(raw_html_zlib)) FROM pages),0) + "
            "coalesce((SELECT sum(archive_length) FROM ("
            "SELECT DISTINCT html_archive_path,html_archive_offset,"
            "html_archive_length AS archive_length "
            "FROM pages WHERE html_archive_path<>''"
            ")),0)"
        ),
        "gibberish_rejected": (
            "SELECT coalesce(sum(rejected_gibberish),0) FROM zones"
        ),
        "retries_pending": "SELECT count(*) FROM crawl_retries",
        "registration_dates": (
            "SELECT count(*) FROM domain_registration "
            "WHERE registration_at<>''"
        ),
        "registration_queued": (
            "SELECT count(*) FROM registration_queue "
            "WHERE status='pending'"
        ),
    }
    return {
        name: int(conn.execute(sql).fetchone()[0])
        for name, sql in values.items()
    }
