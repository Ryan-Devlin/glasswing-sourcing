from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .domains import normalize_domain


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def connect(path: Path | str) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS domains (
        id INTEGER PRIMARY KEY,
        domain TEXT NOT NULL UNIQUE,
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        discovery_source TEXT NOT NULL DEFAULT 'manual',
        active INTEGER NOT NULL DEFAULT 1,
        latest_crawl_status TEXT NOT NULL DEFAULT 'pending',
        last_crawled_at TEXT,
        next_crawl_at TEXT,
        is_parked INTEGER NOT NULL DEFAULT 0,
        is_coming_soon INTEGER NOT NULL DEFAULT 0,
        crawl_failures INTEGER NOT NULL DEFAULT 0,
        last_success_at TEXT,
        last_document_count INTEGER NOT NULL DEFAULT 0,
        last_enrichment_reason TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY,
        domain_id INTEGER NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
        url TEXT NOT NULL,
        final_url TEXT NOT NULL,
        title TEXT NOT NULL DEFAULT '',
        meta_description TEXT NOT NULL DEFAULT '',
        headings TEXT NOT NULL DEFAULT '',
        visible_text TEXT NOT NULL DEFAULT '',
        cleaned_text TEXT NOT NULL DEFAULT '',
        raw_html TEXT NOT NULL DEFAULT '',
        canonical_url TEXT NOT NULL DEFAULT '',
        language TEXT NOT NULL DEFAULT '',
        emails TEXT NOT NULL DEFAULT '[]',
        social_links TEXT NOT NULL DEFAULT '[]',
        json_ld TEXT NOT NULL DEFAULT '[]',
        http_status INTEGER,
        content_type TEXT NOT NULL DEFAULT '',
        fetch_method TEXT NOT NULL DEFAULT 'raw_html',
        word_count INTEGER NOT NULL DEFAULT 0,
        content_hash TEXT NOT NULL DEFAULT '',
        crawled_at TEXT NOT NULL,
        UNIQUE(domain_id, final_url)
    );
    CREATE TABLE IF NOT EXISTS crawl_attempts (
        id INTEGER PRIMARY KEY,
        domain_id INTEGER NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
        requested_url TEXT NOT NULL,
        final_url TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        error_type TEXT NOT NULL DEFAULT '',
        error_message TEXT NOT NULL DEFAULT '',
        http_status INTEGER,
        duration_ms INTEGER NOT NULL DEFAULT 0,
        attempted_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS shortlists (
        domain_id INTEGER PRIMARY KEY REFERENCES domains(id) ON DELETE CASCADE,
        saved_at TEXT NOT NULL,
        note TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS watch_queries (
        id INTEGER PRIMARY KEY,
        query TEXT NOT NULL UNIQUE,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        last_searched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS worker_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS query_discoveries (
        watch_id INTEGER NOT NULL REFERENCES watch_queries(id) ON DELETE CASCADE,
        domain_id INTEGER NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
        provider TEXT NOT NULL,
        result_url TEXT NOT NULL DEFAULT '',
        result_rank INTEGER NOT NULL DEFAULT 0,
        discovered_at TEXT NOT NULL,
        PRIMARY KEY(watch_id, domain_id, provider)
    );
    CREATE TABLE IF NOT EXISTS html_blobs (
        raw_html_hash TEXT PRIMARY KEY,
        archive_path TEXT NOT NULL,
        archive_offset INTEGER NOT NULL DEFAULT 0,
        archive_length INTEGER NOT NULL DEFAULT 0,
        archive_codec TEXT NOT NULL DEFAULT '',
        raw_bytes INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    CREATE VIRTUAL TABLE IF NOT EXISTS page_search USING fts5(
        page_id UNINDEXED, domain, title, description, headings, body,
        tokenize='unicode61 remove_diacritics 2'
    );
    CREATE INDEX IF NOT EXISTS idx_domains_crawl
        ON domains(latest_crawl_status, next_crawl_at);
    CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain_id);
    CREATE INDEX IF NOT EXISTS idx_pages_crawled ON pages(crawled_at);
    """)
    watch_columns = {
        row["name"] for row in conn.execute(
            "PRAGMA table_info(watch_queries)"
        )
    }
    if "last_discovered_at" not in watch_columns:
        conn.execute(
            "ALTER TABLE watch_queries ADD COLUMN last_discovered_at TEXT"
        )
    if "last_discovery_status" not in watch_columns:
        conn.execute("""
            ALTER TABLE watch_queries
            ADD COLUMN last_discovery_status TEXT NOT NULL DEFAULT 'pending'
        """)
    domain_columns = {
        row["name"] for row in conn.execute(
            "PRAGMA table_info(domains)"
        )
    }
    if "crawl_failures" not in domain_columns:
        conn.execute(
            "ALTER TABLE domains "
            "ADD COLUMN crawl_failures INTEGER NOT NULL DEFAULT 0"
        )
    if "last_success_at" not in domain_columns:
        conn.execute(
            "ALTER TABLE domains ADD COLUMN last_success_at TEXT"
        )
    if "last_document_count" not in domain_columns:
        conn.execute(
            "ALTER TABLE domains ADD COLUMN "
            "last_document_count INTEGER NOT NULL DEFAULT 0"
        )
    if "last_enrichment_reason" not in domain_columns:
        conn.execute(
            "ALTER TABLE domains ADD COLUMN "
            "last_enrichment_reason TEXT NOT NULL DEFAULT ''"
        )
    page_columns = {
        row["name"] for row in conn.execute(
            "PRAGMA table_info(pages)"
        )
    }
    page_additions = {
        "raw_html_hash": "TEXT NOT NULL DEFAULT ''",
        "html_archive_path": "TEXT NOT NULL DEFAULT ''",
        "html_archive_offset": "INTEGER NOT NULL DEFAULT 0",
        "html_archive_length": "INTEGER NOT NULL DEFAULT 0",
        "html_archive_codec": "TEXT NOT NULL DEFAULT ''",
        "raw_html_bytes": "INTEGER NOT NULL DEFAULT 0",
        "compressed_html_bytes": "INTEGER NOT NULL DEFAULT 0",
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
    conn.commit()


def add_domain(
    conn: sqlite3.Connection, value: object, source: str = "manual"
) -> bool:
    domain = normalize_domain(value)
    if not domain:
        return False
    now = utc_now()
    conn.execute("""
        INSERT INTO domains(domain, first_seen_at, last_seen_at, discovery_source)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET last_seen_at=excluded.last_seen_at
    """, (domain, now, now, source))
    conn.commit()
    return True


def index_page(conn: sqlite3.Connection, page_id: int) -> None:
    row = conn.execute("""
        SELECT p.id, d.domain, p.title, p.meta_description, p.headings,
               p.cleaned_text
        FROM pages p JOIN domains d ON d.id=p.domain_id WHERE p.id=?
    """, (page_id,)).fetchone()
    if not row:
        return
    conn.execute("DELETE FROM page_search WHERE page_id=?", (page_id,))
    conn.execute("""
        INSERT INTO page_search(page_id, domain, title, description, headings, body)
        VALUES (?, ?, ?, ?, ?, ?)
    """, tuple(row))


def diagnostics(conn: sqlite3.Connection) -> dict[str, int | str]:
    result = {
        "domains": conn.execute("SELECT count(*) FROM domains").fetchone()[0],
        "crawled": conn.execute(
            "SELECT count(*) FROM domains WHERE last_crawled_at IS NOT NULL"
        ).fetchone()[0],
        "pages": conn.execute("SELECT count(*) FROM pages").fetchone()[0],
        "indexed_pages": conn.execute(
            "SELECT count(*) FROM page_search"
        ).fetchone()[0],
        "parked": conn.execute(
            "SELECT count(*) FROM domains WHERE is_parked=1"
        ).fetchone()[0],
        "coming_soon": conn.execute(
            "SELECT count(*) FROM domains WHERE is_coming_soon=1"
        ).fetchone()[0],
        "shortlisted": conn.execute(
            "SELECT count(*) FROM shortlists"
        ).fetchone()[0],
        "watched_queries": conn.execute(
            "SELECT count(*) FROM watch_queries WHERE active=1"
        ).fetchone()[0],
        "pending": conn.execute("""
            SELECT count(*) FROM domains
            WHERE active=1 AND last_crawled_at IS NULL
        """).fetchone()[0],
        "homepage_only": conn.execute(
            """
            SELECT count(*) FROM domains
            WHERE last_document_count=1
            """
        ).fetchone()[0],
        "evidence_enriched": conn.execute(
            """
            SELECT count(*) FROM domains
            WHERE last_document_count>1
            """
        ).fetchone()[0],
    }
    return result


def save_watch(conn: sqlite3.Connection, query: str) -> None:
    query = query.strip()
    if not query:
        return
    conn.execute("""
        INSERT INTO watch_queries(query, active, created_at)
        VALUES (?, 1, ?)
        ON CONFLICT(query) DO UPDATE SET active=1
    """, (query, utc_now()))
    conn.commit()


def set_worker_state(
    conn: sqlite3.Connection, key: str, value: object
) -> None:
    conn.execute("""
        INSERT INTO worker_state(key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value, updated_at=excluded.updated_at
    """, (key, str(value), utc_now()))
    conn.commit()


def worker_state(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM worker_state")
    }
