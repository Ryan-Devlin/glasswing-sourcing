from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from db_backend import (
    PostgresConnection,
    acquire_advisory_lock_with_timeout,
    connect_postgres,
    using_postgres,
)

from .config import DATABASE

ZONE_UNIVERSE_DATABASE_ENV_VAR = "ZONE_UNIVERSE_DATABASE_URL"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def connect(
    path: Path | str = DATABASE,
    *,
    read_only: bool = False,
) -> sqlite3.Connection | PostgresConnection:
    # Its own env var, not the classification store's DATABASE_URL (already
    # live in production) or ai_search's AI_SEARCH_DATABASE_URL -- each
    # Postgres-backed subsystem needs an unambiguous, dedicated signal so
    # enabling one never silently enables another.
    if using_postgres(ZONE_UNIVERSE_DATABASE_ENV_VAR):
        conn = connect_postgres(
            read_only=read_only, env_var=ZONE_UNIVERSE_DATABASE_ENV_VAR
        )
        # Own "zone_universe" schema, not "public" -- classification_store,
        # continuous_pipeline, and ai_search all define same-named tables
        # (domains/pages/...) and could easily end up pointed at the same
        # physical Postgres instance (Railway only provides one today).
        if not read_only:
            conn.execute("CREATE SCHEMA IF NOT EXISTS zone_universe")
            conn.commit()
        conn.execute("SET search_path TO zone_universe, public")
        if not read_only:
            # worker.py runs the registration task and the main crawl loop as
            # separate concurrent asyncio tasks, each independently calling
            # connect() -- without serializing here, they'd race to run this
            # same schema DDL at once and deadlock on conflicting locks, the
            # same bug confirmed live in ai_search's identical connect().
            locked = acquire_advisory_lock_with_timeout(
                conn, "zone_universe_migration"
            )
            try:
                migrate_postgres(conn)
            finally:
                if locked:
                    conn.execute(
                        "SELECT pg_advisory_unlock(hashtext(?))",
                        ("zone_universe_migration",),
                    )
        return conn
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


_POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS zones (
    tld TEXT PRIMARY KEY,
    zone_path TEXT NOT NULL DEFAULT '',
    source_size BIGINT NOT NULL DEFAULT 0,
    source_modified_at TEXT NOT NULL DEFAULT '',
    source_fingerprint TEXT NOT NULL DEFAULT '',
    import_status TEXT NOT NULL DEFAULT 'not_imported',
    imported_at TEXT,
    discovered_domains INTEGER NOT NULL DEFAULT 0,
    shard_count INTEGER NOT NULL DEFAULT 0,
    active_queue_version TEXT NOT NULL DEFAULT '',
    import_error TEXT NOT NULL DEFAULT '',
    rejected_gibberish INTEGER NOT NULL DEFAULT 0,
    snapshot_path TEXT NOT NULL DEFAULT '',
    added_domains INTEGER NOT NULL DEFAULT 0,
    removed_domains INTEGER NOT NULL DEFAULT 0,
    unchanged_domains INTEGER NOT NULL DEFAULT 0,
    delta_added_path TEXT NOT NULL DEFAULT '',
    delta_removed_path TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS shards (
    tld TEXT NOT NULL,
    queue_version TEXT NOT NULL,
    shard INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    path TEXT NOT NULL,
    byte_offset BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    queued_domains INTEGER NOT NULL DEFAULT 0,
    attempted INTEGER NOT NULL DEFAULT 0,
    success INTEGER NOT NULL DEFAULT 0,
    dns_error INTEGER NOT NULL DEFAULT 0,
    timeout INTEGER NOT NULL DEFAULT 0,
    blocked INTEGER NOT NULL DEFAULT 0,
    http_error INTEGER NOT NULL DEFAULT 0,
    empty INTEGER NOT NULL DEFAULT 0,
    other_error INTEGER NOT NULL DEFAULT 0,
    preflight_checked INTEGER NOT NULL DEFAULT 0,
    preflight_promising INTEGER NOT NULL DEFAULT 0,
    preflight_rejected INTEGER NOT NULL DEFAULT 0,
    preflight_response_bytes BIGINT NOT NULL DEFAULT 0,
    skipped_existing INTEGER NOT NULL DEFAULT 0,
    skipped_low_priority INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(tld, queue_version, shard)
);
CREATE INDEX IF NOT EXISTS idx_shards_pending
    ON shards(tld, queue_version, priority, byte_offset, total_bytes);
CREATE TABLE IF NOT EXISTS pages (
    id SERIAL PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE,
    tld TEXT NOT NULL,
    requested_url TEXT NOT NULL DEFAULT '',
    final_url TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    meta_description TEXT NOT NULL DEFAULT '',
    headings TEXT NOT NULL DEFAULT '',
    cleaned_text TEXT NOT NULL DEFAULT '',
    raw_html_zlib BYTEA,
    raw_html_hash TEXT NOT NULL DEFAULT '',
    html_archive_path TEXT NOT NULL DEFAULT '',
    html_archive_offset BIGINT NOT NULL DEFAULT 0,
    html_archive_length BIGINT NOT NULL DEFAULT 0,
    html_archive_codec TEXT NOT NULL DEFAULT '',
    document_count INTEGER NOT NULL DEFAULT 1,
    raw_html_bytes BIGINT NOT NULL DEFAULT 0,
    compressed_html_bytes BIGINT NOT NULL DEFAULT 0,
    http_status INTEGER,
    word_count INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL DEFAULT '',
    is_parked INTEGER NOT NULL DEFAULT 0,
    is_coming_soon INTEGER NOT NULL DEFAULT 0,
    crawl_duration_ms INTEGER NOT NULL DEFAULT 0,
    enrichment_reason TEXT NOT NULL DEFAULT '',
    crawled_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    search_vector tsvector
);
CREATE INDEX IF NOT EXISTS idx_pages_tld_crawled ON pages(tld, crawled_at DESC);
CREATE INDEX IF NOT EXISTS idx_pages_word_count ON pages(word_count DESC);
CREATE INDEX IF NOT EXISTS idx_pages_raw_html_hash ON pages(raw_html_hash);
CREATE INDEX IF NOT EXISTS idx_pages_search_vector ON pages USING GIN(search_vector);
CREATE TABLE IF NOT EXISTS worker_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_log (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    tld TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_log_created ON event_log(created_at DESC);
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
    ON crawl_retries(tld, next_attempt_at, attempts);
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
    ON registration_queue(status, next_attempt_at, priority DESC, queued_at);
CREATE TABLE IF NOT EXISTS html_blobs (
    raw_html_hash TEXT PRIMARY KEY,
    archive_path TEXT NOT NULL,
    archive_offset BIGINT NOT NULL DEFAULT 0,
    archive_length BIGINT NOT NULL DEFAULT 0,
    archive_codec TEXT NOT NULL DEFAULT '',
    raw_bytes BIGINT NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE OR REPLACE VIEW domains AS
    SELECT domain, crawled_at AS last_crawled_at FROM pages;
"""


_BIGINT_COLUMNS = (
    ("zones", "source_size"),
    ("shards", "byte_offset"),
    ("shards", "total_bytes"),
    ("pages", "html_archive_offset"),
    ("pages", "html_archive_length"),
    ("pages", "raw_html_bytes"),
    ("pages", "compressed_html_bytes"),
    ("html_blobs", "archive_offset"),
    ("html_blobs", "archive_length"),
    ("html_blobs", "raw_bytes"),
)


def _already_bigint(conn: PostgresConnection, table: str, column: str) -> bool:
    row = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'zone_universe' AND table_name = ? "
        "AND column_name = ?",
        (table, column),
    ).fetchone()
    return bool(row) and str(row["data_type"]) == "bigint"


def migrate_postgres(conn: PostgresConnection) -> None:
    conn.executescript(_POSTGRES_SCHEMA)
    conn.commit()
    shard_columns = {
        str(row["column_name"])
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='zone_universe' AND table_name='shards'"
        ).fetchall()
    }
    shard_additions = {
        "preflight_checked": "INTEGER NOT NULL DEFAULT 0",
        "preflight_promising": "INTEGER NOT NULL DEFAULT 0",
        "preflight_rejected": "INTEGER NOT NULL DEFAULT 0",
        "preflight_response_bytes": "BIGINT NOT NULL DEFAULT 0",
    }
    for column, definition in shard_additions.items():
        if column not in shard_columns:
            conn.execute(
                f"ALTER TABLE shards ADD COLUMN {column} {definition}"
            )
    conn.commit()
    # These columns were originally created as plain INTEGER; a real .com
    # zone file's source_size (~4.8GB) overflows Postgres's 32-bit int range
    # (max ~2.1GB), something SQLite's flexible typing never caught. Widening
    # is a safe, lossless conversion, but `ALTER COLUMN TYPE` takes an
    # ACCESS EXCLUSIVE lock on the table even when the type already matches --
    # with a live crawler continuously writing to `pages`, re-running this
    # unconditionally on every connect() starves ordinary reads (confirmed
    # live: this is why the dashboard's own page-count query was hanging).
    # Checking the catalog first means the common case, already widened,
    # only costs a metadata lookup rather than an exclusive table lock.
    to_widen = [
        (table, column)
        for table, column in _BIGINT_COLUMNS
        if not _already_bigint(conn, table, column)
    ]
    if to_widen:
        conn.executescript(
            "".join(
                f"ALTER TABLE {table} ALTER COLUMN {column} TYPE BIGINT;\n"
                for table, column in to_widen
            )
        )
        conn.commit()
    # Confirmed live: event_log's SERIAL sequence was left behind the real
    # migrated data's max id (whatever bulk-copied it preserved explicit
    # ids without a matching setval() afterward), so the very first
    # log_event() insert on worker startup collided with an existing row
    # and crash-looped the whole process. Cheap and idempotent -- a no-op
    # once the sequence is already past the current max, so safe to run
    # on every connect() rather than only once.
    conn.execute(
        "SELECT setval(pg_get_serial_sequence(?, ?), "
        "COALESCE((SELECT MAX(id) FROM event_log), 1))",
        ("event_log", "id"),
    )
    conn.commit()


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
            preflight_checked INTEGER NOT NULL DEFAULT 0,
            preflight_promising INTEGER NOT NULL DEFAULT 0,
            preflight_rejected INTEGER NOT NULL DEFAULT 0,
            preflight_response_bytes INTEGER NOT NULL DEFAULT 0,
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
    shard_additions = {
        "preflight_checked": "INTEGER NOT NULL DEFAULT 0",
        "preflight_promising": "INTEGER NOT NULL DEFAULT 0",
        "preflight_rejected": "INTEGER NOT NULL DEFAULT 0",
        "preflight_response_bytes": "INTEGER NOT NULL DEFAULT 0",
    }
    for column, definition in shard_additions.items():
        if column not in shard_columns:
            conn.execute(
                f"ALTER TABLE shards ADD COLUMN {column} {definition}"
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
    postgres = isinstance(conn, PostgresConnection)
    searchable_pages_sql = (
        "SELECT count(*) FROM pages WHERE search_vector IS NOT NULL"
        if postgres
        else "SELECT count(*) FROM page_search"
    )
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
        "preflight_checked": (
            "SELECT coalesce(sum(preflight_checked),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "preflight_promising": (
            "SELECT coalesce(sum(preflight_promising),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "preflight_rejected": (
            "SELECT coalesce(sum(preflight_rejected),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "preflight_response_bytes": (
            "SELECT coalesce(sum(preflight_response_bytes),0) FROM shards s "
            "JOIN zones z ON z.tld=s.tld "
            "AND z.active_queue_version=s.queue_version"
        ),
        "success": "SELECT count(*) FROM pages",
        "searchable_pages": searchable_pages_sql,
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
            ") AS t),0)"
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
        name: int(conn.execute(f"SELECT ({sql}) AS count").fetchone()["count"])
        for name, sql in values.items()
    }
