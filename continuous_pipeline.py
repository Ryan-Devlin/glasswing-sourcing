#!/usr/bin/env python3
"""Continuous CertStream -> queue -> scrape/filter -> selective Claude service."""

from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import gc
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

# Running as its own standalone service (pipeline-worker), this process never
# gets the sys.path setup the Streamlit pages do. filter.py's rejection-purge
# step lazily imports rejection_cleanup, which imports both crawler packages.
# Without these paths, that import crashes every batch that has any rejected
# domain (nearly all of them), and the whole batch gets marked failed and
# requeued instead of completing. Confirmed live via pipeline-worker's status
# file: failures first surfaced for ai_search and later for zone_universe.
_PROJECT_ROOT = Path(__file__).resolve().parent
_AI_SEARCH_SRC = _PROJECT_ROOT / "ai-domain-search-engine" / "src"
_ZONE_UNIVERSE_SRC = _PROJECT_ROOT / "zone-universe" / "src"
for _source_root in (_AI_SEARCH_SRC, _ZONE_UNIVERSE_SRC):
    if str(_source_root) not in sys.path:
        sys.path.insert(0, str(_source_root))

import filter as screening_filter
from classification_store import (
    frame_from_store,
    import_private_sqlite_into_shared,
    using_classification_postgres,
)
from db_backend import (
    acquire_advisory_lock_with_timeout,
    connect_postgres,
    using_postgres,
)

from collector import (
    SUPPORTED_EXTENSIONS,
    domain_extension,
    names_from_message,
    normalize_extensions,
    registrable_domain,
)
from common import (
    AI_SEARCH_DB,
    DATA,
    FILTERED,
    LLM,
    LOGS,
    QUEUE_DB,
    RUN_CONFIG,
    ROOT,
    STREAM_PID,
    STREAM_STATUS,
    STREAM_STOP,
    ZONE_UNIVERSE_DB,
    atomic_write_json,
    stable_domain,
)
from runtime_control import (
    RUNTIME_MODE,
    publish_pipeline_status,
    read_control,
    read_pipeline_config,
    shared_pipeline_backend_available,
)

load_dotenv(ROOT / ".env")

RUN_LOG = LOGS / "continuous_pipeline.log"
SHARED_CLASSIFICATION_MIGRATION_MARKER = (
    DATA / ".shared_classifications_imported_v1.json"
)
STATUS_WRITE_LOCK = threading.Lock()
SHARED_STATUS_PUBLISH_LOCK = threading.Lock()
SHARED_STATUS_MIN_INTERVAL_SECONDS = 5.0
SHARED_CONFIG_CACHE_SECONDS = 10.0
_last_shared_status_publish = 0.0
_shared_config_checked_at = 0.0
_shared_config_cache: dict[str, object] = {}

# Claude diligence pipeline (2026-08-08): fixed, not user-editable. A Needs
# Review company only ever reaches Claude at score 60+; KEEP is the only
# decision that ever becomes Investable. No dashboard toggle disables this
# -- "the website should be as easy as possible to use" means one correct
# pipeline, not a budget knob someone has to remember to tune.
CLAUDE_MINIMUM_SCORE = 60
CLAUDE_EXCEPTIONAL_SCORE = 90
CLAUDE_BATCH_MAX = 10
CLAUDE_HOURLY_LIMIT = 100
CLAUDE_DAILY_LIMIT = 400

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

def write_status(**updates: object) -> None:
    global _last_shared_status_publish
    with STATUS_WRITE_LOCK:
        current: dict[str, object] = {}
        if STREAM_STATUS.exists():
            try:
                current = json.loads(
                    STREAM_STATUS.read_text(encoding="utf-8")
                )
            except Exception:
                current = {}
        urgent = any(
            key in updates and updates[key] != current.get(key)
            for key in ("running", "stage", "connected", "last_error")
        )
        current.update(updates)
        atomic_write_json(STREAM_STATUS, current)
    if not shared_pipeline_backend_available():
        return
    now = time.monotonic()
    if not urgent and now - _last_shared_status_publish < SHARED_STATUS_MIN_INTERVAL_SECONDS:
        return
    if not SHARED_STATUS_PUBLISH_LOCK.acquire(blocking=False):
        return
    try:
        publish_pipeline_status(current)
        _last_shared_status_publish = now
    except Exception as exc:
        # Monitoring must never take down certificate ingestion or screening.
        print(f"shared pipeline status publish failed: {exc}", file=sys.stderr)
    finally:
        SHARED_STATUS_PUBLISH_LOCK.release()

def ensure_domain_registry_schema(
    conn: sqlite3.Connection,
) -> None:
    """Migrate older queue databases without deleting sourcing history."""
    existing_columns = {
        str(row["name"])
        for row in conn.execute(
            "PRAGMA table_info(domains)"
        ).fetchall()
    }
    additions = {
        "seen_count": (
            "INTEGER NOT NULL DEFAULT 1"
        ),
        "first_seen_run_id": (
            "TEXT NOT NULL DEFAULT ''"
        ),
        "last_seen_run_id": (
            "TEXT NOT NULL DEFAULT ''"
        ),
    }
    for column, definition in additions.items():
        if column not in existing_columns:
            conn.execute(
                f"ALTER TABLE domains "
                f"ADD COLUMN {column} {definition}"
            )

    conn.execute(
        """
        UPDATE domains
        SET seen_count = 1
        WHERE seen_count IS NULL OR seen_count < 1
        """
    )
    conn.commit()


def bootstrap_registry_from_screened_history(
    conn: sqlite3.Connection,
) -> int:
    """Seed saved history once, using one bulk transaction."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS registry_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    completed = conn.execute(
        "SELECT value FROM registry_meta WHERE key=?",
        ("history_bootstrap_v2",),
    ).fetchone()
    if completed:
        return 0

    # A preserved Step 1/2 database already contains the known-domain
    # registry. Do not reread and rewrite a large CSV on every startup.
    existing_count = int(
        conn.execute("SELECT COUNT(*) AS count FROM domains")
        .fetchone()["count"]
    )
    if existing_count > 0:
        conn.execute(
            "INSERT OR REPLACE INTO registry_meta(key, value) VALUES (?, ?)",
            ("history_bootstrap_v2", utc_now()),
        )
        conn.commit()
        return 0

    history_path = FILTERED / "all_screened.csv"
    if not history_path.exists() or history_path.stat().st_size == 0:
        conn.execute(
            "INSERT OR REPLACE INTO registry_meta(key, value) VALUES (?, ?)",
            ("history_bootstrap_v2", utc_now()),
        )
        conn.commit()
        return 0

    now = utc_now()
    rows_by_domain: dict[str, tuple[str, ...]] = {}
    try:
        with history_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                domain = stable_domain(row.get("domain", ""))
                if not domain:
                    continue
                first_seen = (
                    str(row.get("observed_at", "")).strip()
                    or str(row.get("run_started_at", "")).strip()
                    or now
                )
                run_id = str(row.get("run_id", "")).strip()
                classification = str(
                    row.get("routing_bucket", "")
                ).strip()
                rows_by_domain[domain] = (
                    domain,
                    first_seen,
                    first_seen,
                    first_seen,
                    classification,
                    run_id,
                    run_id,
                )
    except Exception:
        rows_by_domain = {}

    if rows_by_domain:
        conn.executemany(
            """
            INSERT OR IGNORE INTO domains(
                domain, first_seen, last_seen, status, attempts,
                next_attempt_at, last_processed_at, classification,
                last_error, seen_count, first_seen_run_id, last_seen_run_id
            )
            VALUES (?, ?, ?, 'complete', 1, NULL, ?, ?, '', 1, ?, ?)
            """,
            list(rows_by_domain.values()),
        )

    conn.execute(
        "INSERT OR REPLACE INTO registry_meta(key, value) VALUES (?, ?)",
        ("history_bootstrap_v2", utc_now()),
    )
    conn.commit()
    return len(rows_by_domain)


_PIPELINE_POSTGRES_SCHEMA = """
    CREATE TABLE IF NOT EXISTS domains (
        domain TEXT PRIMARY KEY,
        first_seen TEXT NOT NULL,
        last_seen TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TEXT,
        last_processed_at TEXT,
        classification TEXT,
        last_error TEXT DEFAULT '',
        seen_count INTEGER NOT NULL DEFAULT 1,
        first_seen_run_id TEXT NOT NULL DEFAULT '',
        last_seen_run_id TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS claude_calls (
        called_at TEXT NOT NULL,
        domain TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS registry_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
"""


PIPELINE_DATABASE_ENV_VAR = "PIPELINE_DATABASE_URL"


def connect_db() -> sqlite3.Connection:
    # Deliberately its own env var, not the classification store's
    # DATABASE_URL (already set in production). That variable being set
    # elsewhere must never be readable as "the pipeline queue is on
    # Postgres too" -- other code (og_bridge.pipeline_status_for_corpus)
    # inspects the queue db from outside this module and needs an
    # unambiguous, dedicated signal, not an overloaded shared one.
    if using_postgres(PIPELINE_DATABASE_ENV_VAR):
        return _connect_db_postgres()
    return _connect_db_sqlite()


def _connect_db_postgres() -> sqlite3.Connection:
    # A fresh Postgres-backed queue always gets the modern schema directly
    # (this session's full column set), so unlike the SQLite path there is
    # no legacy-column migration or local-CSV history bootstrap to run --
    # both of those exist only to carry forward a pre-existing local
    # installation, which a cloud database never has.
    #
    # Runs in its own "pipeline" schema, not the default "public" one --
    # ai_search/zone_universe define their own same-named domains/pages
    # tables, and classification_store already owns "public". Since Railway
    # only provides one Postgres instance today, all of these could easily
    # end up pointed at the same physical database; schemas keep that safe
    # (caught by actually running this against the real database -- a first
    # attempt without schema separation collided with ai_search's tables).
    conn = connect_postgres(env_var=PIPELINE_DATABASE_ENV_VAR)
    conn.execute("CREATE SCHEMA IF NOT EXISTS pipeline")
    conn.commit()
    conn.execute("SET search_path TO pipeline, public")
    # Defense in depth: ai_search's identical connect-then-migrate pattern
    # deadlocked live the moment multiple concurrent tasks each called it at
    # startup. This module mostly reuses one shared connection rather than
    # opening one per task, so the same race is less likely here -- but the
    # lock is free when uncontended, so there's no reason not to have it.
    locked = acquire_advisory_lock_with_timeout(conn, "pipeline_migration")
    try:
        conn.executescript(_PIPELINE_POSTGRES_SCHEMA)
        conn.commit()
    finally:
        if locked:
            conn.execute(
                "SELECT pg_advisory_unlock(hashtext(?))", ("pipeline_migration",)
            )
    return conn


def _connect_db_sqlite() -> sqlite3.Connection:
    QUEUE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(QUEUE_DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    search_db = AI_SEARCH_DB
    if search_db.exists():
        try:
            conn.execute(
                "ATTACH DATABASE ? AS ai_search",
                (str(search_db),),
            )
        except sqlite3.OperationalError:
            pass
    zone_db = ZONE_UNIVERSE_DB
    if zone_db.exists():
        try:
            conn.execute(
                "ATTACH DATABASE ? AS zone_universe",
                (str(zone_db),),
            )
        except sqlite3.OperationalError:
            pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            domain TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            last_processed_at TEXT,
            classification TEXT,
            last_error TEXT DEFAULT '',
            seen_count INTEGER NOT NULL DEFAULT 1,
            first_seen_run_id TEXT NOT NULL DEFAULT '',
            last_seen_run_id TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS claude_calls (
            called_at TEXT NOT NULL,
            domain TEXT NOT NULL
        )
    """)
    ensure_domain_registry_schema(conn)

    # Bootstrap only the real project registry. Temporary/test databases
    # must remain isolated from saved project history.
    try:
        is_project_registry = (
            QUEUE_DB.parent.resolve()
            == (ROOT / "data").resolve()
        )
    except Exception:
        is_project_registry = False

    if is_project_registry:
        bootstrap_registry_from_screened_history(conn)

    conn.commit()
    return conn

def selected_extensions() -> set[str]:
    config = load_run_config()
    configured = config.get("selected_extensions", [".ai"])
    if isinstance(configured, str):
        configured = configured.split(",")
    return normalize_extensions(list(configured))


def extension_counts(
    conn: sqlite3.Connection,
) -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {
        extension: {
            "pending": 0,
            "processing": 0,
            "complete": 0,
            "failed": 0,
            "total": 0,
        }
        for extension in sorted(SUPPORTED_EXTENSIONS)
    }
    rows = conn.execute(
        """
        SELECT domain, status
        FROM domains
        """
    ).fetchall()
    for row in rows:
        extension = domain_extension(row["domain"])
        if extension not in results:
            continue
        status = str(row["status"])
        results[extension][status] = (
            results[extension].get(status, 0) + 1
        )
        results[extension]["total"] += 1
    return results


def register_domain_event(
    conn: sqlite3.Connection,
    domain: str,
    allowed_extensions: set[str] | None = None,
    *,
    run_id: str = "",
) -> dict[str, object]:
    """Register one CertStream event without requeueing known domains."""
    domain = stable_domain(domain)
    allowed = allowed_extensions or selected_extensions()
    extension = domain_extension(domain)

    if not domain or extension not in allowed:
        return {
            "accepted": False,
            "queued": False,
            "is_new_domain": False,
            "is_repeat_event": False,
            "domain": domain,
            "extension": extension,
        }

    now = utc_now()
    cursor = conn.execute(
        """
        INSERT INTO domains(
            domain,
            first_seen,
            last_seen,
            status,
            attempts,
            next_attempt_at,
            seen_count,
            first_seen_run_id,
            last_seen_run_id
        )
        VALUES(?, ?, ?, 'pending', 0, ?, 1, ?, ?)
        ON CONFLICT(domain) DO NOTHING
        """,
        (
            domain,
            now,
            now,
            now,
            run_id,
            run_id,
        ),
    )
    if cursor.rowcount == 1:
        conn.commit()
        return {
            "accepted": True,
            "queued": True,
            "is_new_domain": True,
            "is_repeat_event": False,
            "domain": domain,
            "extension": extension,
            "seen_count": 1,
        }

    # Confirmed live: this ran as a separate UPDATE, then a SELECT, then a
    # commit -- three synchronous round trips for every single repeat
    # certificate event (thousands of them per run: "5741 repeat events
    # ignored" observed live), on the same event loop the worker's own
    # outer batch deadline depends on. One UPDATE ... RETURNING cuts that
    # to two round trips (the write, then the commit) without changing
    # what gets stored or returned.
    row = conn.execute(
        """
        UPDATE domains
        SET last_seen = ?,
            seen_count = seen_count + 1,
            last_seen_run_id = ?
        WHERE domain = ?
        RETURNING status, seen_count
        """,
        (
            now,
            run_id,
            domain,
        ),
    ).fetchone()
    conn.commit()

    # A repeat certificate event is evidence that the domain is active,
    # not evidence that a new startup launched. Never requeue it here.
    return {
        "accepted": True,
        "queued": False,
        "is_new_domain": False,
        "is_repeat_event": True,
        "domain": domain,
        "extension": extension,
        "seen_count": int(row["seen_count"] or 1),
        "existing_status": str(row["status"]),
    }


def enqueue(
    conn: sqlite3.Connection,
    domain: str,
    allowed_extensions: set[str] | None = None,
) -> bool:
    """Backward-compatible boolean wrapper used by tests and utilities."""
    result = register_domain_event(
        conn,
        domain,
        allowed_extensions,
    )
    return bool(result.get("queued", False))

def _corpus_priority_for_domains(domains: list[str]) -> dict[str, int]:
    """Priority tier (lower = crawled sooner) for candidate queue domains.

    Postgres mode can't ATTACH the (still-local) ai_search/zone_universe
    SQLite files the way the SQLite queue path below does, so this opens
    them directly and looks up just the candidate domains instead of the
    full corpus. Mirrors claim_batch's SQLite-mode tiers: zone_universe page
    evidence (0) outranks ai_search crawl evidence (1) outranks neither (2).
    """
    if not domains:
        return {}
    placeholders = ",".join("?" for _ in domains)
    priority: dict[str, int] = {}
    if ZONE_UNIVERSE_DB.exists():
        conn = sqlite3.connect(ZONE_UNIVERSE_DB, timeout=30)
        try:
            rows = conn.execute(
                f"SELECT DISTINCT domain FROM pages WHERE domain IN ({placeholders})",
                domains,
            ).fetchall()
            for row in rows:
                priority[row[0]] = 0
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    if AI_SEARCH_DB.exists():
        conn = sqlite3.connect(AI_SEARCH_DB, timeout=30)
        try:
            rows = conn.execute(
                f"SELECT domain FROM domains "
                f"WHERE last_crawled_at IS NOT NULL AND domain IN ({placeholders})",
                domains,
            ).fetchall()
            for row in rows:
                priority.setdefault(row[0], 1)
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    return priority


def claim_batch(
    conn: sqlite3.Connection,
    size: int,
    allowed_extensions: set[str] | None = None,
) -> list[str]:
    now = utc_now()
    if using_postgres(PIPELINE_DATABASE_ENV_VAR):
        # No ATTACH equivalent in Postgres, so pull a bounded candidate
        # window ordered the normal way, then re-rank it in Python using the
        # local corpora above. Bounding this window (rather than scanning
        # every pending row) keeps a single claim cheap regardless of how
        # deep the real-time queue gets.
        candidate_limit = min(max(size * 10, size), 2000)
        rows = conn.execute(
            """
            SELECT domain, first_seen, last_seen_run_id FROM domains
            WHERE status='pending'
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY
              CASE WHEN last_seen_run_id='ai_search_cache_reuse_v1' THEN 0 ELSE 1 END,
              first_seen DESC
            LIMIT ?
            """,
            (now, candidate_limit),
        ).fetchall()
        if allowed_extensions:
            rows = [
                row for row in rows
                if (
                    domain_extension(str(row["domain"])) in allowed_extensions
                    or str(row["last_seen_run_id"] or "")
                    == "ai_search_cache_reuse_v1"
                )
            ]
        priority = _corpus_priority_for_domains([row["domain"] for row in rows])
        # Newest first within each priority tier -- confirmed live: a
        # 121,000+ historical backlog (oldest-first) meant freshly arriving
        # domains would never be reached for days, even after fixing the
        # bugs actually causing the backlog. rows already arrive newest
        # first from the query above; sort on priority alone (stable sort
        # preserves that ordering within each tier) instead of re-sorting
        # by first_seen ascending.
        ordered = sorted(rows, key=lambda row: priority.get(row["domain"], 2))
        domains = [row["domain"] for row in ordered[:size]]
        if not domains:
            return []
        conn.executemany(
            """
            UPDATE domains
            SET status='processing', attempts=attempts+1
            WHERE domain=?
            """,
            [(domain,) for domain in domains],
        )
        conn.commit()
        return domains

    attached = {
        str(row[1])
        for row in conn.execute("PRAGMA database_list").fetchall()
    }
    if "ai_search" in attached and "zone_universe" in attached:
        rows = conn.execute(
            """
            SELECT queue.domain
            FROM main.domains queue
            LEFT JOIN ai_search.domains crawled
              ON crawled.domain=queue.domain
             AND crawled.last_crawled_at IS NOT NULL
            LEFT JOIN zone_universe.pages zone_page
              ON zone_page.domain=queue.domain
            WHERE queue.status='pending'
              AND (
                queue.next_attempt_at IS NULL
                OR queue.next_attempt_at <= ?
              )
            ORDER BY
              CASE
                WHEN zone_page.domain IS NOT NULL THEN 0
                WHEN crawled.domain IS NOT NULL THEN 1
                ELSE 2
              END,
              queue.first_seen DESC
            LIMIT ?
            """,
            (now, size),
        ).fetchall()
    elif "ai_search" in attached:
        rows = conn.execute(
            """
            SELECT queue.domain
            FROM main.domains queue
            LEFT JOIN ai_search.domains crawled
              ON crawled.domain=queue.domain
             AND crawled.last_crawled_at IS NOT NULL
            WHERE queue.status='pending'
              AND (
                queue.next_attempt_at IS NULL
                OR queue.next_attempt_at <= ?
              )
            ORDER BY
              CASE WHEN crawled.domain IS NOT NULL THEN 0 ELSE 1 END,
              queue.first_seen DESC
            LIMIT ?
            """,
            (now, size),
        ).fetchall()
    elif "zone_universe" in attached:
        rows = conn.execute(
            """
            SELECT queue.domain
            FROM main.domains queue
            LEFT JOIN zone_universe.pages zone_page
              ON zone_page.domain=queue.domain
            WHERE queue.status='pending'
              AND (
                queue.next_attempt_at IS NULL
                OR queue.next_attempt_at <= ?
              )
            ORDER BY
              CASE WHEN zone_page.domain IS NOT NULL THEN 0 ELSE 1 END,
              queue.first_seen DESC
            LIMIT ?
            """,
            (now, size),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT domain
            FROM domains
            WHERE status='pending'
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY first_seen DESC
            LIMIT ?
            """,
            (now, size),
        ).fetchall()

    row_domains = [str(row["domain"]) for row in rows]
    manual_domains: set[str] = set()
    if allowed_extensions and row_domains:
        placeholders = ",".join("?" for _ in row_domains)
        manual_domains = {
            str(row["domain"])
            for row in conn.execute(
                f"SELECT domain FROM domains WHERE domain IN ({placeholders}) "
                "AND last_seen_run_id='ai_search_cache_reuse_v1'",
                row_domains,
            ).fetchall()
        }
    domains = [
        domain for domain in row_domains
        if not allowed_extensions
        or domain_extension(domain) in allowed_extensions
        or domain in manual_domains
    ]
    if not domains:
        return []

    conn.executemany(
        """
        UPDATE domains
        SET status='processing', attempts=attempts+1
        WHERE domain=?
        """,
        [(domain,) for domain in domains],
    )
    conn.commit()
    return domains

def clear_stale_work_queue(conn: sqlite3.Connection) -> int:
    """Return interrupted claims to the durable queue after a restart."""
    cursor = conn.execute(
        """
        UPDATE domains
        SET status='pending',
            next_attempt_at=?,
            last_error='interrupted_work_requeued_on_restart'
        WHERE status='processing'
        """,
        (utc_now(),),
    )
    conn.commit()
    return int(cursor.rowcount or 0)


def discard_pending_by_extension(
    conn: sqlite3.Connection,
    extension: str | None,
) -> int:
    """Bulk-discard not-yet-attempted queue rows, optionally for one TLD.

    extension=None wipes the entire not-yet-attempted queue regardless of
    TLD -- for clearing an enormous historical backlog (confirmed live:
    121,000+ pending rows, accumulated over days, that dominated every
    claim_batch() call regardless of which TLD's misconfiguration was fixed)
    so the worker starts fresh from newly-arriving CertStream domains only.
    This only ever touches the pipeline's own scheduling queue
    (undiscovered, unscored hostnames) -- never classifications, training
    truth, or any human decision.
    """
    rows = conn.execute(
        "SELECT domain FROM domains WHERE status IN ('pending', 'processing')"
    ).fetchall()
    to_discard = [
        str(row["domain"])
        for row in rows
        if extension is None or domain_extension(str(row["domain"])) == extension
    ]
    if not to_discard:
        return 0
    reason = (
        "full_queue_wipe_requested"
        if extension is None
        else "stale_extension_backlog_discarded"
    )
    for start in range(0, len(to_discard), 800):
        batch = to_discard[start : start + 800]
        marks = ",".join("?" for _ in batch)
        conn.execute(
            f"""
            UPDATE domains
            SET status='discarded', next_attempt_at=NULL,
                last_error=?
            WHERE domain IN ({marks})
            """,
            [reason, *batch],
        )
    conn.commit()
    return len(to_discard)


def reset_certstream_queue(conn: sqlite3.Connection) -> dict[str, int]:
    """Wipe the entire CertStream domain queue and Claude call log.

    Removes every row (any status: pending, processing, complete, failed,
    discarded) from `domains`, plus every row from `claude_calls`. Never
    touches the classifications store, training truth, or manual decisions
    -- those live in a completely separate system. `registry_meta` is left
    alone so the one-time history-bootstrap flag doesn't re-fire and
    re-seed the now-empty table from the classification export.
    """
    # psycopg's dict_row (Postgres) only supports name-based access, not
    # row[0] -- SQLite's Row supports both, which is why this only breaks
    # in production. AS count + ["count"] works on both backends.
    domain_count = int(
        conn.execute("SELECT COUNT(*) AS count FROM domains")
        .fetchone()["count"]
    )
    claude_call_count = int(
        conn.execute("SELECT COUNT(*) AS count FROM claude_calls")
        .fetchone()["count"]
    )
    conn.execute("DELETE FROM domains")
    conn.execute("DELETE FROM claude_calls")
    conn.commit()
    return {
        "domains_deleted": domain_count,
        "claude_calls_deleted": claude_call_count,
    }


def queue_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS count FROM domains GROUP BY status"
    ).fetchall()
    counts = {row["status"]: int(row["count"]) for row in rows}
    counts["total"] = sum(counts.values())
    return counts

def write_batch_csv(domains: list[str]) -> Path:
    tmp_dir = DATA / "stream_batches"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    path = tmp_dir / f"batch_{int(time.time() * 1000)}.csv"
    now = utc_now()
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["observed_at", "registrable", "hostname"],
        )
        writer.writeheader()
        for domain in domains:
            writer.writerow({
                "observed_at": now,
                "registrable": domain,
                "hostname": domain,
            })
    return path

def subprocess_logged(command: list[str]) -> None:
    with RUN_LOG.open("a", encoding="utf-8") as log:
        log.write(f"\n[{utc_now()}] {' '.join(command)}\n")
        log.flush()
        subprocess.run(
            command,
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
        )

def classification_map() -> dict[str, str]:
    try:
        from classification_store import classification_map as store_map

        result = store_map()
        if result:
            return result
    except Exception:
        pass

    path = FILTERED / "all_screened.csv"
    if not path.exists() or path.stat().st_size == 0:
        return {}
    result: dict[str, str] = {}
    try:
        with path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                domain = stable_domain(row.get("domain", ""))
                if domain:
                    result[domain] = row.get("routing_bucket", "")
    except Exception:
        return {}
    return result

def mark_success(conn: sqlite3.Connection, domains: list[str]) -> None:
    now = utc_now()
    classes = classification_map()

    for domain in domains:
        classification = classes.get(domain, "")
        last_error = (
            "website_unreachable_after_immediate_retry"
            if classification == "ghost"
            else ""
        )
        conn.execute(
            """
            UPDATE domains
            SET status='complete',
                next_attempt_at=NULL,
                last_processed_at=?,
                classification=?,
                last_error=?
            WHERE domain=?
            """,
            (now, classification, last_error, domain),
        )
    conn.commit()

def persist_unreachable_domains(
    domains_with_errors: list[tuple[str, str]],
) -> None:
    """Give up-for-good domains a real, visible bucket instead of vanishing.

    Confirmed: a domain that raises asyncio.TimeoutError on every fetch
    attempt never produces a `page`, so it never reaches process()/
    route_company() at all -- screen_records excludes it from `output`
    entirely and mark_failure only ever updated the pipeline's own queue
    table. After max_attempts (~3, roughly 6-10 minutes of backoff) it
    just became status='failed' in the queue with zero classification
    record, ever -- not delayed, gone. This persists a minimal,
    clearly-labeled "insufficient" record for each one instead, so every
    domain the pipeline actually attempted ends up in some bucket.
    """
    import pandas as pd

    from classification_store import upsert_classifications

    records = []
    for domain, error in domains_with_errors:
        domain = stable_domain(domain)
        if not domain:
            continue
        records.append({
            "domain": domain,
            "final_url": f"https://{domain}",
            "http_status": "",
            "page_title": "",
            "meta_description": "",
            "homepage_excerpt": "",
            "routing_bucket": "insufficient",
            "routing_reason": "website_never_reachable_after_max_attempts",
            "fetch_error": error,
        })
    if records:
        upsert_classifications(pd.DataFrame(records))


def mark_failure(
    conn: sqlite3.Connection,
    domains: list[str],
    error: str,
    max_attempts: int,
) -> None:
    now = datetime.now(timezone.utc)
    gave_up: list[tuple[str, str]] = []
    for domain in domains:
        row = conn.execute(
            "SELECT attempts FROM domains WHERE domain=?",
            (domain,),
        ).fetchone()
        attempts = int(row["attempts"]) if row else max_attempts
        if attempts >= max_attempts:
            conn.execute(
                """
                UPDATE domains
                SET status='failed', last_processed_at=?, last_error=?
                WHERE domain=?
                """,
                (utc_now(), error[:500], domain),
            )
            gave_up.append((domain, error))
        else:
            delay_minutes = min(60, 2 ** attempts)
            retry_at = (
                now + timedelta(minutes=delay_minutes)
            ).isoformat().replace("+00:00", "Z")
            conn.execute(
                """
                UPDATE domains
                SET status='pending', next_attempt_at=?, last_error=?
                WHERE domain=?
                """,
                (retry_at, error[:500], domain),
            )
    conn.commit()
    if gave_up:
        persist_unreachable_domains(gave_up)


def load_run_config() -> dict:
    global _shared_config_cache, _shared_config_checked_at
    if shared_pipeline_backend_available():
        now = time.monotonic()
        if now - _shared_config_checked_at >= SHARED_CONFIG_CACHE_SECONDS:
            try:
                shared = read_pipeline_config()
            except Exception as exc:
                print(f"shared pipeline config read failed: {exc}", file=sys.stderr)
            else:
                if shared:
                    _shared_config_cache = shared
            _shared_config_checked_at = now
        if _shared_config_cache:
            return dict(_shared_config_cache)
    if not RUN_CONFIG.exists():
        return {}
    try:
        return json.loads(RUN_CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return {}

def claude_allowance(conn: sqlite3.Connection) -> int:
    config = load_run_config()
    daily_limit = max(1, min(5000, int(config.get("claude_daily_limit", CLAUDE_DAILY_LIMIT))))
    hourly_limit = min(CLAUDE_HOURLY_LIMIT, daily_limit)
    now = datetime.now(timezone.utc)
    hour_cutoff = (
        now - timedelta(hours=1)
    ).isoformat().replace("+00:00", "Z")
    day_cutoff = (
        now - timedelta(days=1)
    ).isoformat().replace("+00:00", "Z")
    hour_count = conn.execute(
        "SELECT COUNT(*) AS count FROM claude_calls WHERE called_at >= ?",
        (hour_cutoff,),
    ).fetchone()["count"]
    day_count = conn.execute(
        "SELECT COUNT(*) AS count FROM claude_calls WHERE called_at >= ?",
        (day_cutoff,),
    ).fetchone()["count"]

    allowance = max(
        0,
        min(
            hourly_limit - hour_count,
            daily_limit - day_count,
        ),
    )
    if allowance <= 0:
        write_status(
            claude_warning=(
                "Claude diligence is paused by its configured budget: "
                f"{hour_count}/{hourly_limit} calls this hour and "
                f"{day_count}/{daily_limit} calls today. Resumes "
                "automatically once the window rolls over."
            ),
            updated_at=utc_now(),
        )
    return allowance


def diligenced_domains() -> set[str]:
    """Return domains with persisted Claude output for the active prompt."""
    from classification_store import claude_diligenced_domains
    from diligence_logic import DILIGENCE_VERSION

    domains: set[str] = claude_diligenced_domains(
        diligence_version=DILIGENCE_VERSION,
    )
    for path in (LLM / "llm_approved_master.csv", LLM / "llm_denied_master.csv"):
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            with path.open(newline="", encoding="utf-8") as handle:
                domains.update(
                    stable_domain(row.get("domain", ""))
                    for row in csv.DictReader(handle)
                )
        except Exception:
            continue
    return {domain for domain in domains if domain}

def maybe_run_claude(batch_path: Path) -> int:
    """Run optional Claude diligence without blocking Python screening."""
    conn = connect_db()
    try:
        allowance = claude_allowance(conn)
        if allowance <= 0:
            return 0

        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            write_status(
                claude_warning="Claude skipped: ANTHROPIC_API_KEY is missing.",
                updated_at=utc_now(),
            )
            return 0

        if using_classification_postgres():
            # bulk_compatibility_exports_enabled() deliberately disables
            # refreshing all_screened.csv in cloud/Postgres mode (keeps
            # enormous CSV mirrors off worker volumes) -- but this function
            # still needs *some* real file to hand claude_classifier.py.
            # Confirmed live: with that export disabled, this stale/absent
            # local file silently zeroed out every Claude call for two full
            # days despite plenty of freshly-screened, above-threshold
            # domains sitting in the (Postgres) classification store the
            # whole time. Build a fresh candidate file straight from the
            # real store instead of depending on that local mirror at all.
            frame = frame_from_store()
            if frame.empty:
                return 0
            candidates = FILTERED / "all_screened_live.csv"
            frame.to_csv(candidates, index=False)
        else:
            candidates = FILTERED / "all_screened.csv"
            if not candidates.exists() or candidates.stat().st_size == 0:
                return 0

        calls = min(allowance, CLAUDE_BATCH_MAX)
        if calls <= 0:
            return 0

        command = [
            sys.executable,
            "claude_classifier.py",
            str(candidates),
            "--minimum-score",
            str(CLAUDE_MINIMUM_SCORE),
            "--exceptional-score",
            str(CLAUDE_EXCEPTIONAL_SCORE),
            "--percentage-cap",
            "1.0",
            "--max-calls",
            str(calls),
        ]
        attempt_report = LLM / "claude_attempt_report.json"
        attempt_report.unlink(missing_ok=True)
        command.extend(["--attempt-report", str(attempt_report)])

        completed_before = diligenced_domains()
        try:
            subprocess_logged(command)
        except subprocess.CalledProcessError as exc:
            # Claude is an optional enrichment stage. A Claude/API/model
            # failure must never cause screened domains to be retried or block
            # the queue.
            write_status(
                claude_warning=(
                    "Claude diligence failed and was skipped. "
                    "Python screening is continuing. "
                    f"Exit code: {exc.returncode}."
                ),
                updated_at=utc_now(),
            )
            return 0
        except Exception as exc:
            write_status(
                claude_warning=(
                    "Claude diligence was skipped because of "
                    f"{type(exc).__name__}: {exc}"
                )[:500],
                updated_at=utc_now(),
            )
            return 0

        now = utc_now()
        domains = sorted(diligenced_domains() - completed_before)
        attempted_domains = domains
        try:
            report = json.loads(attempt_report.read_text(encoding="utf-8"))
            attempted_domains = list(dict.fromkeys(
                stable_domain(domain)
                for domain in report.get("attempted_domains", [])
                if stable_domain(domain)
            ))
        except (OSError, ValueError, TypeError):
            pass

        conn.executemany(
            "INSERT INTO claude_calls(called_at, domain) VALUES (?, ?)",
            [
                (now, domain)
                for domain in attempted_domains
                if domain
            ],
        )
        conn.commit()
        # Master/local Investable-by-Claude views are a downstream
        # convenience export, not the diligence result itself -- a failure
        # here must never make a real Claude call look like it didn't
        # happen, so it only ever sets a warning, never blocks the return.
        views_warning = ""
        try:
            screening_filter.persist_claude_decision_views()
        except Exception as exc:
            views_warning = (
                "Claude ran, but updating the master/local Investable "
                f"views failed: {type(exc).__name__}: {exc}"
            )[:500]
        write_status(
            claude_warning=views_warning,
            updated_at=utc_now(),
        )
        return len(attempted_domains)
    finally:
        conn.close()

async def listener(
    conn: sqlite3.Connection,
    stop: asyncio.Event,
) -> None:
    allowed_extensions = selected_extensions()
    listener_status = read_status()
    active_run_id = str(
        listener_status.get("run_id", "")
    ).strip()

    received_by_extension = {
        extension: 0
        for extension in sorted(allowed_extensions)
    }
    repeats_by_extension = {
        extension: 0
        for extension in sorted(allowed_extensions)
    }
    certificate_events = int(
        listener_status.get("certificate_events_received", 0) or 0
    )
    new_domains = int(
        listener_status.get(
            "new_domains_received",
            listener_status.get("domains_received", 0),
        )
        or 0
    )
    repeat_events = int(
        listener_status.get("repeat_events_ignored", 0) or 0
    )
    last_repeat_domain = ""
    repeat_status_dirty = False
    repeat_status_flush_seconds = max(
        0.5,
        float(os.getenv("REPEAT_STATUS_FLUSH_SECONDS", "2")),
    )
    repeat_status_flushed_at = time.monotonic()

    def flush_repeat_status(*, force: bool = False) -> None:
        """Coalesce noisy repeat telemetry without losing its counters."""
        nonlocal repeat_status_dirty, repeat_status_flushed_at
        if not repeat_status_dirty:
            return
        now_monotonic = time.monotonic()
        if (
            not force
            and now_monotonic - repeat_status_flushed_at
            < repeat_status_flush_seconds
        ):
            return
        write_status(
            certificate_events_received=certificate_events,
            repeat_events_ignored=repeat_events,
            repeats_by_extension=repeats_by_extension,
            last_repeat_domain=last_repeat_domain,
            updated_at=utc_now(),
        )
        repeat_status_dirty = False
        repeat_status_flushed_at = now_monotonic

    write_status(
        selected_extensions=sorted(allowed_extensions),
        received_by_extension=received_by_extension,
        repeats_by_extension=repeats_by_extension,
    )

    url = os.getenv(
        "CERTSTREAM_URL",
        "ws://localhost:8080/domains-only",
    )
    reconnect_seconds = float(
        os.getenv("STREAM_RECONNECT_SECONDS", "5")
    )
    reconnect_count = int(
        listener_status.get("stream_reconnect_count", 0) or 0
    )
    # This task is created once per container lifetime (main() never
    # restarts it), so computing allowed_extensions only once above meant
    # changing Extensions in the dashboard had zero effect until the whole
    # container redeployed -- confirmed live. Re-read it periodically
    # instead of caching it for the task's entire lifetime.
    extensions_refresh_seconds = 5.0
    extensions_refreshed_at = time.monotonic()

    while not stop.is_set():
        if RUNTIME_MODE != "local" and not read_control()["desired"].get(
            "pipeline", True
        ):
            # A user pause means pause acquisition too, not merely screening.
            # Disconnect so CertStream cannot buffer an invisible burst that
            # floods the queue on resume.
            write_status(
                connected=False,
                stream_connection_state="paused",
                stage="paused",
                updated_at=utc_now(),
            )
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            # Connection state and processing state are independent. The
            # listener must never overwrite an active worker-owned
            # ``screening``/``paused`` stage merely because it connects,
            # receives an event, or retries the websocket.
            write_status(
                connected=False,
                stream_connection_state="connecting",
            )
            async with websockets.connect(
                url,
                ping_interval=20,
                # The listener also performs local SQLite bookkeeping. Under
                # a large certificate burst that synchronous work can delay
                # the event loop long enough to miss an aggressive pong
                # deadline even though local CertStream is healthy. Keep
                # pings enabled, but don't close a useful stream solely for
                # a delayed pong; recv() still detects a real disconnect.
                ping_timeout=None,
            ) as websocket:
                write_status(
                    connected=True,
                    stream_connection_state="connected",
                    last_error="",
                    updated_at=utc_now(),
                )

                while not stop.is_set():
                    if RUNTIME_MODE != "local" and not read_control()["desired"].get(
                        "pipeline", True
                    ):
                        # Exiting the context closes the socket promptly. The
                        # worker may safely finish its already-claimed batch.
                        write_status(
                            connected=False,
                            stream_connection_state="paused",
                            stage="paused",
                            updated_at=utc_now(),
                        )
                        break
                    now_monotonic = time.monotonic()
                    if (
                        now_monotonic - extensions_refreshed_at
                        >= extensions_refresh_seconds
                    ):
                        extensions_refreshed_at = now_monotonic
                        refreshed = selected_extensions()
                        if refreshed != allowed_extensions:
                            allowed_extensions = refreshed
                            for extension in allowed_extensions:
                                received_by_extension.setdefault(
                                    extension, 0
                                )
                                repeats_by_extension.setdefault(
                                    extension, 0
                                )
                            write_status(
                                selected_extensions=sorted(
                                    allowed_extensions
                                ),
                                updated_at=utc_now(),
                            )

                    try:
                        raw = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=1.0,
                        )
                    except asyncio.TimeoutError:
                        flush_repeat_status()
                        continue

                    # SAN certificates often contain many subdomains for the
                    # same root. Count that root once per certificate message.
                    message_domains = {
                        domain
                        for hostname in names_from_message(raw)
                        if (
                            domain := registrable_domain(
                                hostname,
                                allowed_extensions,
                            )
                        )
                    }

                    for domain in sorted(message_domains):
                        _register_started = time.monotonic()
                        event = register_domain_event(
                            conn,
                            domain,
                            allowed_extensions,
                            run_id=active_run_id,
                        )
                        _register_elapsed = (
                            time.monotonic() - _register_started
                        )
                        if _register_elapsed > 0.5:
                            # print()-based diagnostics never reached
                            # anywhere observable -- confirmed live:
                            # railway.pipeline-worker.json runs a bare
                            # "python continuous_pipeline.py" with no log
                            # capture wired up, so stderr output was
                            # invisible for the entire investigation.
                            # write_status() already reaches the live
                            # dashboard reliably.
                            write_status(
                                last_slow_register_domain_event={
                                    "domain": domain,
                                    "seconds": round(_register_elapsed, 3),
                                    "logged_at": utc_now(),
                                }
                            )
                        if not event.get("accepted"):
                            continue

                        extension = str(
                            event.get("extension", "")
                        )
                        certificate_events += 1

                        if event.get("is_new_domain"):
                            # Flush repeat telemetry first so this immediate
                            # new-domain update cannot publish stale totals.
                            flush_repeat_status(force=True)
                            received_by_extension[extension] = (
                                received_by_extension.get(extension, 0)
                                + 1
                            )
                            new_domains += 1
                            write_status(
                                certificate_events_received=(
                                    certificate_events
                                ),
                                new_domains_received=new_domains,
                                domains_received=new_domains,
                                received_by_extension=(
                                    received_by_extension
                                ),
                                last_domain_received=domain,
                                last_domain_extension=extension,
                                queue_by_extension=(
                                    extension_counts(conn)
                                ),
                                updated_at=utc_now(),
                            )
                        else:
                            repeats_by_extension[extension] = (
                                repeats_by_extension.get(extension, 0)
                                + 1
                            )
                            repeat_events += 1
                            last_repeat_domain = domain
                            repeat_status_dirty = True
                            flush_repeat_status()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reconnect_count += 1
            disconnect_detail = (
                f"{type(exc).__name__}: {exc}"
            )[:500]
            write_status(
                connected=False,
                stream_connection_state="reconnecting",
                last_error=disconnect_detail,
                last_stream_disconnect=disconnect_detail,
                last_stream_disconnect_at=utc_now(),
                stream_reconnect_count=reconnect_count,
                updated_at=utc_now(),
            )
            await asyncio.sleep(reconnect_seconds)


def read_status() -> dict[str, object]:
    if not STREAM_STATUS.exists():
        return {}
    try:
        return json.loads(STREAM_STATUS.read_text(encoding="utf-8"))
    except Exception:
        return {}


def pipeline_idle_stage(
    status: dict[str, object] | None = None,
) -> str:
    """Describe an idle worker without clobbering active batch state."""
    snapshot = status if status is not None else read_status()
    return "listening" if bool(snapshot.get("connected")) else "reconnecting"


def adaptive_screening_profile(
    pending: int,
    *,
    base_batch: int,
    base_workers: int,
    base_timeout: float,
) -> dict[str, object]:
    """Fast first-pass screening with bounded Mac-friendly concurrency."""
    # A homepage fetch can open both apex and www connections. Keeping the
    # domain-level worker count modest prevents connector starvation from
    # turning an entire batch into identical TimeoutError results.
    safe_workers = min(4, max(1, base_workers))
    if pending >= 1000:
        # Confirmed live twice: (1) raising the ceiling alone did nothing --
        # min(ceiling, max(floor, base_batch)) just returns base_batch
        # whenever the user's own configured "Domains per batch" is
        # already above a low floor; (2) once the floor landed (confirmed
        # via a real batch=250, workers=40 in the wild), that batch still
        # blew the 240s ceiling at 277.7s -- 250/40 = 6.25 domains per
        # worker meant most of the batch had to queue behind an earlier
        # round instead of running concurrently. Batch size now equals
        # worker count: every domain in a batch gets its own concurrent
        # scraper slot for the whole batch, so total time is bounded by one
        # round (the slowest single domain), not several queued rounds.
        # 100 simultaneous sites plus a 128-thread scoring pool drove the
        # production worker to 6.9 GB RSS and made 97% of requests time out.
        # A smaller one-round batch keeps enough throughput for the live .ai
        # arrival rate without paying for allocator/thread churn all month.
        # All-extension CertStream mode has to drain faster than certificates
        # arrive. Use the configured production ceiling in one wave and keep
        # the first-pass deadline deliberately short; inaccessible domains
        # are durable and retried later, so they must not monopolize the hot
        # acquisition path.
        surge_workers = 40
        return {
            "mode": "surge",
            "batch_size": surge_workers,
            "workers": surge_workers,
            "timeout": max(4.0, min(base_timeout, 8.0)),
            "homepage_only": True,
        }
    if pending >= 250:
        catch_up_workers = min(32, max(16, base_workers))
        return {
            "mode": "catch_up",
            "batch_size": catch_up_workers,
            "workers": catch_up_workers,
            "timeout": max(5.0, min(base_timeout, 10.0)),
            "homepage_only": True,
        }
    # batch_size used to be a fixed 10 against safe_workers's cap of 4 --
    # the exact same mismatch fixed for surge/catch_up below, just left
    # unfixed here: 10/4 meant ~3 sequential rounds per batch (up to 75s
    # at this mode's 25s ceiling) before the last domain in even the
    # *first* batch after a reset got attempted. Matching batch size to
    # worker count means one round, not several queued behind each other.
    return {
        "mode": "normal",
        "batch_size": safe_workers,
        "workers": safe_workers,
        "timeout": max(15.0, min(base_timeout, 25.0)),
        "homepage_only": True,
    }


def release_batch_memory() -> float:
    """Return unused Python/native arenas after a scrape batch.

    CPython's allocator and glibc otherwise keep BeautifulSoup, pandas, and
    thread-worker high-water memory mapped indefinitely in this always-on
    process. Railway bills that retained RSS even when the next batch is idle.
    """
    gc.collect()
    if sys.platform.startswith("linux"):
        try:
            import ctypes

            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except (AttributeError, OSError):
            pass
        try:
            resident_pages = int(Path("/proc/self/statm").read_text().split()[1])
            return round(resident_pages * os.sysconf("SC_PAGE_SIZE") / 1024**2, 1)
        except (OSError, ValueError, IndexError):
            pass
    return 0.0


def should_recycle_worker(rss_mb: float) -> bool:
    threshold_mb = max(
        512.0,
        float(os.getenv("GLASSWING_PIPELINE_RECYCLE_RSS_MB", "1200")),
    )
    return rss_mb > threshold_mb


def live_rate_metrics(
    *,
    started_monotonic: float,
    received: int,
    processed: int,
    previous_received: int,
    previous_processed: int,
    previous_time: float,
    now: float,
) -> dict[str, float]:
    elapsed_minutes = max((now - started_monotonic) / 60.0, 1 / 60)
    interval_minutes = max((now - previous_time) / 60.0, 1 / 60)

    return {
        "received_per_minute": round(
            (received - previous_received) / interval_minutes,
            2,
        ),
        "processed_per_minute": round(
            (processed - previous_processed) / interval_minutes,
            2,
        ),
        "average_received_per_minute": round(
            received / elapsed_minutes,
            2,
        ),
        "average_processed_per_minute": round(
            processed / elapsed_minutes,
            2,
        ),
        "queue_growth_per_minute": round(
            (
                (received - previous_received)
                - (processed - previous_processed)
            ) / interval_minutes,
            2,
        ),
    }


async def worker_loop(
    conn: sqlite3.Connection,
    stop: asyncio.Event,
) -> None:
    config = load_run_config()
    active_extensions = selected_extensions()
    worker_status = read_status()
    active_run_id = str(worker_status.get("run_id", "")).strip()
    active_run_started_at = str(
        worker_status.get("run_started_at", "")
    ).strip()
    base_batch_size = max(
        1,
        min(
            500,
            int(
                config.get(
                    "screening_batch_size",
                    os.getenv("STREAM_BATCH_SIZE", "200"),
                )
            ),
        ),
    )
    base_workers = max(
        1,
        min(
            40,
            int(
                config.get(
                    "scraper_workers",
                    os.getenv("STREAM_SCRAPER_WORKERS", "50"),
                )
            ),
        ),
    )
    base_timeout = max(
        4.0,
        min(
            45.0,
            float(
                config.get(
                    "scrape_timeout",
                    os.getenv("STREAM_SCRAPE_TIMEOUT", "6"),
                )
            ),
        ),
    )
    poll_seconds = float(
        os.getenv("STREAM_QUEUE_POLL_SECONDS", "0.25")
    )
    max_attempts = int(
        os.getenv("STREAM_MAX_ATTEMPTS", "3")
    )

    started_monotonic = time.monotonic()
    previous_rate_time = started_monotonic
    previous_received = 0
    previous_processed = 0
    total_batch_seconds = 0.0
    completed_batches = 0
    total_fetch_failures = 0
    total_rows_screened = 0

    while not stop.is_set():
        if RUNTIME_MODE != "local" and not read_control()["desired"].get(
            "pipeline", True
        ):
            # Cloud pause: leave the CertStream listener connected (so no
            # events are missed) and just stop claiming new batches. Cheap
            # enough to check every tick since read_control() is a single
            # indexed Postgres lookup.
            write_status(stage="paused", updated_at=utc_now())
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
            continue
        counts_before = queue_counts(conn)
        counts_by_extension = extension_counts(conn)
        pending = sum(
            int(counts_by_extension.get(extension, {}).get("pending", 0) or 0)
            for extension in active_extensions
        )
        profile = adaptive_screening_profile(
            pending,
            base_batch=base_batch_size,
            base_workers=base_workers,
            base_timeout=base_timeout,
        )

        # Allow a very short accumulation window so the filter processes
        # several new domains concurrently instead of rewriting every CSV
        # for one domain at a time. This adds under one second of latency.
        flush_seconds = float(
            os.getenv("STREAM_BATCH_FLUSH_SECONDS", "0.75")
        )
        if 0 < pending < min(20, int(profile["batch_size"])):
            await asyncio.sleep(max(0.0, min(flush_seconds, 2.0)))

        domains = claim_batch(
            conn,
            int(profile["batch_size"]),
            allowed_extensions=active_extensions,
        )
        if not domains:
            status = read_status()
            now = time.monotonic()
            received = int(
                status.get("domains_received", 0) or 0
            )
            processed = int(status.get("processed", 0) or 0)
            rates = live_rate_metrics(
                started_monotonic=started_monotonic,
                received=received,
                processed=processed,
                previous_received=previous_received,
                previous_processed=previous_processed,
                previous_time=previous_rate_time,
                now=now,
            )
            write_status(
                queue=counts_before,
                queue_by_extension=extension_counts(conn),
                stage=pipeline_idle_stage(),
                backlog_mode=profile["mode"],
                **rates,
                updated_at=utc_now(),
            )
            previous_received = received
            previous_processed = processed
            previous_rate_time = now
            await asyncio.sleep(poll_seconds)
            continue

        batch_path = write_batch_csv(domains)
        batch_started = time.monotonic()
        records = None
        output = None
        screening_stats = None
        try:
            write_status(
                stage="screening",
                current_batch=domains,
                backlog_mode=profile["mode"],
                active_batch_size=int(profile["batch_size"]),
                active_scraper_workers=int(profile["workers"]),
                active_timeout_seconds=float(profile["timeout"]),
                homepage_only=bool(profile["homepage_only"]),
                updated_at=utc_now(),
            )

            records = [
                {
                    "observed_at": utc_now(),
                    "registrable": domain,
                    "hostname": domain,
                    "run_id": active_run_id,
                    "run_started_at": active_run_started_at,
                }
                for domain in domains
            ]
            # A hang inside screen_records (confirmed live: worker stuck
            # reporting stage="screening" indefinitely, Classified/Failed
            # both frozen at 0, no exception ever raised) used to block this
            # entire worker forever -- the per-domain fetch timeout bounds
            # one HTTP request, not the batch as a whole, so anything that
            # blocks after that stage (a downstream write, a stuck lock, an
            # await that never resolves) had no ceiling at all. Confirmed
            # live: once claim_batch started prioritizing fresh, live domains
            # and scoring got its own dedicated thread pool (removing
            # queueing as a variable), a batch finally completed with real
            # results -- but a following batch of different domains still
            # occasionally hit 180s with zero individual operation ever
            # flagged as slow, consistent with real-world variance in how
            # long different websites take to load rather than a remaining
            # bug. 240s gives a batch containing a few slow-loading real
            # sites realistic room, while still failing fast on a genuine
            # hang (still 2.5x faster than the original 600s).
            outer_deadline_seconds = 240.0
            batch_started_wall = time.monotonic()
            try:
                output, screening_stats = await asyncio.wait_for(
                    screening_filter.screen_records(
                        records,
                        workers=int(profile["workers"]),
                        timeout=float(profile["timeout"]),
                        homepage_only=bool(profile["homepage_only"]),
                        # Backlog timeouts already return to the durable
                        # queue with exponential backoff. Retrying them again
                        # inside the same hot batch duplicated network/CPU
                        # work and cut throughput below the live arrival rate.
                        retry_timeouts=profile["mode"] == "normal",
                        persist=True,
                    ),
                    timeout=outer_deadline_seconds,
                )
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(
                    f"outer_deadline_{outer_deadline_seconds:.0f}s_exceeded "
                    f"after {time.monotonic() - batch_started_wall:.1f}s "
                    f"(batch={len(domains)}, workers={profile['workers']}, "
                    f"per_domain_timeout={profile['timeout']})"
                ) from None
            # Confirmed live: screen_records used to discard an entire
            # batch outright once 80%+ timed out, throwing away the real,
            # successfully-fetched companies in it too. It now persists
            # whatever succeeded and reports which domains timed out
            # instead -- mark only those for retry, not the whole batch.
            timed_out_domains = set(
                screening_stats.get("timed_out_domains", [])
            )
            succeeded_domains = [
                domain for domain in domains if domain not in timed_out_domains
            ]
            if succeeded_domains:
                mark_success(conn, succeeded_domains)
            if timed_out_domains:
                mark_failure(
                    conn,
                    [domain for domain in domains if domain in timed_out_domains],
                    "TimeoutError: fetch timed out",
                    max_attempts,
                )

            batch_seconds = time.monotonic() - batch_started
            total_batch_seconds += batch_seconds
            completed_batches += 1
            total_rows_screened += len(output)

            if not output.empty:
                fetch_errors = output.get(
                    "fetch_error",
                    "",
                ).astype(str).str.strip()
                statuses = output.get(
                    "http_status",
                    "",
                ).astype(str)
                total_fetch_failures += int(
                    (
                        fetch_errors.ne("")
                        | statuses.str.match(r"^[45]")
                    ).sum()
                )

            status = read_status()
            processed = (
                int(status.get("processed", 0) or 0)
                + len(domains)
            )

            successfully_screened = 0
            if not output.empty and "routing_bucket" in output.columns:
                countable_buckets = {
                    "priority",
                    "coming_soon",
                    "reject",
                    "review",
                    "screened_out",
                    "comp",
                }
                successfully_screened = int(
                    output["routing_bucket"]
                    .astype(str)
                    .isin(countable_buckets)
                    .sum()
                )

            screened = (
                int(status.get("screened", 0) or 0)
                + successfully_screened
            )

            received = int(
                status.get("domains_received", 0) or 0
            )
            now = time.monotonic()
            rates = live_rate_metrics(
                started_monotonic=started_monotonic,
                received=received,
                processed=processed,
                previous_received=previous_received,
                previous_processed=previous_processed,
                previous_time=previous_rate_time,
                now=now,
            )

            write_status(
                processed=processed,
                screened=screened,
                last_domain_processed=domains[-1],
                current_batch=[],
                queue=queue_counts(conn),
                queue_by_extension=extension_counts(conn),
                stage=pipeline_idle_stage(),
                last_error="",
                backlog_mode=profile["mode"],
                last_batch_seconds=round(batch_seconds, 2),
                average_batch_seconds=round(
                    total_batch_seconds / completed_batches,
                    2,
                ),
                average_seconds_per_domain=round(
                    total_batch_seconds
                    / max(total_rows_screened, 1),
                    3,
                ),
                fetch_failure_rate=round(
                    total_fetch_failures
                    / max(total_rows_screened, 1),
                    4,
                ),
                last_batch_results=screening_stats,
                **rates,
                updated_at=utc_now(),
            )
            previous_received = received
            previous_processed = processed
            previous_rate_time = now

            claude_calls = await asyncio.to_thread(
                maybe_run_claude,
                batch_path,
            )
            status = read_status()
            automatic_total = int(
                status.get("automatic_claude_calls", 0) or 0
            ) + claude_calls
            manual_total = int(
                status.get("manual_claude_calls", 0) or 0
            )
            write_status(
                automatic_claude_calls=automatic_total,
                manual_claude_calls=manual_total,
                claude_calls=automatic_total + manual_total,
                queue=queue_counts(conn),
                queue_by_extension=extension_counts(conn),
                stage=pipeline_idle_stage(),
                updated_at=utc_now(),
            )
        except Exception as exc:
            mark_failure(
                conn,
                domains,
                f"{type(exc).__name__}: {exc}",
                max_attempts,
            )
            failure_status = read_status()
            failed_this_run = (
                int(failure_status.get("failed_this_run", 0) or 0)
                + len(domains)
            )
            write_status(
                failed_this_run=failed_this_run,
                current_batch=[],
                stage=pipeline_idle_stage(),
                last_error=(
                    f"{type(exc).__name__}: {exc}"
                )[:500],
                queue=queue_counts(conn),
                queue_by_extension=extension_counts(conn),
                updated_at=utc_now(),
            )
        finally:
            batch_path.unlink(missing_ok=True)
            records = None
            output = None
            screening_stats = None
            worker_rss_mb = release_batch_memory()
            write_status(
                worker_rss_mb=worker_rss_mb,
                updated_at=utc_now(),
            )
            if should_recycle_worker(worker_rss_mb):
                write_status(
                    stage="restarting_to_release_memory",
                    memory_recycle_at_mb=worker_rss_mb,
                    updated_at=utc_now(),
                )
                os.execv(sys.executable, [sys.executable, *sys.argv])


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one queued batch and exit (testing only).",
    )
    args = parser.parse_args()

    # A PID recorded here can outlive the process that wrote it: now that
    # this container's storage is a persistent volume (it used to be wiped
    # every restart), the file survives a restart, and the OS can reuse the
    # same PID number for the new legitimate process -- making
    # os.kill(existing, 0) succeed spuriously and refuse to start. The
    # fcntl instance lock below is the real single-instance guarantee (the
    # kernel releases it the instant a process exits, unlike this file), so
    # this file alone is never grounds to refuse to start.
    STREAM_PID.unlink(missing_ok=True)

    STREAM_STOP.unlink(missing_ok=True)
    STREAM_PID.write_text(str(os.getpid()), encoding="utf-8")
    conn = connect_db()
    cleared_stale_queue = clear_stale_work_queue(conn)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    active_config = load_run_config()
    active_extensions = selected_extensions()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_started_at = utc_now()
    write_status(
        running=True,
        pid=os.getpid(),
        stage="starting",
        run_id=run_id,
        run_started_at=run_started_at,
        selected_extensions=sorted(active_extensions),
        received_by_extension={
            extension: 0
            for extension in sorted(active_extensions)
        },
        repeats_by_extension={
            extension: 0
            for extension in sorted(active_extensions)
        },
        queue_by_extension=extension_counts(conn),
        screening_batch_size=int(
            active_config.get("screening_batch_size", 200)
        ),
        scraper_workers=int(
            active_config.get("scraper_workers", 100)
        ),
        connected=False,
        started_at=utc_now(),
        updated_at=utc_now(),
        certificate_events_received=0,
        new_domains_received=0,
        repeat_events_ignored=0,
        domains_received=0,
        processed=0,
        screened=0,
        failed_this_run=0,
        cleared_stale_queue=cleared_stale_queue,
        claude_calls=0,
        automatic_claude_calls=0,
        manual_claude_calls=0,
        last_error="",
        queue=queue_counts(conn),
    )

    if (
        using_classification_postgres()
        and not SHARED_CLASSIFICATION_MIGRATION_MARKER.exists()
    ):
        write_status(
            stage="syncing_saved_results",
            updated_at=utc_now(),
        )
        try:
            imported = await asyncio.to_thread(
                import_private_sqlite_into_shared
            )
        except Exception as exc:
            write_status(
                classification_sync_error=(
                    f"{type(exc).__name__}: {exc}"
                )[:800],
                updated_at=utc_now(),
            )
        else:
            atomic_write_json(
                SHARED_CLASSIFICATION_MIGRATION_MARKER,
                {
                    "imported": int(imported),
                    "completed_at": utc_now(),
                },
            )
            write_status(
                migrated_private_classifications=int(imported),
                classification_sync_error="",
                stage="starting",
                updated_at=utc_now(),
            )

    async def stop_file_watch() -> None:
        while not stop.is_set():
            if STREAM_STOP.exists():
                stop.set()
                break
            await asyncio.sleep(0.5)

    listener_task = asyncio.create_task(listener(conn, stop))
    worker_task = asyncio.create_task(worker_loop(conn, stop))
    watcher_task = asyncio.create_task(stop_file_watch())

    def stop_on_unexpected_task_exit(task: asyncio.Task) -> None:
        if task.cancelled() or stop.is_set():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is None:
            detail = "Background task exited unexpectedly."
        else:
            detail = f"{type(error).__name__}: {error}"
        write_status(
            running=False,
            stage="crashed",
            last_error=detail[:800],
            updated_at=utc_now(),
        )
        stop.set()

    listener_task.add_done_callback(stop_on_unexpected_task_exit)
    worker_task.add_done_callback(stop_on_unexpected_task_exit)

    if args.once:
        await asyncio.sleep(5)
        stop.set()

    await stop.wait()
    write_status(
        running=True,
        stage="stopping",
        connected=False,
        updated_at=utc_now(),
    )

    listener_task.cancel()
    await asyncio.gather(listener_task, return_exceptions=True)

    # Let the worker finish its current batch before exit.
    try:
        await asyncio.wait_for(worker_task, timeout=180)
    except asyncio.TimeoutError:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

    watcher_task.cancel()
    await asyncio.gather(watcher_task, return_exceptions=True)

    write_status(
        running=False,
        stage="stopped",
        connected=False,
        stopped_at=utc_now(),
        updated_at=utc_now(),
        queue=queue_counts(conn),
    )
    conn.close()
    STREAM_PID.unlink(missing_ok=True)
    STREAM_STOP.unlink(missing_ok=True)

def main() -> None:
    instance_lock_path = LOGS / "continuous_pipeline.instance.lock"
    instance_lock_path.parent.mkdir(parents=True, exist_ok=True)
    instance_lock = instance_lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(
                instance_lock.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        except BlockingIOError:
            # Another healthy process owns the queue. Exit quietly without
            # overwriting its status or PID files.
            return
        asyncio.run(async_main())
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        LOGS.mkdir(parents=True, exist_ok=True)
        with RUN_LOG.open("a", encoding="utf-8") as log:
            log.write(
                f"\n[{utc_now()}] PIPELINE CRASH\n"
                f"{traceback.format_exc()}\n"
            )
        write_status(
            running=False,
            stage="crashed",
            connected=False,
            last_error=detail[:800],
            updated_at=utc_now(),
        )
        raise
    finally:
        try:
            fcntl.flock(instance_lock.fileno(), fcntl.LOCK_UN)
        finally:
            instance_lock.close()

if __name__ == "__main__":
    main()
