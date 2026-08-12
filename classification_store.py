"""Canonical SQLite store for screening results and human decisions.

The historical CSV files remain compatibility exports, but live application
reads and writes use this database so a one-row action never requires loading
and rewriting the complete screening corpus.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from psycopg import Error as PostgresError

from common import DATA, FILTERED, atomic_write_csv, clean, stable_domain
from db_backend import (
    PostgresConnection,
    acquire_advisory_lock_with_timeout,
    begin_immediate,
    connect_postgres,
    table_columns_postgres,
    using_postgres,
)


CLASSIFICATION_DB = Path(
    os.getenv(
        "GLASSWING_CLASSIFICATION_DB",
        str(DATA / "classifications.sqlite3"),
    )
).expanduser().resolve()
MODEL_VERSION = "glasswing-local-v5-refined-buckets"
CLASSIFICATION_DATABASE_ENV_VARS = (
    "CLASSIFICATION_DATABASE_URL",
    "DATABASE_URL",
    "PIPELINE_DATABASE_URL",
)
_POSTGRES_MIGRATION_LOCK = threading.Lock()
_MIGRATED_POSTGRES_TARGETS: set[str] = set()

CORE_COLUMNS = (
    "domain",
    "company_name",
    "final_url",
    "description",
    "excerpt",
    "availability_status",
    "crawl_status",
    "content_source",
    "http_status",
    "fetch_error",
    "routing_bucket",
    "routing_reason",
    "fit_score",
    "confidence_score",
    "priority_score",
    "thesis_pillar",
    "evidence",
    "word_count",
    "manual_override",
    "manual_override_target",
    "record_json",
    "classified_at",
    "updated_at",
    "model_version",
)


def classification_database_env_var() -> str:
    """Return the configured shared classification database variable.

    Railway's web service historically receives ``DATABASE_URL`` while the
    standalone CertStream worker receives ``PIPELINE_DATABASE_URL``. They
    point at the same Postgres service, but checking only the first name made
    the worker silently fall back to its private SQLite volume.
    """
    for env_var in CLASSIFICATION_DATABASE_ENV_VARS:
        if using_postgres(env_var):
            return env_var
    return ""


def using_classification_postgres() -> bool:
    return bool(classification_database_env_var())


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def connect_store(
    path: Path | str = CLASSIFICATION_DB,
    *,
    read_only: bool = False,
) -> sqlite3.Connection | PostgresConnection:
    database_env_var = classification_database_env_var()
    if database_env_var:
        conn = connect_postgres(
            read_only=read_only,
            env_var=database_env_var,
        )
        # Pin the schema. This store owns "public", but when DATABASE_URL is
        # absent it falls back to PIPELINE_DATABASE_URL and therefore shares
        # continuous_pipeline's connection pool -- whose connections carry a
        # session-level `SET search_path TO pipeline, public`. Unqualified table
        # names then resolved to the pipeline schema, so migrate() created a
        # duplicate set of tables there and every write landed in
        # pipeline.classifications while the dashboard read public.classifications.
        # Reads came from the separate read-only pool, which kept the default
        # search_path, so the two diverged in total silence: writes reported
        # success, reads returned correct-looking counts, and 11,687
        # classifications plus 36,950 exclusions accumulated invisibly in the
        # wrong schema. That is what "Candidates retained: 0" actually was.
        conn.execute("SET search_path TO public")
        if not read_only:
            conn.commit()
        if not read_only and database_env_var not in _MIGRATED_POSTGRES_TARGETS:
            # This connection string is now shared by the web service and
            # the ai-worker/zone-worker cloud services too -- multiple
            # services independently connecting at once would otherwise
            # race to run this same schema DDL and deadlock on conflicting
            # locks (confirmed live for ai_search's identical pattern).
            with _POSTGRES_MIGRATION_LOCK:
                if database_env_var not in _MIGRATED_POSTGRES_TARGETS:
                    locked = acquire_advisory_lock_with_timeout(
                        conn,
                        "classification_store_migration_v2",
                        transactional=True,
                    )
                    # PostgreSQL DDL still takes catalog locks even when
                    # guarded by IF NOT EXISTS. Run it once per web/worker
                    # process instead of once for every write request.
                    if locked:
                        migrate(conn)
                    _MIGRATED_POSTGRES_TARGETS.add(database_env_var)
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
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        migrate(conn)
    else:
        conn.execute("PRAGMA query_only=ON")
    return conn


def migrate(conn: sqlite3.Connection | PostgresConnection) -> None:
    postgres = isinstance(conn, PostgresConnection)
    movement_id_column = (
        "id SERIAL PRIMARY KEY" if postgres else "id INTEGER PRIMARY KEY AUTOINCREMENT"
    )
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS classifications (
            domain TEXT PRIMARY KEY,
            company_name TEXT NOT NULL DEFAULT '',
            final_url TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            excerpt TEXT NOT NULL DEFAULT '',
            availability_status TEXT NOT NULL DEFAULT 'unknown',
            crawl_status TEXT NOT NULL DEFAULT '',
            content_source TEXT NOT NULL DEFAULT '',
            http_status TEXT NOT NULL DEFAULT '',
            fetch_error TEXT NOT NULL DEFAULT '',
            routing_bucket TEXT NOT NULL DEFAULT 'insufficient',
            routing_reason TEXT NOT NULL DEFAULT '',
            fit_score INTEGER NOT NULL DEFAULT 0,
            confidence_score INTEGER NOT NULL DEFAULT 0,
            priority_score INTEGER NOT NULL DEFAULT 0,
            thesis_pillar TEXT NOT NULL DEFAULT '',
            evidence TEXT NOT NULL DEFAULT '',
            word_count INTEGER NOT NULL DEFAULT 0,
            manual_override INTEGER NOT NULL DEFAULT 0,
            manual_override_target TEXT NOT NULL DEFAULT '',
            record_json TEXT NOT NULL DEFAULT '{{}}',
            classified_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            model_version TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_classifications_bucket_priority
            ON classifications(routing_bucket, priority_score DESC, domain);
        CREATE INDEX IF NOT EXISTS idx_classifications_availability
            ON classifications(availability_status, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_classifications_updated
            ON classifications(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_classifications_bucket_updated
            ON classifications(routing_bucket, updated_at DESC, domain);

        CREATE TABLE IF NOT EXISTS manual_decisions (
            domain TEXT PRIMARY KEY,
            target_label TEXT NOT NULL,
            target_bucket TEXT NOT NULL,
            objective_score INTEGER NOT NULL DEFAULT 0,
            note TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'human',
            decided_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_manual_decisions_bucket
            ON manual_decisions(target_bucket, decided_at DESC);

        CREATE TABLE IF NOT EXISTS movement_log (
            {movement_id_column},
            domain TEXT NOT NULL,
            from_bucket TEXT NOT NULL DEFAULT '',
            to_bucket TEXT NOT NULL,
            objective_score INTEGER NOT NULL DEFAULT 0,
            note TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'human',
            moved_at TEXT NOT NULL,
            predicted_bucket TEXT NOT NULL DEFAULT '',
            model_version TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_movement_log_domain
            ON movement_log(domain, moved_at DESC);

        CREATE TABLE IF NOT EXISTS store_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hidden_domains (
            domain TEXT PRIMARY KEY,
            hidden_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tracked_domains (
            domain TEXT PRIMARY KEY,
            tracked_at TEXT NOT NULL,
            tracking_status TEXT NOT NULL DEFAULT 'tracking',
            reached_out_at TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_tracked_domains_recent
            ON tracked_domains(tracked_at DESC, domain);

        CREATE TABLE IF NOT EXISTS automated_exclusions (
            domain TEXT PRIMARY KEY,
            bucket TEXT NOT NULL DEFAULT '',
            reason TEXT NOT NULL DEFAULT '',
            glasswing_rating INTEGER NOT NULL DEFAULT 0,
            content_source TEXT NOT NULL DEFAULT '',
            excluded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_automated_exclusions_reason
            ON automated_exclusions(reason, excluded_at DESC);

        CREATE TABLE IF NOT EXISTS training_labels (
            domain TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            rating INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'human',
            reason TEXT NOT NULL DEFAULT '',
            labeled_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_training_labels_source
            ON training_labels(source, labeled_at DESC);

        CREATE TABLE IF NOT EXISTS classification_run_membership (
            domain TEXT NOT NULL,
            run_id TEXT NOT NULL,
            first_seen_at TEXT NOT NULL DEFAULT '',
            last_seen_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(domain, run_id)
        );
        CREATE INDEX IF NOT EXISTS idx_classification_run_membership_run
            ON classification_run_membership(run_id, domain);

        CREATE TABLE IF NOT EXISTS daily_digest_runs (
            run_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL DEFAULT '',
            candidate_count INTEGER NOT NULL DEFAULT 0,
            sent_count INTEGER NOT NULL DEFAULT 0,
            recipient_count INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_daily_digest_runs_started
            ON daily_digest_runs(started_at DESC);

        CREATE TABLE IF NOT EXISTS daily_digest_items (
            recipient TEXT NOT NULL,
            domain TEXT NOT NULL,
            run_id TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            PRIMARY KEY (recipient, domain)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_digest_items_sent
            ON daily_digest_items(sent_at DESC);

        CREATE TABLE IF NOT EXISTS claude_diligence (
            domain TEXT PRIMARY KEY,
            decision TEXT NOT NULL DEFAULT '',
            fit_score INTEGER NOT NULL DEFAULT 0,
            category TEXT NOT NULL DEFAULT '',
            us_based TEXT NOT NULL DEFAULT '',
            product_summary TEXT NOT NULL DEFAULT '',
            reasoning TEXT NOT NULL DEFAULT '',
            concerns TEXT NOT NULL DEFAULT '',
            diligence_sources TEXT NOT NULL DEFAULT '',
            diligence_version TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            result_json TEXT NOT NULL DEFAULT '{{}}',
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_claude_diligence_decision_score
            ON claude_diligence(decision, fit_score DESC, updated_at DESC);

        CREATE TABLE IF NOT EXISTS claude_review_queue (
            domain TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'queued',
            requested_at TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT '',
            completed_at TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            override_budget BOOLEAN NOT NULL DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_claude_review_queue_status
            ON claude_review_queue(status, requested_at);

        CREATE TABLE IF NOT EXISTS competitive_landscape_jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'queued',
            website TEXT NOT NULL,
            template_name TEXT NOT NULL,
            materials_json TEXT NOT NULL DEFAULT '[]',
            notes TEXT NOT NULL DEFAULT '',
            requested_count INTEGER NOT NULL DEFAULT 25,
            company_name TEXT NOT NULL DEFAULT '',
            competitor_count INTEGER NOT NULL DEFAULT 0,
            output_path TEXT NOT NULL DEFAULT '',
            result_json TEXT NOT NULL DEFAULT '{{}}',
            error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT '',
            completed_at TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_competitive_jobs_status
            ON competitive_landscape_jobs(status, created_at);

        CREATE TABLE IF NOT EXISTS admin_audit_log (
            {movement_id_column},
            action TEXT NOT NULL,
            target TEXT NOT NULL DEFAULT '',
            detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_admin_audit_created
            ON admin_audit_log(created_at DESC);
        """
    )
    if postgres:
        movement_columns = table_columns_postgres(conn, "movement_log")
    else:
        movement_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(movement_log)")
        }
    if "predicted_bucket" not in movement_columns:
        conn.execute(
            "ALTER TABLE movement_log ADD COLUMN "
            "predicted_bucket TEXT NOT NULL DEFAULT ''"
        )
    if "model_version" not in movement_columns:
        conn.execute(
            "ALTER TABLE movement_log ADD COLUMN "
            "model_version TEXT NOT NULL DEFAULT ''"
        )
    queue_columns = (
        table_columns_postgres(conn, "claude_review_queue")
        if postgres else {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(claude_review_queue)")
        }
    )
    if "override_budget" not in queue_columns:
        conn.execute(
            "ALTER TABLE claude_review_queue ADD COLUMN "
            "override_budget BOOLEAN NOT NULL DEFAULT FALSE"
        )
    tracked_columns = (
        table_columns_postgres(conn, "tracked_domains")
        if postgres else {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(tracked_domains)")
        }
    )
    if "tracking_status" not in tracked_columns:
        conn.execute(
            "ALTER TABLE tracked_domains ADD COLUMN "
            "tracking_status TEXT NOT NULL DEFAULT 'tracking'"
        )
    if "reached_out_at" not in tracked_columns:
        conn.execute(
            "ALTER TABLE tracked_domains ADD COLUMN "
            "reached_out_at TEXT NOT NULL DEFAULT ''"
        )
    membership_count = int(conn.execute(
        "SELECT count(*) AS total FROM classification_run_membership"
    ).fetchone()["total"])
    if membership_count == 0:
        for row in conn.execute(
            "SELECT domain,record_json,classified_at,updated_at FROM classifications"
        ).fetchall():
            try:
                stored = json.loads(str(row["record_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                stored = {}
            run_id = clean(stored.get("run_id", ""))
            if run_id:
                conn.execute(
                    "INSERT INTO classification_run_membership(domain,run_id,first_seen_at,last_seen_at) "
                    "VALUES (?,?,?,?) ON CONFLICT(domain,run_id) DO NOTHING",
                    (row["domain"], run_id, row["classified_at"], row["updated_at"]),
                )
    conn.commit()


def _plain(value: object) -> object:
    if value is None:
        return ""
    try:
        missing = pd.isna(value)
        if isinstance(missing, bool) and missing:
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    return str(value)


def _integer(value: object, default: int = 0) -> int:
    try:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return default
        return int(float(numeric))
    except (TypeError, ValueError):
        return default


def availability_for_record(record: dict[str, object]) -> str:
    """Return technical availability without making an investment judgment."""
    crawl_status = clean(record.get("crawl_status", "")).lower()
    fetch_error = clean(record.get("fetch_error", "")).lower()
    status = clean(record.get("http_status", ""))
    word_count = _integer(record.get("word_count", 0))
    content = clean(
        " ".join(
            [
                str(record.get("page_title", "") or ""),
                str(record.get("meta_description", "") or ""),
                str(record.get("homepage_excerpt", "") or ""),
            ]
        )
    )

    if word_count >= 3 or len(content.split()) >= 3:
        if fetch_error or crawl_status not in {"", "success", "live"}:
            return "cached_content"
        return "live"
    combined_error = f"{crawl_status} {fetch_error}".lower()
    if "dns" in combined_error or "name or service" in combined_error:
        return "dns_error"
    if "timeout" in combined_error:
        return "timeout"
    if (
        "blocked" in combined_error
        or status in {"401", "403", "429"}
    ):
        return "blocked"
    if status.startswith(("4", "5")) or "http_error" in combined_error:
        return "http_error"
    if "non_html" in combined_error:
        return "non_html"
    if fetch_error or crawl_status not in {"", "pending", "success", "live"}:
        return "unreachable"
    return "empty"


def confidence_for_record(
    record: dict[str, object],
    availability: str | None = None,
) -> int:
    """Score evidence completeness separately from Glasswing fit."""
    availability = availability or availability_for_record(record)
    word_count = _integer(record.get("word_count", 0))
    pages = _integer(record.get("pages_scraped", 0))
    score = {
        "live": 45,
        "cached_content": 35,
        "blocked": 10,
        "http_error": 8,
        "timeout": 5,
        "dns_error": 0,
        "non_html": 5,
        "unreachable": 3,
        "empty": 2,
    }.get(availability, 0)
    if clean(record.get("page_title", "")):
        score += 8
    if clean(record.get("meta_description", "")):
        score += 12
    if word_count >= 25:
        score += 10
    if word_count >= 100:
        score += 10
    if word_count >= 300:
        score += 5
    if pages >= 2:
        score += 5
    if clean(record.get("rule_evidence", "")):
        score += 5
    return max(0, min(score, 100))


def priority_for_record(
    record: dict[str, object],
    *,
    fit_score: int,
    confidence_score: int,
) -> int:
    """Rank review value while preventing sparse sites from looking certain."""
    bucket = clean(record.get("routing_bucket", "")).lower()
    if bucket == "reject":
        return 0
    if bucket == "coming_soon":
        return max(20, min(70, fit_score))
    confidence_multiplier = 0.55 + (0.45 * confidence_score / 100)
    priority = round(fit_score * confidence_multiplier)
    if bucket == "priority":
        priority = max(priority, 55)
    preference_ready = str(
        record.get("preference_model_ready", "")
    ).lower() in {"true", "1", "yes"}
    if preference_ready:
        try:
            probability = float(record.get("preference_yes_probability", 0.5))
            preference_confidence = float(
                record.get("preference_confidence", 0.0)
            )
        except (TypeError, ValueError):
            probability = 0.5
            preference_confidence = 0.0
        # The labeled set is still small, so the learned preference layer may
        # reorder close calls but cannot overwhelm factual website evidence.
        learned_adjustment = round(
            (probability - 0.5)
            * 16
            * max(0.0, min(preference_confidence, 1.0))
        )
        priority += learned_adjustment
    if bool(record.get("manual_override")):
        priority += 3
    return max(0, min(priority, 100))


def normalized_record(record: dict[str, object]) -> dict[str, object]:
    plain = {str(key): _plain(value) for key, value in record.items()}
    domain = stable_domain(plain.get("domain", ""))
    now = utc_now()
    availability = availability_for_record(plain)
    fit_score = max(
        0,
        min(
            100,
            _integer(
                plain.get(
                    "glasswing_rating",
                    plain.get(
                        "diligence_rank_score",
                        plain.get("rule_score", 0),
                    ),
                )
            ),
        ),
    )
    routing_reason = clean(plain.get("routing_reason", ""))
    if routing_reason in {
        "trusted_human_training_yes",
        "manual_override_investable",
        "manual_override_priority",
    }:
        fit_score = max(
            fit_score,
            max(0, min(100, _integer(plain.get("human_rating", 0)))),
        )
    confidence_score = confidence_for_record(plain, availability)
    priority_score = priority_for_record(
        plain,
        fit_score=fit_score,
        confidence_score=confidence_score,
    )
    raw_json = json.dumps(plain, ensure_ascii=False, separators=(",", ":"))
    return {
        "domain": domain,
        "company_name": clean(
            plain.get("page_title", "") or domain
        ),
        "final_url": clean(
            plain.get("final_url", "") or f"https://{domain}"
        ),
        "description": clean(
            plain.get("meta_description", "")
            or plain.get("homepage_excerpt", "")
            or plain.get("excerpt", "")
        )[:1000],
        "excerpt": clean(plain.get("homepage_excerpt", ""))[:12000],
        "availability_status": availability,
        "crawl_status": clean(plain.get("crawl_status", "")),
        "content_source": clean(
            plain.get("content_source", "network_fetch")
        ),
        "http_status": clean(plain.get("http_status", "")),
        "fetch_error": clean(plain.get("fetch_error", "")),
        "routing_bucket": clean(
            plain.get("routing_bucket", "insufficient")
        ),
        "routing_reason": routing_reason,
        "fit_score": fit_score,
        "confidence_score": confidence_score,
        "priority_score": priority_score,
        "thesis_pillar": clean(plain.get("rule_thesis_pillar", "")),
        "evidence": clean(plain.get("rule_evidence", "")),
        "word_count": _integer(plain.get("word_count", 0)),
        "manual_override": int(
            str(plain.get("manual_override", "")).lower()
            in {"true", "1", "yes"}
        ),
        "manual_override_target": clean(
            plain.get("manual_override_target", "")
        ),
        "record_json": raw_json,
        "classified_at": clean(
            plain.get("screened_at", "")
            or plain.get("observed_at", "")
            or now
        ),
        "updated_at": now,
        "model_version": MODEL_VERSION,
    }


def upsert_classifications(
    frame: pd.DataFrame,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    if frame is None or frame.empty:
        return 0
    records = [
        normalized_record(row)
        for row in frame.to_dict(orient="records")
    ]
    records = [row for row in records if row["domain"]]
    if not records:
        return 0
    placeholders = ",".join("?" for _ in CORE_COLUMNS)
    human_truth_columns = {
        "routing_bucket",
        "routing_reason",
        "fit_score",
        "confidence_score",
        "priority_score",
        "manual_override",
        "manual_override_target",
        "record_json",
        "model_version",
    }
    updates = ",".join(
        (
            f"{column}=CASE WHEN classifications.manual_override=1 "
            f"THEN classifications.{column} ELSE excluded.{column} END"
            if column in human_truth_columns
            else f"{column}=excluded.{column}"
        )
        for column in CORE_COLUMNS
        if column not in {"domain", "classified_at"}
    )
    sql = (
        f"INSERT INTO classifications({','.join(CORE_COLUMNS)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT(domain) DO UPDATE SET {updates}"
    )
    with connect_store(path) as conn:
        begin_immediate(conn)
        conn.executemany(
            sql,
            [tuple(record[column] for column in CORE_COLUMNS) for record in records],
        )
        for record in records:
            try:
                stored_record = json.loads(str(record.get("record_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                stored_record = {}
            run_id = clean(stored_record.get("run_id", ""))
            if run_id:
                conn.execute(
                    "INSERT INTO classification_run_membership(domain,run_id,first_seen_at,last_seen_at) "
                    "VALUES (?,?,?,?) ON CONFLICT(domain,run_id) DO UPDATE SET "
                    "last_seen_at=excluded.last_seen_at",
                    (record["domain"], run_id, record["classified_at"], record["updated_at"]),
                )
        # Scraper refreshes cannot return a decisively reviewed company to an
        # incompatible bucket. Persisted Claude output owns canonical routing.
        for record in records:
            diligence = conn.execute(
                "SELECT decision FROM claude_diligence WHERE domain=?",
                (record["domain"],),
            ).fetchone()
            if diligence and diligence["decision"] in {"KEEP", "REJECT"}:
                decision = str(diligence["decision"])
                conn.execute(
                    "UPDATE classifications SET routing_bucket=?,routing_reason=? "
                    "WHERE domain=? AND manual_override=0",
                    (
                        "priority" if decision == "KEEP" else "reject",
                        "claude_keep" if decision == "KEEP" else "claude_reject",
                        record["domain"],
                    ),
                )
        conn.commit()
    return len(records)


def count_classifications(
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    if not using_classification_postgres() and not Path(path).exists():
        return 0
    with connect_store(path, read_only=True) as conn:
        return int(
            conn.execute(
                "SELECT count(*) AS total FROM classifications"
            ).fetchone()["total"]
        )


def search_classifications(
    query: str,
    *,
    limit: int = 5,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict]:
    """Simple substring match on domain/title -- not full-text search, just
    enough to surface a tracked company by name from a single search box.

    Excludes buckets that are never "a company we could invest in" (ghost,
    hard-rejected, screened out) -- this is an investment-discovery search,
    not an archival lookup across every domain ever seen. Ranked by score,
    not recency, since finding the best matches fast is the point."""
    query = query.strip()
    if not query or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return []
    needle = f"%{query.lower()}%"
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT domain, company_name, routing_bucket, final_url,
                   description, excerpt, fit_score, thesis_pillar,
                   content_source, updated_at
            FROM classifications
            WHERE (lower(domain) LIKE ? OR lower(company_name) LIKE ?)
              AND routing_bucket NOT IN ('ghost', 'reject', 'screened_out')
            ORDER BY fit_score DESC, updated_at DESC
            LIMIT ?
            """,
            (needle, needle, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_claude_diligence(
    rows: Iterable[dict[str, object]],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Persist Claude results in the shared store used by every service."""
    values = []
    now = utc_now()
    for original in rows:
        row = {str(key): _plain(value) for key, value in original.items()}
        domain = stable_domain(row.get("domain", ""))
        if not domain:
            continue
        result = {
            key.removeprefix("llm_"): value
            for key, value in row.items()
            if key.startswith("llm_")
        }
        decision = clean(result.get("decision", "")).upper()
        if decision not in {"KEEP", "REJECT"}:
            continue
        values.append((
            domain,
            decision,
            max(0, min(100, _integer(result.get("fit_score", 0)))),
            clean(result.get("category", "")),
            clean(result.get("us_based", "")),
            clean(result.get("product_summary", "")),
            clean(result.get("reasoning", "")),
            clean(result.get("concerns", "")),
            clean(result.get("diligence_sources", "")),
            clean(result.get("diligence_version", "")),
            clean(result.get("model", row.get("llm_model", ""))),
            json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            now,
        ))
    if not values:
        return 0
    with connect_store(path) as conn:
        begin_immediate(conn)
        conn.executemany(
            """
            INSERT INTO claude_diligence(
                domain,decision,fit_score,category,us_based,product_summary,
                reasoning,concerns,diligence_sources,diligence_version,model,
                result_json,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(domain) DO UPDATE SET
                decision=excluded.decision,
                fit_score=excluded.fit_score,
                category=excluded.category,
                us_based=excluded.us_based,
                product_summary=excluded.product_summary,
                reasoning=excluded.reasoning,
                concerns=excluded.concerns,
                diligence_sources=excluded.diligence_sources,
                diligence_version=excluded.diligence_version,
                model=excluded.model,
                result_json=excluded.result_json,
                updated_at=excluded.updated_at
            """,
            values,
        )
        # A decisive diligence result must move the canonical company and
        # its training label together; otherwise REJECT cards can remain in
        # Investable even though Claude has already denied them.
        for value in values:
            domain, decision, fit_score = value[0], value[1], value[2]
            bucket = "priority" if decision == "KEEP" else "reject"
            label = "yes" if decision == "KEEP" else "no"
            reason = "claude_keep" if decision == "KEEP" else "claude_reject"
            conn.execute(
                "UPDATE classifications SET routing_bucket=?,routing_reason=?,"
                "updated_at=? WHERE domain=? AND manual_override=0",
                (bucket, reason, now, domain),
            )
            conn.execute(
                """
                INSERT INTO training_labels(
                    domain,label,rating,source,reason,labeled_at
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(domain) DO UPDATE SET
                    label=excluded.label,rating=excluded.rating,
                    source=excluded.source,reason=excluded.reason,
                    labeled_at=excluded.labeled_at
                WHERE lower(training_labels.source) NOT IN (
                    'human','glasswing_portfolio'
                )
                """,
                (domain, label, fit_score, "claude", reason, now),
            )
        conn.commit()
    return len(values)


def reconcile_claude_routing(
    *, path: Path | str = CLASSIFICATION_DB
) -> int:
    """Repair canonical buckets/labels for every stored Claude decision."""
    now = utc_now()
    with connect_store(path) as conn:
        rows = conn.execute(
            "SELECT domain,decision,fit_score FROM claude_diligence "
            "WHERE decision IN ('KEEP','REJECT')"
        ).fetchall()
        begin_immediate(conn)
        for row in rows:
            domain = str(row["domain"])
            decision = str(row["decision"])
            fit_score = int(row["fit_score"] or 0)
            bucket = "priority" if decision == "KEEP" else "reject"
            label = "yes" if decision == "KEEP" else "no"
            reason = "claude_keep" if decision == "KEEP" else "claude_reject"
            conn.execute(
                "UPDATE classifications SET routing_bucket=?,routing_reason=?,"
                "updated_at=? WHERE domain=? AND manual_override=0",
                (bucket, reason, now, domain),
            )
            conn.execute(
                """
                INSERT INTO training_labels(
                    domain,label,rating,source,reason,labeled_at
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(domain) DO UPDATE SET
                    label=excluded.label,rating=excluded.rating,
                    source=excluded.source,reason=excluded.reason,
                    labeled_at=excluded.labeled_at
                WHERE lower(training_labels.source) NOT IN (
                    'human','glasswing_portfolio'
                )
                """,
                (domain, label, fit_score, "claude", reason, now),
            )
        conn.commit()
    return len(rows)


def query_claude_diligence(
    domains: Iterable[str] = (),
    *,
    diligence_version: str = "",
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Read shared Claude results using the legacy UI column names."""
    normalized = list(dict.fromkeys(
        domain for value in domains if (domain := stable_domain(value))
    ))
    if not using_classification_postgres() and not Path(path).exists():
        return []
    clauses = []
    params: list[object] = []
    if normalized:
        marks = ",".join("?" for _ in normalized)
        clauses.append(f"domain IN ({marks})")
        params.extend(normalized)
    if clean(diligence_version):
        clauses.append("diligence_version=?")
        params.append(clean(diligence_version))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT domain,decision AS llm_decision,
                   fit_score AS llm_fit_score,
                   product_summary AS llm_product_summary,
                   reasoning AS llm_reasoning,
                   concerns AS llm_concerns,result_json,
                   model AS llm_model,diligence_version,updated_at
            FROM claude_diligence
            """ + where + " ORDER BY updated_at DESC",
            params,
        ).fetchall()
    result = []
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result.append({**plain, **{f"llm_{key}": value for key, value in detail.items()}})
    return result


def claude_diligenced_domains(
    *,
    diligence_version: str = "",
    path: Path | str = CLASSIFICATION_DB,
) -> set[str]:
    return {
        str(row["domain"])
        for row in query_claude_diligence(
            diligence_version=diligence_version,
            path=path,
        )
    }


def classification_source_label(content_source: object) -> str:
    """Human label for the pipeline that originally supplied website copy."""
    source = clean(content_source).lower()
    if source.startswith("zone_universe"):
        return ".COM/.APP crawler"
    if source.startswith("crawler_cache"):
        return ".AI crawler"
    if source in {"network_fetch", "certstream", "certstream_fetch"}:
        return "CertStream"
    if source.startswith("classification_cache"):
        return "Saved classification"
    return "Unknown"


def classification_bucket_counts(
    *,
    run_id: str = "",
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, int]:
    """Count canonical classifications, optionally for one CertStream run."""
    if not using_classification_postgres() and not Path(path).exists():
        return {}
    where = ""
    params: list[object] = []
    if clean(run_id):
        where = (
            "WHERE EXISTS (SELECT 1 FROM classification_run_membership m "
            "WHERE m.domain=classifications.domain AND m.run_id=?)"
        )
        params.append(clean(run_id))
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT routing_bucket, count(*) AS total "
            f"FROM classifications {where} GROUP BY routing_bucket",
            params,
        ).fetchall()
    return {
        str(row["routing_bucket"]): int(row["total"])
        for row in rows
    }


def source_bucket_counts(
    source_prefixes: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, int]:
    """Fast bucket counts for one crawler source without cross-DB scans."""
    prefixes = [clean(value) for value in source_prefixes if clean(value)]
    if not prefixes or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return {}
    clauses = " OR ".join("content_source LIKE ?" for _ in prefixes)
    params = [f"{prefix}%" for prefix in prefixes]
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT routing_bucket,count(*) AS total FROM classifications "
            f"WHERE ({clauses}) GROUP BY routing_bucket",
            params,
        ).fetchall()
    return {str(row["routing_bucket"]): int(row["total"]) for row in rows}


def query_source_classifications(
    source_prefixes: Iterable[str],
    buckets: Iterable[str],
    *,
    limit: int = 100,
    offset: int = 0,
    sort: str = "Recently classified",
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Page classifications for a crawler using indexed lightweight columns."""
    prefixes = [clean(value) for value in source_prefixes if clean(value)]
    bucket_values = list(dict.fromkeys(clean(value) for value in buckets if clean(value)))
    if not prefixes or not bucket_values or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return []
    order = {
        "Fit: high to low": "fit_score DESC,priority_score DESC,domain",
        "Fit: low to high": "fit_score ASC,priority_score ASC,domain",
        "Company: A to Z": "lower(company_name),domain",
        "Company: Z to A": "lower(company_name) DESC,domain DESC",
        "Domain: A to Z": "domain",
        "Domain: Z to A": "domain DESC",
    }.get(sort, "updated_at DESC,domain")
    source_clauses = " OR ".join("content_source LIKE ?" for _ in prefixes)
    bucket_marks = ",".join("?" for _ in bucket_values)
    params: list[object] = [f"{prefix}%" for prefix in prefixes]
    params.extend(bucket_values)
    params.extend([max(1, min(int(limit), 250)), max(0, int(offset))])
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT domain,company_name,final_url,description,routing_bucket,"
            "routing_reason,fit_score,priority_score,thesis_pillar,"
            "content_source,manual_override,manual_override_target,updated_at FROM classifications "
            f"WHERE ({source_clauses}) AND routing_bucket IN ({bucket_marks}) "
            f"ORDER BY {order} LIMIT ? OFFSET ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def query_classifications(
    buckets: Iterable[str],
    *,
    limit: int = 100,
    offset: int = 0,
    sort: str = "Priority: high to low",
    run_id: str = "",
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Page through the master classification table without loading its HTML."""
    bucket_values = list(dict.fromkeys(str(value) for value in buckets))
    if not bucket_values or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return []
    order = {
        "Fit: high to low": "fit_score DESC, priority_score DESC, domain",
        "Fit: low to high": "fit_score ASC, priority_score ASC, domain",
        "Recently classified": "updated_at DESC, domain",
        "Recently detected": "COALESCE(NULLIF(classified_at,''),updated_at) DESC, domain",
        "Oldest classified": "updated_at ASC, domain",
        "Company: A to Z": "lower(company_name), domain",
        "Company: Z to A": "lower(company_name) DESC, domain DESC",
        "Domain: A to Z": "domain",
        "Domain: Z to A": "domain DESC",
    }.get(sort, "priority_score DESC, fit_score DESC, domain")
    bounded_limit = max(1, min(int(limit), 250))
    bounded_offset = max(0, int(offset))
    marks = ",".join("?" for _ in bucket_values)
    clauses = [f"routing_bucket IN ({marks})"]
    params: list[object] = list(bucket_values)
    if clean(run_id):
        clauses.append(
            "EXISTS (SELECT 1 FROM classification_run_membership m "
            "WHERE m.domain=classifications.domain AND m.run_id=?)"
        )
        params.append(clean(run_id))
    params.extend([bounded_limit, bounded_offset])
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT domain,company_name,final_url,description,"
            "routing_bucket,routing_reason,fit_score,priority_score,"
            "thesis_pillar,content_source,manual_override,manual_override_target,classified_at,updated_at "
            "FROM classifications WHERE "
            + " AND ".join(clauses)
            + f" ORDER BY {order} LIMIT ? OFFSET ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def claude_decisions_for_domains(
    domains: Iterable[str], *, path: Path | str = CLASSIFICATION_DB
) -> dict[str, dict[str, object]]:
    """Return compact Claude review fields for one displayed table page."""
    normalized = list(dict.fromkeys(stable_domain(value) for value in domains if stable_domain(value)))
    if not normalized or (not using_classification_postgres() and not Path(path).exists()):
        return {}
    marks = ",".join("?" for _ in normalized)
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT domain,decision,fit_score,reasoning,concerns,diligence_version,result_json,updated_at "
            f"FROM claude_diligence WHERE domain IN ({marks})",
            normalized,
        ).fetchall()
    result = {}
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result[str(row["domain"])] = {**plain, **detail}
    return result


def classification_record_for_domain(
    domain: str, *, path: Path | str = CLASSIFICATION_DB
) -> dict[str, object] | None:
    """Return one complete stored screening record for manual diligence."""
    target = stable_domain(domain)
    if not target or (not using_classification_postgres() and not Path(path).exists()):
        return None
    with connect_store(path, read_only=True) as conn:
        row = conn.execute("SELECT * FROM classifications WHERE domain=?", (target,)).fetchone()
    if not row:
        return None
    plain = dict(row)
    try:
        stored = json.loads(str(plain.get("record_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        stored = {}
    return {**stored, **plain, "domain": target}


def enqueue_claude_review(
    domain: str,
    *,
    override_budget: bool = False,
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, object]:
    target = stable_domain(domain)
    if not target:
        raise ValueError("Invalid domain")
    now = utc_now()
    with connect_store(path) as conn:
        conn.execute(
            "INSERT INTO claude_review_queue(domain,status,requested_at,started_at,completed_at,error,override_budget) "
            "VALUES (?, 'queued', ?, '', '', '', ?) ON CONFLICT(domain) DO UPDATE SET "
            "status='queued',requested_at=excluded.requested_at,started_at='',completed_at='',error='',"
            "override_budget=excluded.override_budget",
            (target, now, bool(override_budget)),
        )
        conn.commit()
    return {
        "domain": target, "status": "queued", "requested_at": now,
        "override_budget": bool(override_budget),
    }


def enqueue_new_claude_reviews(
    domains: Iterable[str], *, path: Path | str = CLASSIFICATION_DB
) -> int:
    """Queue previously unseen automatic reviews without resetting any job."""
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values:
        return 0
    queued = 0
    now = utc_now()
    with connect_store(path) as conn:
        for domain in values:
            if conn.execute("SELECT 1 FROM claude_diligence WHERE domain=?", (domain,)).fetchone():
                continue
            cursor = conn.execute(
                "INSERT INTO claude_review_queue(domain,status,requested_at,started_at,completed_at,error,override_budget) "
                "VALUES (?, 'queued', ?, '', '', '', ?) ON CONFLICT(domain) DO NOTHING",
                (domain, now, False),
            )
            queued += max(0, int(cursor.rowcount or 0))
        conn.commit()
    return queued


def defer_claude_review(
    domain: str, reason: str, *, path: Path | str = CLASSIFICATION_DB
) -> None:
    """Keep a paid review queued until a temporary budget gate clears."""
    with connect_store(path) as conn:
        conn.execute(
            "UPDATE claude_review_queue SET status='queued',started_at='',completed_at='',error=? WHERE domain=?",
            (clean(reason)[:1000], stable_domain(domain)),
        )
        conn.commit()


def recover_claude_review_queue(*, path: Path | str = CLASSIFICATION_DB) -> int:
    with connect_store(path) as conn:
        cursor = conn.execute(
            "UPDATE claude_review_queue SET status='queued',started_at='',error='' WHERE status='reviewing'"
        )
        recovered = max(0, int(cursor.rowcount or 0))
        false_failures = conn.execute(
            "UPDATE claude_review_queue SET status='queued',started_at='',completed_at='',error='' "
            "WHERE status='failed' AND error LIKE ?",
            ("%No companies selected for paid external diligence%",),
        )
        recovered += max(0, int(false_failures.rowcount or 0))
        conn.commit()
        return recovered


def claim_claude_review(*, path: Path | str = CLASSIFICATION_DB) -> dict[str, object] | None:
    with connect_store(path) as conn:
        begin_immediate(conn)
        row = conn.execute(
            "SELECT domain,requested_at,override_budget FROM claude_review_queue WHERE status='queued' "
            "ORDER BY requested_at LIMIT 1"
        ).fetchone()
        if not row:
            conn.commit()
            return None
        now = utc_now()
        conn.execute(
            "UPDATE claude_review_queue SET status='reviewing',started_at=?,error='' "
            "WHERE domain=? AND status='queued'",
            (now, row["domain"]),
        )
        conn.commit()
        return {
            "domain": str(row["domain"]), "status": "reviewing",
            "started_at": now, "override_budget": bool(row["override_budget"]),
        }


def finish_claude_review(domain: str, *, error: str = "", path: Path | str = CLASSIFICATION_DB) -> None:
    with connect_store(path) as conn:
        conn.execute(
            "UPDATE claude_review_queue SET status=?,completed_at=?,error=? WHERE domain=?",
            ("failed" if error else "completed", utc_now(), clean(error)[:1000], stable_domain(domain)),
        )
        conn.commit()


def claude_review_status(domain: str, *, path: Path | str = CLASSIFICATION_DB) -> dict[str, object] | None:
    with connect_store(path, read_only=True) as conn:
        row = conn.execute(
            "SELECT domain,status,requested_at,started_at,completed_at,error,override_budget "
            "FROM claude_review_queue WHERE domain=?",
            (stable_domain(domain),),
        ).fetchone()
    return dict(row) if row else None


def record_admin_audit(action: str, target: str = "", detail: str = "", *, path: Path | str = CLASSIFICATION_DB) -> None:
    with connect_store(path) as conn:
        conn.execute(
            "INSERT INTO admin_audit_log(action,target,detail,created_at) VALUES (?,?,?,?)",
            (clean(action), clean(target), clean(detail)[:1000], utc_now()),
        )
        conn.commit()


def admin_audit_rows(limit: int = 100, *, path: Path | str = CLASSIFICATION_DB) -> list[dict[str, object]]:
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT action,target,detail,created_at FROM admin_audit_log "
            "ORDER BY created_at DESC LIMIT ?",
            (max(1, min(int(limit), 500)),),
        ).fetchall()
    return [dict(row) for row in rows]


def query_training_labels(
    label: str, *, search: str = "", run_id: str = "",
    limit: int = 100, offset: int = 0,
    path: Path | str = CLASSIFICATION_DB,
) -> tuple[list[dict[str, object]], int]:
    target = clean(label).lower()
    if target not in {"yes", "no"}:
        return [], 0
    clauses = ["label=?"]
    params: list[object] = [target]
    if clean(search):
        clauses.append("lower(domain) LIKE ?")
        params.append(f"%{clean(search).lower()}%")
    if clean(run_id):
        clauses.append(
            "EXISTS (SELECT 1 FROM classifications c "
            "JOIN classification_run_membership m ON m.domain=c.domain "
            "WHERE c.domain=training_labels.domain AND m.run_id=?)"
        )
        params.append(clean(run_id))
    where = " AND ".join(clauses)
    with connect_store(path, read_only=True) as conn:
        total = int(conn.execute(f"SELECT count(*) AS total FROM training_labels WHERE {where}", params).fetchone()["total"])
        rows = conn.execute(
            "SELECT domain,label,rating,source,reason,labeled_at FROM training_labels "
            f"WHERE {where} ORDER BY labeled_at DESC,domain LIMIT ? OFFSET ?",
            params + [max(1, min(int(limit), 500)), max(0, int(offset))],
        ).fetchall()
    return [dict(row) for row in rows], total


def claude_review_queue_rows(limit: int = 200, *, path: Path | str = CLASSIFICATION_DB) -> list[dict[str, object]]:
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT q.domain,q.status,q.requested_at,q.started_at,q.completed_at,q.error,"
            "d.decision,d.fit_score,d.reasoning,d.concerns,d.diligence_version,d.result_json "
            "FROM claude_review_queue q "
            "LEFT JOIN claude_diligence d ON d.domain=q.domain "
            "ORDER BY q.requested_at DESC LIMIT ?",
            (max(1, min(int(limit), 500)),),
        ).fetchall()
    result = []
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result.append({**plain, **detail})
    return result


def top_claude_keeps(
    limit: int = 6,
    *,
    diligence_version: str = "",
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Highest-conviction decisive Claude approvals for the homepage."""
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT d.domain,d.fit_score,d.reasoning,d.product_summary,d.result_json,d.updated_at,"
            "c.company_name,c.final_url,c.description,c.thesis_pillar,c.classified_at "
            "FROM claude_diligence d LEFT JOIN classifications c ON c.domain=d.domain "
            "WHERE d.decision='KEEP' "
            + ("AND d.diligence_version=? " if clean(diligence_version) else "")
            + "ORDER BY d.fit_score DESC,d.updated_at DESC LIMIT ?",
            ([clean(diligence_version)] if clean(diligence_version) else [])
            + [max(1, min(int(limit), 20))],
        ).fetchall()
    result = []
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result.append({**plain, **detail})
    return result


def query_classifications_by_domains(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Fetch lightweight display records for an explicit domain page."""
    normalized = list(dict.fromkeys(
        stable_domain(domain)
        for domain in domains
        if stable_domain(domain)
    ))
    if not normalized or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return []
    rows = []
    with connect_store(path, read_only=True) as conn:
        for start in range(0, len(normalized), 800):
            batch = normalized[start:start + 800]
            marks = ",".join("?" for _ in batch)
            rows.extend(conn.execute(
                # classified_at must be selected, not just updated_at: the UI
                # dates a deal by `classified_at || updated_at`, and
                # updated_at is refreshed by every re-screen, so omitting
                # classified_at made every tracked deal read "Today" no
                # matter how long ago it was actually discovered.
                "SELECT domain,company_name,final_url,description,"
                "routing_bucket,routing_reason,fit_score,priority_score,"
                "thesis_pillar,content_source,manual_override,"
                "manual_override_target,classified_at,updated_at "
                f"FROM classifications WHERE domain IN ({marks})",
                batch,
            ).fetchall())
    return [dict(row) for row in rows]


def record_automated_exclusions(
    records: Iterable[dict[str, object]],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Keep only the tiny facts required to avoid recrawling deleted junk."""
    now = utc_now()
    normalized: dict[str, tuple[object, ...]] = {}
    for record in records:
        domain = stable_domain(record.get("domain", ""))
        if not domain:
            continue
        normalized[domain] = (
            domain,
            clean(record.get("routing_bucket", record.get("bucket", ""))),
            clean(record.get("routing_reason", record.get("reason", ""))),
            max(0, min(100, _integer(record.get(
                "glasswing_rating",
                record.get("diligence_rank_score", record.get("rule_score", 0)),
            )))),
            clean(record.get("content_source", "")),
            now,
        )
    if not normalized:
        return 0
    with connect_store(path) as conn:
        begin_immediate(conn)
        conn.executemany(
            """
            INSERT INTO automated_exclusions(
                domain,bucket,reason,glasswing_rating,content_source,excluded_at
            ) VALUES (?,?,?,?,?,?)
            ON CONFLICT(domain) DO UPDATE SET
                bucket=excluded.bucket,
                reason=excluded.reason,
                glasswing_rating=excluded.glasswing_rating,
                content_source=excluded.content_source,
                excluded_at=excluded.excluded_at
            """,
            list(normalized.values()),
        )
        conn.commit()
    return len(normalized)


def excluded_domain_set(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> set[str]:
    """Return automated tombstones, except domains later labeled by a human."""
    normalized = list(dict.fromkeys(
        stable_domain(domain)
        for domain in domains
        if stable_domain(domain)
    ))
    if not normalized or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return set()
    found: set[str] = set()
    try:
        with connect_store(path, read_only=True) as conn:
            for start in range(0, len(normalized), 800):
                batch = normalized[start:start + 800]
                marks = ",".join("?" for _ in batch)
                found.update(
                    stable_domain(row["domain"])
                    for row in conn.execute(
                        "SELECT domain FROM automated_exclusions "
                        f"WHERE domain IN ({marks})",
                        batch,
                    ).fetchall()
                )
    except (sqlite3.OperationalError, PostgresError):
        # Safe during a rolling deployment before the migration has run.
        return set()
    if not found:
        return set()
    from training_truth import protected_training_domains

    return found - protected_training_domains()


def claude_decisions_for_domains(
    domains: Iterable[str], *, path: Path | str = CLASSIFICATION_DB
) -> dict[str, dict[str, object]]:
    """Return compact Claude review fields for one displayed table page."""
    normalized = list(dict.fromkeys(stable_domain(value) for value in domains if stable_domain(value)))
    if not normalized or (not using_classification_postgres() and not Path(path).exists()):
        return {}
    marks = ",".join("?" for _ in normalized)
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT domain,decision,fit_score,reasoning,concerns,diligence_version,result_json,updated_at "
            f"FROM claude_diligence WHERE domain IN ({marks})",
            normalized,
        ).fetchall()
    result = {}
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result[str(row["domain"])] = {**plain, **detail}
    return result


def classification_record_for_domain(
    domain: str, *, path: Path | str = CLASSIFICATION_DB
) -> dict[str, object] | None:
    """Return one complete stored screening record for manual diligence."""
    target = stable_domain(domain)
    if not target or (not using_classification_postgres() and not Path(path).exists()):
        return None
    with connect_store(path, read_only=True) as conn:
        row = conn.execute("SELECT * FROM classifications WHERE domain=?", (target,)).fetchone()
    if not row:
        return None
    plain = dict(row)
    try:
        stored = json.loads(str(plain.get("record_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        stored = {}
    return {**stored, **plain, "domain": target}


def enqueue_claude_review(
    domain: str,
    *,
    override_budget: bool = False,
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, object]:
    target = stable_domain(domain)
    if not target:
        raise ValueError("Invalid domain")
    now = utc_now()
    with connect_store(path) as conn:
        conn.execute(
            "INSERT INTO claude_review_queue(domain,status,requested_at,started_at,completed_at,error,override_budget) "
            "VALUES (?, 'queued', ?, '', '', '', ?) ON CONFLICT(domain) DO UPDATE SET "
            "status='queued',requested_at=excluded.requested_at,started_at='',completed_at='',error='',"
            "override_budget=excluded.override_budget",
            (target, now, bool(override_budget)),
        )
        conn.commit()
    return {
        "domain": target, "status": "queued", "requested_at": now,
        "override_budget": bool(override_budget),
    }


def enqueue_new_claude_reviews(
    domains: Iterable[str], *, path: Path | str = CLASSIFICATION_DB
) -> int:
    """Queue previously unseen automatic reviews without resetting any job."""
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values:
        return 0
    queued = 0
    now = utc_now()
    with connect_store(path) as conn:
        for domain in values:
            if conn.execute("SELECT 1 FROM claude_diligence WHERE domain=?", (domain,)).fetchone():
                continue
            cursor = conn.execute(
                "INSERT INTO claude_review_queue(domain,status,requested_at,started_at,completed_at,error,override_budget) "
                "VALUES (?, 'queued', ?, '', '', '', ?) ON CONFLICT(domain) DO NOTHING",
                (domain, now, False),
            )
            queued += max(0, int(cursor.rowcount or 0))
        conn.commit()
    return queued


def defer_claude_review(
    domain: str, reason: str, *, path: Path | str = CLASSIFICATION_DB
) -> None:
    """Keep a paid review queued until a temporary budget gate clears."""
    with connect_store(path) as conn:
        conn.execute(
            "UPDATE claude_review_queue SET status='queued',started_at='',completed_at='',error=? WHERE domain=?",
            (clean(reason)[:1000], stable_domain(domain)),
        )
        conn.commit()


def recover_claude_review_queue(*, path: Path | str = CLASSIFICATION_DB) -> int:
    with connect_store(path) as conn:
        cursor = conn.execute(
            "UPDATE claude_review_queue SET status='queued',started_at='',error='' WHERE status='reviewing'"
        )
        recovered = max(0, int(cursor.rowcount or 0))
        false_failures = conn.execute(
            "UPDATE claude_review_queue SET status='queued',started_at='',completed_at='',error='' "
            "WHERE status='failed' AND error LIKE ?",
            ("%No companies selected for paid external diligence%",),
        )
        recovered += max(0, int(false_failures.rowcount or 0))
        conn.commit()
        return recovered


def claim_claude_review(*, path: Path | str = CLASSIFICATION_DB) -> dict[str, object] | None:
    with connect_store(path) as conn:
        begin_immediate(conn)
        row = conn.execute(
            "SELECT domain,requested_at,override_budget FROM claude_review_queue WHERE status='queued' "
            "ORDER BY requested_at LIMIT 1"
        ).fetchone()
        if not row:
            conn.commit()
            return None
        now = utc_now()
        conn.execute(
            "UPDATE claude_review_queue SET status='reviewing',started_at=?,error='' "
            "WHERE domain=? AND status='queued'",
            (now, row["domain"]),
        )
        conn.commit()
        return {
            "domain": str(row["domain"]), "status": "reviewing",
            "started_at": now, "override_budget": bool(row["override_budget"]),
        }


def finish_claude_review(domain: str, *, error: str = "", path: Path | str = CLASSIFICATION_DB) -> None:
    with connect_store(path) as conn:
        conn.execute(
            "UPDATE claude_review_queue SET status=?,completed_at=?,error=? WHERE domain=?",
            ("failed" if error else "completed", utc_now(), clean(error)[:1000], stable_domain(domain)),
        )
        conn.commit()


def claude_review_status(domain: str, *, path: Path | str = CLASSIFICATION_DB) -> dict[str, object] | None:
    with connect_store(path, read_only=True) as conn:
        row = conn.execute(
            "SELECT domain,status,requested_at,started_at,completed_at,error,override_budget "
            "FROM claude_review_queue WHERE domain=?",
            (stable_domain(domain),),
        ).fetchone()
    return dict(row) if row else None


def record_admin_audit(action: str, target: str = "", detail: str = "", *, path: Path | str = CLASSIFICATION_DB) -> None:
    with connect_store(path) as conn:
        conn.execute(
            "INSERT INTO admin_audit_log(action,target,detail,created_at) VALUES (?,?,?,?)",
            (clean(action), clean(target), clean(detail)[:1000], utc_now()),
        )
        conn.commit()


def admin_audit_rows(limit: int = 100, *, path: Path | str = CLASSIFICATION_DB) -> list[dict[str, object]]:
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT action,target,detail,created_at FROM admin_audit_log "
            "ORDER BY created_at DESC LIMIT ?",
            (max(1, min(int(limit), 500)),),
        ).fetchall()
    return [dict(row) for row in rows]


def query_training_labels(
    label: str, *, search: str = "", run_id: str = "",
    limit: int = 100, offset: int = 0,
    path: Path | str = CLASSIFICATION_DB,
) -> tuple[list[dict[str, object]], int]:
    target = clean(label).lower()
    if target not in {"yes", "no"}:
        return [], 0
    clauses = ["label=?"]
    params: list[object] = [target]
    if clean(search):
        clauses.append("lower(domain) LIKE ?")
        params.append(f"%{clean(search).lower()}%")
    if clean(run_id):
        clauses.append(
            "EXISTS (SELECT 1 FROM classifications c "
            "JOIN classification_run_membership m ON m.domain=c.domain "
            "WHERE c.domain=training_labels.domain AND m.run_id=?)"
        )
        params.append(clean(run_id))
    where = " AND ".join(clauses)
    with connect_store(path, read_only=True) as conn:
        total = int(conn.execute(f"SELECT count(*) AS total FROM training_labels WHERE {where}", params).fetchone()["total"])
        rows = conn.execute(
            "SELECT domain,label,rating,source,reason,labeled_at FROM training_labels "
            f"WHERE {where} ORDER BY labeled_at DESC,domain LIMIT ? OFFSET ?",
            params + [max(1, min(int(limit), 500)), max(0, int(offset))],
        ).fetchall()
    return [dict(row) for row in rows], total


def claude_review_queue_rows(limit: int = 200, *, path: Path | str = CLASSIFICATION_DB) -> list[dict[str, object]]:
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT q.domain,q.status,q.requested_at,q.started_at,q.completed_at,q.error,"
            "d.decision,d.fit_score,d.reasoning,d.concerns,d.diligence_version,d.result_json "
            "FROM claude_review_queue q "
            "LEFT JOIN claude_diligence d ON d.domain=q.domain "
            "ORDER BY q.requested_at DESC LIMIT ?",
            (max(1, min(int(limit), 500)),),
        ).fetchall()
    result = []
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result.append({**plain, **detail})
    return result


def top_claude_keeps(
    limit: int = 6,
    *,
    diligence_version: str = "",
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Highest-conviction decisive Claude approvals for the homepage."""
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT d.domain,d.fit_score,d.reasoning,d.product_summary,d.result_json,d.updated_at,"
            "c.company_name,c.final_url,c.description,c.thesis_pillar,c.classified_at "
            "FROM claude_diligence d LEFT JOIN classifications c ON c.domain=d.domain "
            "WHERE d.decision='KEEP' "
            + ("AND d.diligence_version=? " if clean(diligence_version) else "")
            + "ORDER BY d.fit_score DESC,d.updated_at DESC LIMIT ?",
            ([clean(diligence_version)] if clean(diligence_version) else [])
            + [max(1, min(int(limit), 20))],
        ).fetchall()
    result = []
    for row in rows:
        plain = dict(row)
        try:
            detail = json.loads(str(plain.pop("result_json", "{}") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = {}
        result.append({**plain, **detail})
    return result


def query_classifications_by_domains(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Fetch lightweight display records for an explicit domain page."""
    normalized = list(dict.fromkeys(
        stable_domain(domain)
        for domain in domains
        if stable_domain(domain)
    ))
    if not normalized or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return []
    rows = []
    with connect_store(path, read_only=True) as conn:
        for start in range(0, len(normalized), 800):
            batch = normalized[start:start + 800]
            marks = ",".join("?" for _ in batch)
            rows.extend(conn.execute(
                # classified_at must be selected, not just updated_at: the UI
                # dates a deal by `classified_at || updated_at`, and
                # updated_at is refreshed by every re-screen, so omitting
                # classified_at made every tracked deal read "Today" no
                # matter how long ago it was actually discovered.
                "SELECT domain,company_name,final_url,description,"
                "routing_bucket,routing_reason,fit_score,priority_score,"
                "thesis_pillar,content_source,manual_override,"
                "manual_override_target,classified_at,updated_at "
                f"FROM classifications WHERE domain IN ({marks})",
                batch,
            ).fetchall())
    return [dict(row) for row in rows]


def record_automated_exclusions(
    records: Iterable[dict[str, object]],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Keep only the tiny facts required to avoid recrawling deleted junk."""
    now = utc_now()
    normalized: dict[str, tuple[object, ...]] = {}
    for record in records:
        domain = stable_domain(record.get("domain", ""))
        if not domain:
            continue
        normalized[domain] = (
            domain,
            clean(record.get("routing_bucket", record.get("bucket", ""))),
            clean(record.get("routing_reason", record.get("reason", ""))),
            max(0, min(100, _integer(record.get(
                "glasswing_rating",
                record.get("diligence_rank_score", record.get("rule_score", 0)),
            )))),
            clean(record.get("content_source", "")),
            now,
        )
    if not normalized:
        return 0
    with connect_store(path) as conn:
        begin_immediate(conn)
        conn.executemany(
            """
            INSERT INTO automated_exclusions(
                domain,bucket,reason,glasswing_rating,content_source,excluded_at
            ) VALUES (?,?,?,?,?,?)
            ON CONFLICT(domain) DO UPDATE SET
                bucket=excluded.bucket,
                reason=excluded.reason,
                glasswing_rating=excluded.glasswing_rating,
                content_source=excluded.content_source,
                excluded_at=excluded.excluded_at
            """,
            list(normalized.values()),
        )
        conn.commit()
    return len(normalized)


def excluded_domain_set(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> set[str]:
    """Return automated tombstones, except domains later labeled by a human."""
    normalized = list(dict.fromkeys(
        stable_domain(domain)
        for domain in domains
        if stable_domain(domain)
    ))
    if not normalized or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return set()
    found: set[str] = set()
    try:
        with connect_store(path, read_only=True) as conn:
            for start in range(0, len(normalized), 800):
                batch = normalized[start:start + 800]
                marks = ",".join("?" for _ in batch)
                found.update(
                    stable_domain(row["domain"])
                    for row in conn.execute(
                        "SELECT domain FROM automated_exclusions "
                        f"WHERE domain IN ({marks})",
                        batch,
                    ).fetchall()
                )
    except (sqlite3.OperationalError, PostgresError):
        # Safe during a rolling deployment before the migration has run.
        return set()
    if not found:
        return set()
    from training_truth import protected_training_domains

    return found - protected_training_domains()


def automated_exclusion_reason_counts(
    *,
    since_hours: int = 0,
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, int]:
    """How many domains each automated rule has discarded.

    The scorer that produces these is explicitly documented as untrustworthy on
    its own ("Python classifications are suggestions, not labels"), yet it
    discards the large majority of readable sites. Surfacing the shape of that
    decision is what makes it auditable instead of invisible.
    """
    if not using_classification_postgres() and not Path(path).exists():
        return {}
    clauses = ""
    params: list[object] = []
    if since_hours > 0:
        clauses = " WHERE excluded_at >= ?"
        params.append(
            (
                datetime.now(timezone.utc) - timedelta(hours=int(since_hours))
            ).isoformat().replace("+00:00", "Z")
        )
    try:
        with connect_store(path, read_only=True) as conn:
            rows = conn.execute(
                "SELECT reason, COUNT(*) AS total FROM automated_exclusions"
                + clauses
                + " GROUP BY reason ORDER BY total DESC",
                params,
            ).fetchall()
    except Exception as exc:
        # Only a genuinely absent table means "nothing discarded". Anything
        # else -- a pool timeout, a connection failure -- must surface rather
        # than be reported as a confident zero. psycopg_pool's PoolTimeout
        # subclasses psycopg.Error, so a broad except here silently turned an
        # unreachable database into "0 domains discarded", which is exactly the
        # kind of reassuring lie this page exists to eliminate.
        if _missing_relation(exc, "automated_exclusions"):
            return {}
        raise
    return {str(row["reason"]): int(row["total"]) for row in rows}


def query_automated_exclusions(
    *,
    reason: str = "",
    since_hours: int = 0,
    limit: int = 100,
    offset: int = 0,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Page through what the automated rules have thrown away."""
    if not using_classification_postgres() and not Path(path).exists():
        return []
    clauses: list[str] = []
    params: list[object] = []
    if reason:
        clauses.append("reason = ?")
        params.append(clean(reason))
    if since_hours > 0:
        clauses.append("excluded_at >= ?")
        params.append(
            (
                datetime.now(timezone.utc) - timedelta(hours=int(since_hours))
            ).isoformat().replace("+00:00", "Z")
        )
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    params.extend([max(1, min(int(limit), 500)), max(0, int(offset))])
    try:
        with connect_store(path, read_only=True) as conn:
            rows = conn.execute(
                "SELECT domain, bucket, reason, glasswing_rating, "
                "content_source, excluded_at FROM automated_exclusions"
                + where
                + " ORDER BY excluded_at DESC, domain LIMIT ? OFFSET ?",
                params,
            ).fetchall()
    except Exception as exc:
        if _missing_relation(exc, "automated_exclusions"):
            return []
        raise
    return [dict(row) for row in rows]


def upsert_training_labels(
    records: Iterable[dict[str, object]],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Mirror human Yes/No truth into the cross-service canonical store."""
    normalized: dict[str, tuple[object, ...]] = {}
    now = utc_now()
    for record in records:
        domain = stable_domain(record.get("domain", record.get("domain_url", "")))
        label = clean(record.get("label", "")).lower()
        if not domain or label not in {"yes", "no"}:
            continue
        normalized[domain] = (
            domain,
            label,
            max(0, min(100, _integer(record.get("rating", 0)))),
            clean(record.get("source", "human")).lower() or "human",
            clean(record.get("reason", "")),
            clean(record.get("labeled_at", "")) or now,
        )
    if not normalized:
        return 0
    with connect_store(path) as conn:
        begin_immediate(conn)
        conn.executemany(
            """
            INSERT INTO training_labels(
                domain,label,rating,source,reason,labeled_at
            ) VALUES (?,?,?,?,?,?)
            ON CONFLICT(domain) DO UPDATE SET
                label=excluded.label,
                rating=excluded.rating,
                source=excluded.source,
                reason=excluded.reason,
                labeled_at=excluded.labeled_at
            """,
            list(normalized.values()),
        )
        conn.commit()
    return len(normalized)


def training_label_rows(
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Return the canonical cross-service Yes/No label set."""
    if not using_classification_postgres() and not Path(path).exists():
        return []
    try:
        with connect_store(path, read_only=True) as conn:
            rows = conn.execute(
                "SELECT domain,label,rating,source,reason,labeled_at "
                "FROM training_labels"
            ).fetchall()
    except (sqlite3.OperationalError, PostgresError):
        # Safe while an older deployment is still creating the table.
        return []
    return [dict(row) for row in rows]


def backfill_claude_training_labels(
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Reconcile historical decisive Claude results into shared baskets."""
    now = utc_now()
    with connect_store(path) as conn:
        begin_immediate(conn)
        before = conn.execute(
            "SELECT COUNT(*) AS count FROM training_labels WHERE source='claude'"
        ).fetchone()["count"]
        conn.execute(
            """
            INSERT INTO training_labels(
                domain,label,rating,source,reason,labeled_at
            )
            SELECT domain,
                   CASE decision WHEN 'KEEP' THEN 'yes' ELSE 'no' END,
                   fit_score,'claude',
                   CASE decision WHEN 'KEEP' THEN 'claude_keep' ELSE 'claude_reject' END,
                   COALESCE(NULLIF(updated_at, ''), ?)
            FROM claude_diligence
            WHERE decision IN ('KEEP','REJECT')
            ON CONFLICT(domain) DO UPDATE SET
                label=excluded.label,
                rating=excluded.rating,
                source=excluded.source,
                reason=excluded.reason,
                labeled_at=excluded.labeled_at
            WHERE lower(training_labels.source) NOT IN ('human','glasswing_portfolio')
            """,
            (now,),
        )
        after = conn.execute(
            "SELECT COUNT(*) AS count FROM training_labels WHERE source='claude'"
        ).fetchone()["count"]
        conn.commit()
    return max(0, int(after) - int(before))


def purge_invalid_claude_diligence(
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Remove only failed/empty automated results so they can be retried."""
    invalid_where = """
        reasoning='Diligence call failed'
        OR result_json LIKE ?
        OR (
            trim(product_summary)=''
            AND trim(reasoning)=''
            AND trim(diligence_sources)=''
        )
    """
    with connect_store(path) as conn:
        invalid = [
            str(row["domain"])
            for row in conn.execute(
                f"SELECT domain FROM claude_diligence WHERE {invalid_where}",
                ('%"diligence_error"%',),
            ).fetchall()
        ]
        if not invalid:
            return 0
        begin_immediate(conn)
        for start in range(0, len(invalid), 800):
            batch = invalid[start : start + 800]
            marks = ",".join("?" for _ in batch)
            conn.execute(
                "DELETE FROM training_labels "
                f"WHERE source='claude' AND domain IN ({marks})",
                batch,
            )
            conn.execute(
                f"DELETE FROM claude_diligence WHERE domain IN ({marks})",
                batch,
            )
        conn.commit()
    return len(invalid)


def remove_training_labels(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    normalized = list(dict.fromkeys(
        stable_domain(domain)
        for domain in domains
        if stable_domain(domain)
    ))
    if not normalized or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return 0
    removed = 0
    with connect_store(path) as conn:
        begin_immediate(conn)
        for start in range(0, len(normalized), 800):
            batch = normalized[start:start + 800]
            marks = ",".join("?" for _ in batch)
            cursor = conn.execute(
                f"DELETE FROM training_labels WHERE domain IN ({marks})",
                batch,
            )
            removed += max(0, int(cursor.rowcount))
        conn.commit()
    return removed


def hide_domains(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Mark domains as hidden from review tables -- a view preference only.

    Never touches classifications/manual_decisions; the domain stays exactly
    where it is in whatever dataset it came from (crawl results, search
    corpus, ...). This is purely "I've looked at this, don't show it to me
    again" bookkeeping, shared across every table in the app since it's
    keyed on domain alone.
    """
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values:
        return 0
    now = utc_now()
    with connect_store(path) as conn:
        conn.executemany(
            """
            INSERT INTO hidden_domains (domain, hidden_at) VALUES (?, ?)
            ON CONFLICT(domain) DO UPDATE SET hidden_at = excluded.hidden_at
            """,
            [(domain, now) for domain in values],
        )
        conn.commit()
    return len(values)


def unhide_domains(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values:
        return 0
    removed = 0
    with connect_store(path) as conn:
        for start in range(0, len(values), 700):
            batch = values[start : start + 700]
            marks = ",".join("?" for _ in batch)
            cursor = conn.execute(
                f"DELETE FROM hidden_domains WHERE domain IN ({marks})",
                batch,
            )
            removed += max(0, int(cursor.rowcount))
        conn.commit()
    return removed


def _missing_hidden_table(exc: Exception) -> bool:
    """Only suppress the expected pre-migration missing-table read."""
    current: BaseException | None = exc
    while current is not None:
        if getattr(current, "sqlstate", "") == "42P01":
            return True
        message = str(current).lower()
        if "no such table: hidden_domains" in message:
            return True
        current = current.__cause__
    return False


def _missing_relation(exc: Exception, table: str) -> bool:
    """Identify a missing SQLite/Postgres table on read-only migrations."""
    current: BaseException | None = exc
    target = str(table).lower()
    while current is not None:
        if getattr(current, "sqlstate", "") == "42P01":
            return target in str(current).lower()
        message = str(current).lower()
        if f"no such table: {target}" in message:
            return True
        current = current.__cause__
    return False


def hidden_domain_set(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> set[str]:
    """Which of the given domains are currently hidden."""
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return set()
    try:
        with connect_store(path, read_only=True) as conn:
            rows = []
            for start in range(0, len(values), 700):
                batch = values[start : start + 700]
                marks = ",".join("?" for _ in batch)
                rows.extend(conn.execute(
                    f"SELECT domain FROM hidden_domains WHERE domain IN ({marks})",
                    batch,
                ).fetchall())
    except Exception as exc:
        # Read-only connections never run migrate() (see connect_store), so
        # a database predating this table -- real locally and in production,
        # since neither backend migrates on a read path -- doesn't have it
        # yet. No rows hidden is the correct answer either way; the table
        # gets created for real the first time hide_domains() runs (it uses
        # a writable connection).
        if _missing_hidden_table(exc):
            return set()
        raise
    return {str(row["domain"]) for row in rows}


def list_hidden_domains(
    *,
    limit: int = 500,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, str]]:
    """Return recent hidden preferences for a global unhide control."""
    if not using_classification_postgres() and not Path(path).exists():
        return []
    try:
        with connect_store(path, read_only=True) as conn:
            rows = conn.execute(
                "SELECT domain,hidden_at FROM hidden_domains "
                "ORDER BY hidden_at DESC,domain LIMIT ?",
                (max(1, min(int(limit), 5000)),),
            ).fetchall()
    except Exception as exc:
        if _missing_hidden_table(exc):
            return []
        raise
    return [dict(row) for row in rows]


def track_domains(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Add companies to the shared homepage tracking list."""
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values:
        return 0
    now = utc_now()
    with connect_store(path) as conn:
        conn.executemany(
            """
            INSERT INTO tracked_domains (domain, tracked_at) VALUES (?, ?)
            ON CONFLICT(domain) DO NOTHING
            """,
            [(domain, now) for domain in values],
        )
        conn.commit()
    return len(values)


def untrack_domains(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Remove companies from the shared homepage tracking list."""
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values:
        return 0
    removed = 0
    with connect_store(path) as conn:
        for start in range(0, len(values), 700):
            batch = values[start : start + 700]
            marks = ",".join("?" for _ in batch)
            cursor = conn.execute(
                f"DELETE FROM tracked_domains WHERE domain IN ({marks})",
                batch,
            )
            removed += max(0, int(cursor.rowcount))
        conn.commit()
    return removed


def set_tracking_status(
    domain: str,
    status: str,
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> bool:
    """Move a tracked company through the lightweight sourcing workflow."""
    target = stable_domain(domain)
    normalized_status = str(status).strip().lower()
    if not target or normalized_status not in {"tracking", "reached_out", "meeting"}:
        return False
    reached_out_at = utc_now() if normalized_status in {"reached_out", "meeting"} else ""
    with connect_store(path) as conn:
        cursor = conn.execute(
            "UPDATE tracked_domains SET tracking_status=?,reached_out_at=? "
            "WHERE domain=?",
            (normalized_status, reached_out_at, target),
        )
        conn.commit()
    return int(cursor.rowcount or 0) > 0


def tracked_domain_set(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> set[str]:
    """Return the requested domains that are on the tracking list."""
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return set()
    try:
        with connect_store(path, read_only=True) as conn:
            rows = []
            for start in range(0, len(values), 700):
                batch = values[start : start + 700]
                marks = ",".join("?" for _ in batch)
                rows.extend(conn.execute(
                    f"SELECT domain FROM tracked_domains WHERE domain IN ({marks})",
                    batch,
                ).fetchall())
    except Exception as exc:
        if _missing_relation(exc, "tracked_domains"):
            return set()
        raise
    return {str(row["domain"]) for row in rows}


def list_tracked_domains(
    *,
    limit: int = 100,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, str]]:
    """Return tracked domains newest first."""
    if not using_classification_postgres() and not Path(path).exists():
        return []
    try:
        with connect_store(path, read_only=True) as conn:
            rows = conn.execute(
                "SELECT domain,tracked_at,tracking_status,reached_out_at "
                "FROM tracked_domains "
                "ORDER BY tracked_at DESC,domain LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
    except Exception as exc:
        if _missing_relation(exc, "tracked_domains"):
            return []
        raise
    return [dict(row) for row in rows]


def bootstrap_from_csv(
    csv_path: Path = FILTERED / "all_screened.csv",
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """One-time import of historical rows into the canonical database."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return 0
    existing = count_classifications(path)
    imported = 0
    for chunk in pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        chunksize=1000,
    ):
        imported += upsert_classifications(chunk, path)
    return max(0, count_classifications(path) - existing)


def import_private_sqlite_into_shared(
    sqlite_path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Merge classifications stranded on a worker volume into Postgres.

    This is a one-time cloud migration helper. It reads SQLite explicitly so
    backend auto-selection cannot redirect the source read back to Postgres,
    and imports only records newer than the shared store's latest update.
    Existing human overrides are protected by ``upsert_classifications``.
    """
    source = Path(sqlite_path)
    if not using_classification_postgres() or not source.exists():
        return 0

    with connect_store(read_only=True) as shared:
        row = shared.execute(
            "SELECT max(updated_at) AS latest FROM classifications"
        ).fetchone()
    cutoff = pd.to_datetime(
        row["latest"] if row else "",
        utc=True,
        errors="coerce",
    )

    with sqlite3.connect(
        f"file:{source}?mode=ro",
        uri=True,
        timeout=30,
    ) as local:
        try:
            rows = local.execute(
                "SELECT record_json FROM classifications"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                return 0
            raise

    records: list[dict[str, object]] = []
    for (raw_record,) in rows:
        try:
            record = json.loads(raw_record or "{}")
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(record, dict):
            records.append(record)
    if not records:
        return 0

    frame = pd.DataFrame(records)
    timestamp = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    for column in ("screened_at", "observed_at", "run_started_at"):
        if column not in frame.columns:
            continue
        candidate = pd.to_datetime(frame[column], utc=True, errors="coerce")
        timestamp = timestamp.fillna(candidate)
    if not pd.isna(cutoff):
        frame = frame[timestamp.gt(cutoff)].copy()
    return upsert_classifications(frame)


def classification_map(
    domains: Iterable[str] | None = None,
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, str]:
    if not using_classification_postgres() and not Path(path).exists():
        return {}
    with connect_store(path, read_only=True) as conn:
        if domains is None:
            rows = conn.execute(
                "SELECT domain, routing_bucket FROM classifications"
            ).fetchall()
        else:
            normalized = list(
                dict.fromkeys(
                    stable_domain(domain)
                    for domain in domains
                    if stable_domain(domain)
                )
            )
            if not normalized:
                return {}
            rows = []
            for start in range(0, len(normalized), 800):
                batch = normalized[start : start + 800]
                marks = ",".join("?" for _ in batch)
                rows.extend(
                    conn.execute(
                        "SELECT domain, routing_bucket FROM classifications "
                        f"WHERE domain IN ({marks})",
                        batch,
                    ).fetchall()
                )
    return {str(row["domain"]): str(row["routing_bucket"]) for row in rows}


def record_manual_move(
    domains: Iterable[str],
    *,
    target_label: str,
    target_bucket: str,
    scores: dict[str, int] | None = None,
    note: str = "",
    source: str = "human",
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    normalized = list(
        dict.fromkeys(
            stable_domain(domain)
            for domain in domains
            if stable_domain(domain)
        )
    )
    if not normalized:
        return 0
    scores = scores or {}
    now = utc_now()
    with connect_store(path) as conn:
        begin_immediate(conn)
        for domain in normalized:
            current = conn.execute(
                "SELECT routing_bucket, record_json, model_version "
                "FROM classifications WHERE domain=?",
                (domain,),
            ).fetchone()
            from_bucket = str(current["routing_bucket"]) if current else ""
            current_model_version = (
                str(current["model_version"]) if current else MODEL_VERSION
            )
            objective_score = max(
                0,
                min(100, int(scores.get(domain, 0))),
            )
            conn.execute(
                """
                INSERT INTO manual_decisions(
                    domain,target_label,target_bucket,objective_score,
                    note,source,decided_at
                ) VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(domain) DO UPDATE SET
                    target_label=excluded.target_label,
                    target_bucket=excluded.target_bucket,
                    objective_score=excluded.objective_score,
                    note=excluded.note,
                    source=excluded.source,
                    decided_at=excluded.decided_at
                """,
                (
                    domain,
                    target_label,
                    target_bucket,
                    objective_score,
                    clean(note),
                    clean(source) or "human",
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO movement_log(
                    domain,from_bucket,to_bucket,objective_score,
                    note,source,moved_at,predicted_bucket,model_version
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    domain,
                    from_bucket,
                    target_bucket,
                    objective_score,
                    clean(note),
                    clean(source) or "human",
                    now,
                    from_bucket,
                    current_model_version,
                ),
            )
            if current:
                try:
                    record = json.loads(current["record_json"] or "{}")
                except (TypeError, json.JSONDecodeError):
                    record = {}
                record.update(
                    {
                        "routing_bucket": target_bucket,
                        "routing_reason": f"manual_override_{target_bucket}",
                        "manual_override": True,
                        "manual_override_target": target_label,
                    }
                )
                normalized_row = normalized_record(record)
                conn.execute(
                    """
                    UPDATE classifications SET
                        routing_bucket=?,
                        routing_reason=?,
                        fit_score=?,
                        confidence_score=?,
                        priority_score=?,
                        manual_override=1,
                        manual_override_target=?,
                        record_json=?,
                        updated_at=?
                    WHERE domain=?
                    """,
                    (
                        target_bucket,
                        f"manual_override_{target_bucket}",
                        normalized_row["fit_score"],
                        normalized_row["confidence_score"],
                        normalized_row["priority_score"],
                        target_label,
                        normalized_row["record_json"],
                        now,
                        domain,
                    ),
                )
        conn.commit()
    if Path(path).resolve() == Path(CLASSIFICATION_DB).resolve():
        from manual_truth_backup import create_manual_snapshot

        create_manual_snapshot(
            reason=f"manual_move:{clean(source) or 'human'}",
            classification_db=path,
        )
        from model_evaluation import write_performance_metrics

        write_performance_metrics()
    return len(normalized)


def recent_movements(
    *,
    limit: int = 100,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    if not using_classification_postgres() and not Path(path).exists():
        return []
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT domain,from_bucket,to_bucket,objective_score,
                   note,source,moved_at,predicted_bucket,model_version
            FROM movement_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(int(limit), 500)),),
        ).fetchall()
    return [dict(row) for row in rows]


def recent_digest_runs(
    *,
    limit: int = 20,
    path: Path | str = CLASSIFICATION_DB,
) -> list[dict[str, object]]:
    """Daily digest run history -- written by daily_digest.py, but nothing
    ever reads it back; a failed run is otherwise invisible."""
    if not using_classification_postgres() and not Path(path).exists():
        return []
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT run_id, status, started_at, completed_at,
                   candidate_count, sent_count, recipient_count, error
            FROM daily_digest_runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (max(1, min(int(limit), 200)),),
        ).fetchall()
    return [dict(row) for row in rows]


def delete_bucket_classifications(
    bucket: str,
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Bulk-delete every classification currently in one routing bucket.

    Explicit human decisions and verified training truth are never touched
    by this, matching every other bulk operation in this store: a domain is
    skipped if it has a manual_decisions row (a human, not Python/Claude,
    put it there or confirmed it) or is in protected_training_domains().
    """
    if not using_classification_postgres() and not Path(path).exists():
        return 0
    from training_truth import protected_training_domains

    protected = protected_training_domains()
    with connect_store(path) as conn:
        candidates = [
            str(row["domain"])
            for row in conn.execute(
                "SELECT domain FROM classifications WHERE routing_bucket=?",
                (bucket,),
            ).fetchall()
        ]
        manually_decided = {
            str(row["domain"])
            for row in conn.execute(
                "SELECT domain FROM manual_decisions"
            ).fetchall()
        }
        to_delete = [
            domain
            for domain in candidates
            if domain not in protected and domain not in manually_decided
        ]
        if not to_delete:
            return 0
        begin_immediate(conn)
        for start in range(0, len(to_delete), 800):
            batch = to_delete[start : start + 800]
            marks = ",".join("?" for _ in batch)
            conn.execute(
                f"DELETE FROM classifications WHERE domain IN ({marks})",
                batch,
            )
        conn.commit()
    return len(to_delete)


def discard_uninvestable_classifications(
    domains: Iterable[str],
    *,
    path: Path | str = CLASSIFICATION_DB,
) -> int:
    """Discard machine-rejected crawl results while preserving human truth.

    Zone crawling can inspect millions of names. Only ``priority`` companies
    belong in the durable company corpus; negative machine output is useful
    for the current batch counters but should not grow the primary store.
    Explicit human decisions are always protected.
    """
    values = list(dict.fromkeys(
        stable_domain(domain) for domain in domains if stable_domain(domain)
    ))
    if not values or (
        not using_classification_postgres() and not Path(path).exists()
    ):
        return 0
    removed = 0
    with connect_store(path) as conn:
        begin_immediate(conn)
        for start in range(0, len(values), 700):
            batch = values[start : start + 700]
            marks = ",".join("?" for _ in batch)
            params = batch + batch + batch
            cursor = conn.execute(
                f"DELETE FROM classifications WHERE domain IN ({marks}) "
                "AND routing_bucket<>'priority' "
                f"AND domain NOT IN (SELECT domain FROM manual_decisions WHERE domain IN ({marks})) "
                f"AND domain NOT IN (SELECT domain FROM training_labels WHERE domain IN ({marks}))",
                params,
            )
            removed += max(0, int(cursor.rowcount or 0))
            conn.execute(
                f"DELETE FROM automated_exclusions WHERE domain IN ({marks})",
                batch,
            )
            conn.execute(
                f"DELETE FROM classification_run_membership WHERE domain IN ({marks}) "
                "AND domain NOT IN (SELECT domain FROM classifications)",
                batch,
            )
        conn.commit()
    return removed


_CRAWLED_CORPUS_CACHE_SECONDS = 10.0
_crawled_corpus_cache: dict[str, tuple[float, list[str]]] = {}


def _crawled_corpus_domains(
    search_db: Path,
    *,
    connect_fn=None,
) -> list[str]:
    """Domains with crawl evidence in a corpus DB (.AI search / Zone Universe).

    Those corpora predate a shared classification backend and used to always
    stay on SQLite regardless of it -- that stopped being true once ai_search
    and zone_universe each got their own optional Postgres backend, but nothing
    here was updated to match. In production both moved to Postgres and their
    local SQLite files were correctly identified as unused and removed, which
    made this silently return nothing (not error) -- "Bucketed records: 0"
    despite thousands actually screened. Callers that are Postgres-backed
    should pass their own subsystem's ``connect()`` (already Postgres-aware);
    callers still purely on SQLite can omit it for the original behavior.

    A single page render can call this several times over (once per bucket
    tab, since Streamlit executes every ``st.tabs()`` block on each rerun) --
    each a full table scan. Cache briefly per-path so a page load costs one
    scan, not one per caller, without risking staleness beyond a few seconds.
    """
    cache_key = str(search_db)
    cached = _crawled_corpus_cache.get(cache_key)
    now = time.monotonic()
    if cached is not None and now - cached[0] < _CRAWLED_CORPUS_CACHE_SECONDS:
        return cached[1]
    if connect_fn is not None:
        conn = connect_fn(search_db, read_only=True)
        try:
            domains = [
                str(row["domain"])
                for row in conn.execute(
                    "SELECT domain FROM domains WHERE last_crawled_at IS NOT NULL"
                )
            ]
        finally:
            conn.close()
    else:
        with sqlite3.connect(f"file:{search_db}?mode=ro", uri=True, timeout=30) as conn:
            domains = [
                str(row[0])
                for row in conn.execute(
                    "SELECT domain FROM domains WHERE last_crawled_at IS NOT NULL"
                )
            ]
    _crawled_corpus_cache[cache_key] = (now, domains)
    return domains


def corpus_bucket_counts(
    search_db: Path,
    path: Path | str = CLASSIFICATION_DB,
    *,
    connect_fn=None,
) -> dict[str, int]:
    if not using_classification_postgres() and not Path(path).exists():
        return {}
    if connect_fn is None and not search_db.exists():
        return {}
    if using_classification_postgres():
        domains = set(_crawled_corpus_domains(search_db, connect_fn=connect_fn))
        if not domains:
            return {}
        counts: dict[str, int] = {}
        # Filter by domain in Python against a single unfiltered fetch rather
        # than chunking the (potentially tens-of-thousands-long) domain list
        # into repeated IN (...) round trips -- with a large crawled corpus,
        # that turned one page load into 20+ sequential Postgres queries.
        with connect_store(path) as conn:
            for row in conn.execute(
                "SELECT domain, routing_bucket FROM classifications"
            ).fetchall():
                if str(row["domain"]) not in domains:
                    continue
                bucket = str(row["routing_bucket"])
                counts[bucket] = counts.get(bucket, 0) + 1
        return counts
    with connect_store(path) as conn:
        conn.execute("ATTACH DATABASE ? AS corpus", (str(search_db),))
        rows = conn.execute(
            """
            SELECT c.routing_bucket, count(*) AS total
            FROM classifications c
            JOIN corpus.domains d ON d.domain=c.domain
            WHERE d.last_crawled_at IS NOT NULL
            GROUP BY c.routing_bucket
            """
        ).fetchall()
    return {str(row["routing_bucket"]): int(row["total"]) for row in rows}


def query_corpus_classifications(
    search_db: Path,
    buckets: Iterable[str],
    *,
    limit: int = 100,
    offset: int = 0,
    sort: str = "Priority: high to low",
    path: Path | str = CLASSIFICATION_DB,
    connect_fn=None,
) -> list[dict[str, object]]:
    bucket_values = list(dict.fromkeys(str(bucket) for bucket in buckets))
    if not bucket_values or (connect_fn is None and not search_db.exists()):
        return []
    if not using_classification_postgres() and not Path(path).exists():
        return []
    order = {
        "Fit: high to low": "fit_score DESC, domain",
        "Confidence: high to low": "confidence_score DESC, domain",
        "Company: A to Z": "lower(company_name), domain",
        "Domain: A to Z": "domain",
        "Recently classified": "updated_at DESC, domain",
    }.get(sort, "priority_score DESC, fit_score DESC, domain")
    bounded_limit = max(1, min(int(limit), 250))
    bounded_offset = max(0, int(offset))
    if using_classification_postgres():
        domains = set(_crawled_corpus_domains(search_db, connect_fn=connect_fn))
        if not domains:
            return []
        # Filter by the (small) bucket list in SQL, then by domain membership
        # in Python -- the reverse used to chunk the crawled-domain list
        # (which can be tens of thousands long) into repeated IN (...) round
        # trips per bucket tab; with six bucket tabs re-executed on every
        # Streamlit rerun, that alone accounted for most of a page load.
        bucket_marks = ",".join("?" for _ in bucket_values)
        with connect_store(path) as conn:
            fetched = conn.execute(
                "SELECT * FROM classifications "
                f"WHERE routing_bucket IN ({bucket_marks})",
                bucket_values,
            ).fetchall()
        rows = [row for row in fetched if str(row["domain"]) in domains]
        rows.sort(key=lambda row: row["domain"])
        # Sorting/paging across a chunked, batch-fetched result set happens in
        # Python rather than SQL here -- the corpus is a separate SQLite file,
        # so the domain filter can't be pushed into the same ORDER BY/LIMIT.
        sort_key_map = {
            "Fit: high to low": lambda r: -int(r["fit_score"] or 0),
            "Confidence: high to low": lambda r: -int(r["confidence_score"] or 0),
            "Company: A to Z": lambda r: str(r["company_name"] or "").lower(),
            "Domain: A to Z": lambda r: str(r["domain"] or ""),
            "Recently classified": lambda r: str(r["updated_at"] or ""),
        }
        if sort == "Recently classified":
            rows.sort(key=sort_key_map[sort], reverse=True)
        else:
            rows.sort(key=sort_key_map.get(
                sort,
                lambda r: -int(r["priority_score"] or 0),
            ))
        rows = rows[bounded_offset : bounded_offset + bounded_limit]
    else:
        marks = ",".join("?" for _ in bucket_values)
        params: list[object] = bucket_values + [bounded_limit, bounded_offset]
        with connect_store(path) as conn:
            conn.execute("ATTACH DATABASE ? AS corpus", (str(search_db),))
            rows = conn.execute(
                f"""
                SELECT c.*
                FROM classifications c
                JOIN corpus.domains d ON d.domain=c.domain
                WHERE d.last_crawled_at IS NOT NULL
                  AND c.routing_bucket IN ({marks})
                ORDER BY c.{order}
                LIMIT ? OFFSET ?
                """,
                params,
            ).fetchall()
    results: list[dict[str, object]] = []
    venture_fields = (
        "venture_quality_score",
        "venture_thesis_score",
        "venture_product_score",
        "venture_wedge_score",
        "venture_moat_score",
        "venture_founder_score",
        "venture_wedge_evidence",
        "venture_moat_evidence",
        "venture_founder_evidence",
    )
    for row in rows:
        result = dict(row)
        try:
            raw_record = json.loads(result.get("record_json", "") or "{}")
        except (TypeError, json.JSONDecodeError):
            raw_record = {}
        for field in venture_fields:
            result[field] = raw_record.get(field, "")
        results.append(result)
    return results


def frame_from_store(
    path: Path | str = CLASSIFICATION_DB,
) -> pd.DataFrame:
    if not using_classification_postgres() and not Path(path).exists():
        return pd.DataFrame()
    with connect_store(path, read_only=True) as conn:
        rows = conn.execute(
            "SELECT record_json FROM classifications ORDER BY domain"
        ).fetchall()
    records = []
    for row in rows:
        try:
            records.append(json.loads(row["record_json"] or "{}"))
        except (TypeError, json.JSONDecodeError):
            continue
    return pd.DataFrame(records)


def export_all_screened_csv(
    *,
    force: bool = False,
    minimum_interval_seconds: int = 300,
    path: Path | str = CLASSIFICATION_DB,
) -> pd.DataFrame | None:
    """Write the legacy master CSV at a controlled compatibility cadence."""
    target = FILTERED / "all_screened.csv"
    now = datetime.now(timezone.utc).timestamp()
    if (
        not force
        and target.exists()
        and now - target.stat().st_mtime < minimum_interval_seconds
    ):
        return None
    frame = frame_from_store(path)
    if not frame.empty:
        atomic_write_csv(target, frame)
    return frame
