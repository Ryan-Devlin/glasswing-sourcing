"""Canonical SQLite store for screening results and human decisions.

The historical CSV files remain compatibility exports, but live application
reads and writes use this database so a one-row action never requires loading
and rewriting the complete screening corpus.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from common import DATA, FILTERED, atomic_write_csv, clean, stable_domain
from db_backend import (
    PostgresConnection,
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
MODEL_VERSION = "glasswing-local-v4-venture-quality"
CLASSIFICATION_DATABASE_ENV_VARS = (
    "CLASSIFICATION_DATABASE_URL",
    "DATABASE_URL",
    "PIPELINE_DATABASE_URL",
)

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
        if not read_only:
            # This connection string is now shared by the web service and
            # the ai-worker/zone-worker cloud services too -- multiple
            # services independently connecting at once would otherwise
            # race to run this same schema DDL and deadlock on conflicting
            # locks (confirmed live for ai_search's identical pattern).
            conn.execute("SELECT pg_advisory_lock(hashtext(?))", ("classification_store_migration",))
            try:
                migrate(conn)
            finally:
                conn.execute("SELECT pg_advisory_unlock(hashtext(?))", ("classification_store_migration",))
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
                    "diligence_rank_score",
                    plain.get("rule_score", 0),
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
        "description": clean(plain.get("meta_description", "")),
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
                   description, excerpt, fit_score, thesis_pillar, updated_at
            FROM classifications
            WHERE (lower(domain) LIKE ? OR lower(company_name) LIKE ?)
              AND routing_bucket NOT IN ('ghost', 'reject', 'screened_out')
            ORDER BY fit_score DESC, updated_at DESC
            LIMIT ?
            """,
            (needle, needle, limit),
        ).fetchall()
    return [dict(row) for row in rows]


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


def _crawled_corpus_domains(search_db: Path) -> list[str]:
    """Domains with crawl evidence in a corpus DB (.AI search / Zone Universe).

    Those corpora stay on SQLite in every deployment mode for now, so this is
    a plain sqlite3 read regardless of which backend classifications live in.
    """
    with sqlite3.connect(f"file:{search_db}?mode=ro", uri=True, timeout=30) as conn:
        return [
            str(row[0])
            for row in conn.execute(
                "SELECT domain FROM domains WHERE last_crawled_at IS NOT NULL"
            )
        ]


def corpus_bucket_counts(
    search_db: Path,
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, int]:
    if not using_classification_postgres() and not Path(path).exists():
        return {}
    if not search_db.exists():
        return {}
    if using_classification_postgres():
        domains = _crawled_corpus_domains(search_db)
        if not domains:
            return {}
        counts: dict[str, int] = {}
        with connect_store(path) as conn:
            for start in range(0, len(domains), 800):
                batch = domains[start : start + 800]
                marks = ",".join("?" for _ in batch)
                for row in conn.execute(
                    "SELECT routing_bucket, count(*) AS total "
                    "FROM classifications "
                    f"WHERE domain IN ({marks}) "
                    "GROUP BY routing_bucket",
                    batch,
                ).fetchall():
                    bucket = str(row["routing_bucket"])
                    counts[bucket] = counts.get(bucket, 0) + int(row["total"])
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
) -> list[dict[str, object]]:
    bucket_values = list(dict.fromkeys(str(bucket) for bucket in buckets))
    if not bucket_values or not search_db.exists():
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
        domains = set(_crawled_corpus_domains(search_db))
        if not domains:
            return []
        rows = []
        with connect_store(path) as conn:
            for start in range(0, len(bucket_values), 800):
                bucket_batch = bucket_values[start : start + 800]
                bucket_marks = ",".join("?" for _ in bucket_batch)
                for domain_start in range(0, len(domains), 800):
                    domain_batch = list(domains)[domain_start : domain_start + 800]
                    domain_marks = ",".join("?" for _ in domain_batch)
                    rows.extend(
                        conn.execute(
                            "SELECT * FROM classifications "
                            f"WHERE domain IN ({domain_marks}) "
                            f"AND routing_bucket IN ({bucket_marks})",
                            domain_batch + bucket_batch,
                        ).fetchall()
                    )
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
