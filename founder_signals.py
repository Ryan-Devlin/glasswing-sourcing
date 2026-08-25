"""Founder Signals: public-evidence founder formation monitoring.

The module shares the canonical Glasswing classification database.  It keeps
network access behind ``GitHubClient`` so collection is mockable and never
scrapes GitHub or LinkedIn HTML.
"""

from __future__ import annotations

import csv
import base64
import io
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from common import clean, stable_domain


# Bumped when a detector is corrected rather than merely extended, because
# signals are insert-only: the rows a wrong rule already wrote outlive the fix.
# v2 corrects two of them -- focus_left_the_day_job fired on baselines of two
# and three events, and domain_in_certificate_feed matched upstream project
# homepages carried in by forks. retract_superseded_signals withdraws what v1
# wrote for exactly those two types.
SIGNAL_VERSION = "founder-signals-v2"

# The types v2 corrected, and the version whose output for them cannot be
# stood behind. Deliberately narrow: every other v1 signal was produced by a
# rule v2 did not change, and re-judging those would be a change of mind about
# evidence rather than a correction to a rule.
SUPERSEDED_SIGNALS = {
    "founder-signals-v1": (
        "focus_left_the_day_job",
        "domain_in_certificate_feed",
    ),
}
PEDIGREE_VERSION = "founder-pedigree-v1"
DEFAULT_COMPANIES = [
    "OpenAI", "Anthropic", "Google DeepMind", "Google Research", "Meta FAIR",
    "NVIDIA", "Microsoft Research", "Palantir", "Anduril", "Databricks",
    "Scale AI",
]
DEFAULT_UNIVERSITIES = [
    "MIT", "Stanford", "UC Berkeley", "Carnegie Mellon", "Harvard",
    "Princeton", "Cornell",
]
DEFAULT_CATEGORIES = [
    "Enterprise AI", "AI infrastructure", "Data infrastructure",
    "Cybersecurity", "Software supply-chain security", "Physical AI",
    "Robotics", "Autonomous systems", "Industrial technology",
    "Frontier technology", "Scientific computing", "Computational biology",
    "Semiconductor and hardware systems",
]
DEFAULT_SIGNAL_WEIGHTS = {
    "new_personal_repository": 5,
    "activity_acceleration": 8,
    "commit_step_change": 18,
    # The strongest corroboration available here: an independent system
    # watching certificate transparency saw somebody provision the domain this
    # person's repository points at.
    "domain_in_certificate_feed": 30,
    "focus_left_the_day_job": 25,
    "new_organization": 15,
    "former_colleague_contributes": 15,
    "repository_cluster": 10,
    "new_external_domain": 15,
    "commercial_language": 20,
    "founding_hiring_language": 20,
    "employer_departure": 20,
    "glasswing_category": 10,
    "dormancy_break": 10,
    "first_release": 8,
}
# Sets how quickly the transition score approaches 100. At 70, a candidate
# carrying 70 points of signal scores 63 and one carrying 200 scores 96 -- so
# the strongest cases are still separated from each other instead of all
# landing on the ceiling together.
TRANSITION_SCALE = max(10.0, float(os.getenv("FOUNDER_TRANSITION_SCALE", "70")))
DEFAULT_SCORE_WEIGHTS = {
    "talent": 0.25,
    "transition": 0.35,
    "glasswing_fit": 0.25,
    "identity": 0.10,
    "signal_confidence": 0.05,
}
ANALYST_STATUSES = {
    "new", "monitoring", "priority", "contact_now", "watch",
    "needs_research", "false_positive", "not_relevant", "archived",
}
IDENTITY_STATES = {
    "verified", "high_confidence", "needs_review", "ambiguous",
    "not_found", "rejected",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _loads(value: Any, default: Any) -> Any:
    try:
        parsed = json.loads(str(value or ""))
        return parsed if isinstance(parsed, type(default)) else default
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


# (table, column, DDL) for every column added after the initial schema. New
# columns go here as well as in the CREATE above, or they reach fresh installs
# only.
ADDITIVE_COLUMNS = (
    (
        "founder_github_repositories",
        "is_fork",
        "is_fork BOOLEAN NOT NULL DEFAULT FALSE",
    ),
)


def migrate_founder_signals(conn, *, postgres: bool = False) -> None:
    """Add the module schema without changing existing sourcing tables."""
    id_column = "id SERIAL PRIMARY KEY" if postgres else "id INTEGER PRIMARY KEY AUTOINCREMENT"
    conn.executescript(f"""
    CREATE TABLE IF NOT EXISTS founder_candidates (
        candidate_id TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        first_name TEXT NOT NULL DEFAULT '',
        last_name TEXT NOT NULL DEFAULT '',
        current_employer TEXT NOT NULL DEFAULT '',
        current_title TEXT NOT NULL DEFAULT '',
        previous_employers_json TEXT NOT NULL DEFAULT '[]',
        school TEXT NOT NULL DEFAULT '', degree TEXT NOT NULL DEFAULT '',
        research_laboratory TEXT NOT NULL DEFAULT '',
        graduation_year INTEGER NOT NULL DEFAULT 0,
        geographic_location TEXT NOT NULL DEFAULT '',
        technical_categories_json TEXT NOT NULL DEFAULT '[]',
        research_interests_json TEXT NOT NULL DEFAULT '[]',
        personal_website TEXT NOT NULL DEFAULT '',
        linkedin_url TEXT NOT NULL DEFAULT '', google_scholar_url TEXT NOT NULL DEFAULT '',
        semantic_scholar_url TEXT NOT NULL DEFAULT '', github_username TEXT NOT NULL DEFAULT '',
        github_profile_url TEXT NOT NULL DEFAULT '', social_url TEXT NOT NULL DEFAULT '',
        source_url TEXT NOT NULL DEFAULT '', source_type TEXT NOT NULL DEFAULT '',
        talent_score INTEGER NOT NULL DEFAULT 0, glasswing_fit_score INTEGER NOT NULL DEFAULT 0,
        founder_transition_score INTEGER NOT NULL DEFAULT 0, overall_priority_score INTEGER NOT NULL DEFAULT 0,
        identity_confidence INTEGER NOT NULL DEFAULT 0, signal_confidence INTEGER NOT NULL DEFAULT 0,
        monitoring_status TEXT NOT NULL DEFAULT 'inactive', analyst_status TEXT NOT NULL DEFAULT 'new',
        analyst_owner TEXT NOT NULL DEFAULT '', next_review_at TEXT NOT NULL DEFAULT '',
        outreach_completed_at TEXT NOT NULL DEFAULT '', outreach_outcome TEXT NOT NULL DEFAULT '',
        muted_until TEXT NOT NULL DEFAULT '', archived_at TEXT NOT NULL DEFAULT '',
        synthetic BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, last_verified_at TEXT NOT NULL DEFAULT '',
        last_refreshed_at TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX IF NOT EXISTS idx_founder_candidates_priority ON founder_candidates(overall_priority_score DESC, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_founder_candidates_status ON founder_candidates(analyst_status, monitoring_status);
    CREATE INDEX IF NOT EXISTS idx_founder_candidates_github ON founder_candidates(github_username);

    CREATE TABLE IF NOT EXISTS founder_affiliations (
        affiliation_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL,
        organization TEXT NOT NULL, role TEXT NOT NULL DEFAULT '', affiliation_type TEXT NOT NULL DEFAULT '',
        started_at TEXT NOT NULL DEFAULT '', ended_at TEXT NOT NULL DEFAULT '',
        source_url TEXT NOT NULL DEFAULT '', confidence INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL, UNIQUE(candidate_id,organization,role,started_at),
        FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_public_profiles (
        profile_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, profile_type TEXT NOT NULL,
        profile_url TEXT NOT NULL, username TEXT NOT NULL DEFAULT '', verified BOOLEAN NOT NULL DEFAULT FALSE,
        last_checked_at TEXT NOT NULL DEFAULT '', profile_json TEXT NOT NULL DEFAULT '{{}}',
        UNIQUE(candidate_id,profile_type,profile_url), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_sources (
        source_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, source_type TEXT NOT NULL,
        source_url TEXT NOT NULL, raw_source_name TEXT NOT NULL DEFAULT '', extraction_method TEXT NOT NULL DEFAULT '',
        match_confidence INTEGER NOT NULL DEFAULT 0, discovered_at TEXT NOT NULL,
        UNIQUE(candidate_id,source_url), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_founder_sources_url ON founder_sources(source_url);
    CREATE TABLE IF NOT EXISTS founder_pedigree_evidence (
        evidence_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, provider TEXT NOT NULL,
        evidence_type TEXT NOT NULL, label TEXT NOT NULL, evidence TEXT NOT NULL DEFAULT '',
        source_url TEXT NOT NULL, strength INTEGER NOT NULL DEFAULT 0,
        confidence INTEGER NOT NULL DEFAULT 0, verification_status TEXT NOT NULL DEFAULT 'needs_review',
        external_id TEXT NOT NULL DEFAULT '', raw_json TEXT NOT NULL DEFAULT '{{}}',
        checked_at TEXT NOT NULL, rule_version TEXT NOT NULL,
        UNIQUE(candidate_id,provider,evidence_type,external_id),
        FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_founder_pedigree_candidate ON founder_pedigree_evidence(candidate_id,verification_status,confidence DESC);
    CREATE TABLE IF NOT EXISTS github_identity_candidates (
        identity_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, username TEXT NOT NULL,
        profile_url TEXT NOT NULL DEFAULT '', positive_score INTEGER NOT NULL DEFAULT 0,
        confidence_score INTEGER NOT NULL DEFAULT 0, evidence_json TEXT NOT NULL DEFAULT '[]',
        contradictions_json TEXT NOT NULL DEFAULT '[]', resolution_status TEXT NOT NULL DEFAULT 'needs_review',
        resolution_method TEXT NOT NULL DEFAULT 'automatic', resolved_at TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
        UNIQUE(candidate_id,username), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_github_identity_review ON github_identity_candidates(resolution_status,confidence_score DESC);
    CREATE TABLE IF NOT EXISTS verified_github_identities (
        candidate_id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, github_user_id TEXT NOT NULL DEFAULT '',
        confidence_score INTEGER NOT NULL, verification_method TEXT NOT NULL,
        verified_at TEXT NOT NULL, last_collected_at TEXT NOT NULL DEFAULT '', etag TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_github_events (
        event_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, username TEXT NOT NULL,
        event_type TEXT NOT NULL, repo_name TEXT NOT NULL DEFAULT '', organization TEXT NOT NULL DEFAULT '',
        event_created_at TEXT NOT NULL, collected_at TEXT NOT NULL, payload_json TEXT NOT NULL DEFAULT '{{}}',
        source_url TEXT NOT NULL DEFAULT '', FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_founder_events_candidate_time ON founder_github_events(candidate_id,event_created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_founder_events_type_time ON founder_github_events(event_type,event_created_at DESC);
    CREATE TABLE IF NOT EXISTS founder_github_repositories (
        repository_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, full_name TEXT NOT NULL,
        owner_login TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', topics_json TEXT NOT NULL DEFAULT '[]',
        primary_language TEXT NOT NULL DEFAULT '', readme_excerpt TEXT NOT NULL DEFAULT '', homepage TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT '', first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL, archived BOOLEAN NOT NULL DEFAULT FALSE,
        is_fork BOOLEAN NOT NULL DEFAULT FALSE,
        UNIQUE(candidate_id,full_name), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_github_organizations (
        organization_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, login TEXT NOT NULL,
        profile_url TEXT NOT NULL DEFAULT '', first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
        UNIQUE(candidate_id,login), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_public_organization_memberships (
        membership_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, organization_login TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member', public BOOLEAN NOT NULL DEFAULT TRUE,
        first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
        UNIQUE(candidate_id,organization_login), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_repository_contributors (
        repository_id TEXT NOT NULL, contributor_login TEXT NOT NULL, contributions INTEGER NOT NULL DEFAULT 0,
        first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, PRIMARY KEY(repository_id,contributor_login)
    );
    CREATE TABLE IF NOT EXISTS founder_relationships (
        relationship_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, collaborator_candidate_id TEXT NOT NULL DEFAULT '',
        collaborator_name TEXT NOT NULL DEFAULT '', collaborator_username TEXT NOT NULL DEFAULT '',
        relationship_type TEXT NOT NULL, evidence_json TEXT NOT NULL DEFAULT '[]', strength INTEGER NOT NULL DEFAULT 0,
        first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
        UNIQUE(candidate_id,collaborator_username,relationship_type), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_founder_relationships_candidate ON founder_relationships(candidate_id,strength DESC);
    CREATE TABLE IF NOT EXISTS founder_activity_daily (
        candidate_id TEXT NOT NULL, metric_date TEXT NOT NULL, metrics_json TEXT NOT NULL,
        total_activity INTEGER NOT NULL DEFAULT 0, active_repositories INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(candidate_id,metric_date), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_activity_monthly (
        candidate_id TEXT NOT NULL, metric_month TEXT NOT NULL, metrics_json TEXT NOT NULL,
        total_activity INTEGER NOT NULL DEFAULT 0, active_days INTEGER NOT NULL DEFAULT 0,
        baseline_status TEXT NOT NULL DEFAULT 'insufficient', PRIMARY KEY(candidate_id,metric_month),
        FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_signals (
        signal_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, signal_type TEXT NOT NULL,
        detected_at TEXT NOT NULL, event_at TEXT NOT NULL, evidence TEXT NOT NULL,
        source_url TEXT NOT NULL DEFAULT '', strength INTEGER NOT NULL, confidence INTEGER NOT NULL,
        explanation TEXT NOT NULL, rule_version TEXT NOT NULL, analyst_disposition TEXT NOT NULL DEFAULT '',
        dedupe_key TEXT NOT NULL UNIQUE, FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_founder_signals_candidate_time ON founder_signals(candidate_id,event_at DESC);
    CREATE INDEX IF NOT EXISTS idx_founder_signals_type_time ON founder_signals(signal_type,event_at DESC);
    CREATE TABLE IF NOT EXISTS founder_score_snapshots (
        snapshot_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, talent_score INTEGER NOT NULL,
        transition_score INTEGER NOT NULL, glasswing_fit_score INTEGER NOT NULL, identity_confidence INTEGER NOT NULL,
        signal_confidence INTEGER NOT NULL, overall_score INTEGER NOT NULL, explanation_json TEXT NOT NULL,
        score_version TEXT NOT NULL, calculated_at TEXT NOT NULL, FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE INDEX IF NOT EXISTS idx_founder_scores_candidate_time ON founder_score_snapshots(candidate_id,calculated_at DESC);
    CREATE TABLE IF NOT EXISTS founder_analyst_reviews (
        {id_column}, candidate_id TEXT NOT NULL, action TEXT NOT NULL, previous_value TEXT NOT NULL DEFAULT '',
        new_value TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '', analyst TEXT NOT NULL DEFAULT 'admin',
        created_at TEXT NOT NULL, FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_monitoring_jobs (
        job_id TEXT PRIMARY KEY, job_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
        candidate_id TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '', completed_at TEXT NOT NULL DEFAULT '',
        attempted INTEGER NOT NULL DEFAULT 0, succeeded INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0,
        detail_json TEXT NOT NULL DEFAULT '{{}}', error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_founder_jobs_status ON founder_monitoring_jobs(status,created_at);
    CREATE TABLE IF NOT EXISTS founder_collection_errors (
        {id_column}, job_id TEXT NOT NULL DEFAULT '', candidate_id TEXT NOT NULL DEFAULT '', source TEXT NOT NULL,
        error_type TEXT NOT NULL, message TEXT NOT NULL, retryable BOOLEAN NOT NULL DEFAULT TRUE,
        occurred_at TEXT NOT NULL, resolved_at TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS founder_profile_changes (
        change_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, profile_type TEXT NOT NULL,
        field_name TEXT NOT NULL, previous_value TEXT NOT NULL DEFAULT '', new_value TEXT NOT NULL DEFAULT '',
        detected_at TEXT NOT NULL, source_url TEXT NOT NULL DEFAULT '', confidence INTEGER NOT NULL DEFAULT 0,
        UNIQUE(candidate_id,profile_type,field_name,new_value), FOREIGN KEY(candidate_id) REFERENCES founder_candidates(candidate_id)
    );
    CREATE TABLE IF NOT EXISTS founder_settings (
        setting_key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by TEXT NOT NULL DEFAULT 'system'
    );
    """)

    # Columns added after a database already exists. CREATE TABLE IF NOT
    # EXISTS is a no-op on an existing table, so every column added to one of
    # the definitions above lands only on fresh installs and silently never on
    # production. That is not theoretical: is_fork was added to
    # founder_github_repositories and then read by certificate_feed_matches,
    # which raised UndefinedColumn 693 times over four days and took every
    # founder signal down with it while events and repositories kept being
    # written, so the tables looked healthy the whole time.
    for table, column, definition in ADDITIVE_COLUMNS:
        try:
            if postgres:
                from db_backend import table_columns_postgres

                existing = table_columns_postgres(conn, table)
            else:
                existing = {
                    str(row["name"])
                    for row in conn.execute(f"PRAGMA table_info({table})")
                }
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
        except Exception:
            # A missing table here means the CREATE above has not run yet on
            # this connection, which the next call will handle.
            continue

    defaults = {
        "target_companies": DEFAULT_COMPANIES,
        "target_universities": DEFAULT_UNIVERSITIES,
        "target_laboratories": [],
        "technical_categories": DEFAULT_CATEGORIES,
        "signal_weights": DEFAULT_SIGNAL_WEIGHTS,
        "score_weights": DEFAULT_SCORE_WEIGHTS,
        "minimum_alert_threshold": 60,
        "collection_frequency_hours": 24,
        "maximum_people_per_run": 200,
        "automatic_monitoring_confidence": 90,
        "pedigree_minimum_score": 75,
        "pedigree_minimum_confidence": 80,
        "event_retention_days": 0,
        "notification_preferences": {"dashboard": True, "daily": False, "weekly": True, "immediate_threshold": 85},
    }
    now = utc_now()
    for key, value in defaults.items():
        conn.execute(
            "INSERT INTO founder_settings(setting_key,value_json,updated_at,updated_by) VALUES (?,?,?,?) "
            "ON CONFLICT(setting_key) DO NOTHING",
            (key, _json(value), now, "system"),
        )
    conn.commit()


def _store(read_only: bool = False):
    import classification_store
    return classification_store.connect_store(
        classification_store.CLASSIFICATION_DB,
        read_only=read_only,
    )


def founder_settings() -> dict[str, Any]:
    with _store(True) as conn:
        rows = conn.execute("SELECT setting_key,value_json FROM founder_settings").fetchall()
    return {str(row["setting_key"]): _loads(row["value_json"], {}) if str(row["value_json"]).startswith("{") else _loads(row["value_json"], []) if str(row["value_json"]).startswith("[") else json.loads(str(row["value_json"])) for row in rows}


def update_settings(values: dict[str, Any], *, actor: str = "admin") -> dict[str, Any]:
    allowed = {
        "target_companies", "target_universities", "target_laboratories", "technical_categories",
        "signal_weights", "score_weights", "minimum_alert_threshold", "collection_frequency_hours",
        "maximum_people_per_run", "automatic_monitoring_confidence", "pedigree_minimum_score",
        "pedigree_minimum_confidence", "event_retention_days",
        "notification_preferences",
    }
    now = utc_now()
    with _store() as conn:
        for key, value in values.items():
            if key not in allowed:
                continue
            conn.execute(
                "INSERT INTO founder_settings(setting_key,value_json,updated_at,updated_by) VALUES (?,?,?,?) "
                "ON CONFLICT(setting_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at,updated_by=excluded.updated_by",
                (key, _json(value), now, actor),
            )
        conn.commit()
    return founder_settings()


def _name_parts(full_name: str) -> tuple[str, str]:
    parts = clean(full_name).split()
    return (parts[0] if parts else "", parts[-1] if len(parts) > 1 else "")


def _normalized_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", clean(value).lower())


def dedupe_candidates(conn, person: dict[str, Any]) -> list[dict[str, Any]]:
    """Return possible matches; callers never silently merge ambiguity."""
    name = _normalized_name(person.get("full_name", ""))
    github = clean(person.get("github_username", "")).lower().lstrip("@")
    website = stable_domain(person.get("personal_website", ""))
    rows = conn.execute("SELECT * FROM founder_candidates WHERE archived_at='' OR archived_at IS NULL").fetchall()
    matches = []
    for raw in rows:
        row = dict(raw)
        reasons = []
        if github and github == clean(row.get("github_username", "")).lower():
            reasons.append("same GitHub username")
        if website and website == stable_domain(row.get("personal_website", "")):
            reasons.append("same personal website")
        if name and name == _normalized_name(row.get("full_name", "")):
            employer = clean(person.get("current_employer", "")).lower()
            school = clean(person.get("school", "")).lower()
            if employer and employer == clean(row.get("current_employer", "")).lower():
                reasons.append("same name and employer")
            if school and school == clean(row.get("school", "")).lower():
                reasons.append("same name and school")
        if reasons:
            matches.append({"candidate_id": row["candidate_id"], "full_name": row["full_name"], "reasons": reasons})
    return matches


def create_candidate(person: dict[str, Any], *, allow_possible_duplicate: bool = False) -> dict[str, Any]:
    full_name = clean(person.get("full_name", ""))
    if not full_name:
        raise ValueError("Full name is required")
    now = utc_now()
    with _store() as conn:
        duplicates = dedupe_candidates(conn, person)
        if duplicates and not allow_possible_duplicate:
            return {"status": "needs_review", "duplicates": duplicates}
        candidate_id = clean(person.get("candidate_id", "")) or str(uuid.uuid4())
        first, last = _name_parts(full_name)
        fields = {
            "candidate_id": candidate_id, "full_name": full_name,
            "first_name": clean(person.get("first_name", "")) or first,
            "last_name": clean(person.get("last_name", "")) or last,
            "current_employer": clean(person.get("current_employer", "")),
            "current_title": clean(person.get("current_title", "")),
            "previous_employers_json": _json(person.get("previous_employers", [])),
            "school": clean(person.get("school", "")), "degree": clean(person.get("degree", "")),
            "research_laboratory": clean(person.get("research_laboratory", "")),
            "graduation_year": int(person.get("graduation_year") or 0),
            "geographic_location": clean(person.get("geographic_location", "")),
            "technical_categories_json": _json(person.get("technical_categories", [])),
            "research_interests_json": _json(person.get("research_interests", [])),
            "personal_website": clean(person.get("personal_website", "")),
            "linkedin_url": clean(person.get("linkedin_url", "")),
            "google_scholar_url": clean(person.get("google_scholar_url", "")),
            "semantic_scholar_url": clean(person.get("semantic_scholar_url", "")),
            "github_username": clean(person.get("github_username", "")).lstrip("@"),
            "github_profile_url": clean(person.get("github_profile_url", "")),
            "social_url": clean(person.get("social_url", "")),
            "source_url": clean(person.get("source_url", "")),
            "source_type": clean(person.get("source_type", "")) or "manual",
            "talent_score": max(0, min(100, int(person.get("talent_score") or 0))),
            "glasswing_fit_score": max(0, min(100, int(person.get("glasswing_fit_score") or 0))),
            "identity_confidence": max(0, min(100, int(person.get("identity_confidence") or 0))),
            "monitoring_status": clean(person.get("monitoring_status", "")) or "inactive",
            "analyst_status": clean(person.get("analyst_status", "")) or "new",
            "synthetic": bool(person.get("synthetic", False)), "created_at": now, "updated_at": now,
        }
        columns = ",".join(fields); marks = ",".join("?" for _ in fields)
        conn.execute(f"INSERT INTO founder_candidates({columns}) VALUES ({marks})", tuple(fields.values()))
        if fields["source_url"]:
            conn.execute(
                "INSERT INTO founder_sources(source_id,candidate_id,source_type,source_url,raw_source_name,extraction_method,match_confidence,discovered_at) VALUES (?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), candidate_id, fields["source_type"], fields["source_url"], full_name, clean(person.get("extraction_method", "manual")), int(person.get("match_confidence") or 100), now),
            )
        conn.commit()
    return {"status": "created", "candidate_id": candidate_id}


FIELD_ALIASES = {
    "name": "full_name", "company": "current_employer", "employer": "current_employer",
    "title": "current_title", "location": "geographic_location", "university": "school",
    "github": "github_username", "website": "personal_website", "linkedin": "linkedin_url",
    "category": "technical_categories",
}


def import_people(content: bytes, filename: str) -> dict[str, Any]:
    """Import CSV/XLSX with row-level errors and conservative deduplication."""
    if filename.lower().endswith(".xlsx"):
        import pandas as pd
        frame = pd.read_excel(io.BytesIO(content), dtype=str).fillna("")
        rows = frame.to_dict(orient="records")
    elif filename.lower().endswith(".csv"):
        rows = list(csv.DictReader(io.StringIO(content.decode("utf-8-sig"))))
    else:
        raise ValueError("Upload a .csv or .xlsx file")
    created = 0; review = 0; errors = []
    for index, raw in enumerate(rows, start=2):
        person: dict[str, Any] = {}
        for key, value in raw.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", clean(key).lower()).strip("_")
            normalized = FIELD_ALIASES.get(normalized, normalized)
            if normalized in {"technical_categories", "previous_employers", "research_interests"}:
                person[normalized] = [clean(item) for item in re.split(r"[;,|]", clean(value)) if clean(item)]
            else:
                person[normalized] = value
        try:
            result = create_candidate(person)
            created += result["status"] == "created"
            review += result["status"] == "needs_review"
        except Exception as exc:
            errors.append({"row": index, "error": str(exc)})
    return {"rows": len(rows), "created": created, "identity_review": review, "errors": errors}


def _candidate_dict(row: Any) -> dict[str, Any]:
    value = dict(row)
    for source, target in (
        ("previous_employers_json", "previous_employers"),
        ("technical_categories_json", "technical_categories"),
        ("research_interests_json", "research_interests"),
    ):
        value[target] = _loads(value.pop(source, "[]"), [])
    value["confidence_level"] = "high" if value["identity_confidence"] >= 90 else "medium" if value["identity_confidence"] >= 70 else "low"
    return value


def list_candidates(*, query: str = "", status: str = "", employer: str = "", category: str = "", monitoring: str = "", minimum_score: int = 0, sort: str = "priority", limit: int = 100, offset: int = 0) -> dict[str, Any]:
    clauses = ["archived_at=''", "overall_priority_score>=?"]; params: list[Any] = [minimum_score]
    if query:
        clauses.append("(lower(full_name) LIKE ? OR lower(current_employer) LIKE ? OR lower(github_username) LIKE ?)")
        term = f"%{query.lower()}%"; params.extend([term, term, term])
    if status: clauses.append("analyst_status=?"); params.append(status)
    if employer: clauses.append("lower(current_employer)=?"); params.append(employer.lower())
    if category: clauses.append("lower(technical_categories_json) LIKE ?"); params.append(f"%{category.lower()}%")
    if monitoring: clauses.append("monitoring_status=?"); params.append(monitoring)
    order = {
        "recent": "updated_at DESC", "signal": "last_refreshed_at DESC",
        "transition": "founder_transition_score DESC", "talent": "talent_score DESC",
        "fit": "glasswing_fit_score DESC",
    }.get(sort, "overall_priority_score DESC,updated_at DESC")
    where = " AND ".join(clauses)
    with _store(True) as conn:
        total = int(conn.execute(f"SELECT count(*) AS total FROM founder_candidates WHERE {where}", params).fetchone()["total"])
        rows = conn.execute(f"SELECT * FROM founder_candidates WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?", [*params, limit, offset]).fetchall()
    return {"rows": [_candidate_dict(row) for row in rows], "total": total}


def update_candidate(candidate_id: str, values: dict[str, Any]) -> dict[str, Any]:
    scalar = {"full_name", "first_name", "last_name", "current_employer", "current_title", "school", "degree", "research_laboratory", "graduation_year", "geographic_location", "personal_website", "linkedin_url", "google_scholar_url", "semantic_scholar_url", "social_url", "source_url", "source_type", "talent_score", "glasswing_fit_score", "identity_confidence"}
    list_fields = {"previous_employers": "previous_employers_json", "technical_categories": "technical_categories_json", "research_interests": "research_interests_json"}
    updates: dict[str, Any] = {}
    for key, value in values.items():
        if key in scalar: updates[key] = value
        elif key in list_fields: updates[list_fields[key]] = _json(value if isinstance(value, list) else [])
    if not updates: return candidate_profile(candidate_id) or {}
    for key in {"talent_score", "glasswing_fit_score", "identity_confidence"} & updates.keys(): updates[key] = max(0, min(100, int(updates[key] or 0)))
    with _store() as conn:
        assignments = ",".join(f"{key}=?" for key in updates)
        conn.execute(f"UPDATE founder_candidates SET {assignments},updated_at=? WHERE candidate_id=?", (*updates.values(), utc_now(), candidate_id)); conn.commit()
    recalculate_scores(candidate_id)
    return candidate_profile(candidate_id) or {}


def identity_review_queue(limit: int = 100) -> list[dict[str, Any]]:
    with _store(True) as conn:
        rows = conn.execute("SELECT i.*,c.full_name,c.current_employer,c.current_title,c.school FROM github_identity_candidates i JOIN founder_candidates c ON c.candidate_id=i.candidate_id WHERE i.resolution_status IN ('needs_review','ambiguous') ORDER BY i.confidence_score DESC,i.created_at DESC LIMIT ?", (limit,)).fetchall()
    output = []
    for row in rows:
        item = dict(row); item["evidence"] = _loads(item.pop("evidence_json", "[]"), []); item["contradictions"] = _loads(item.pop("contradictions_json", "[]"), []); output.append(item)
    return output


def monitoring_jobs(limit: int = 50) -> list[dict[str, Any]]:
    with _store(True) as conn:
        return [dict(row) for row in conn.execute("SELECT * FROM founder_monitoring_jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()]


def candidate_profile(candidate_id: str) -> dict[str, Any] | None:
    with _store(True) as conn:
        row = conn.execute("SELECT * FROM founder_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
        if not row: return None
        result = _candidate_dict(row)
        tables = {
            "sources": ("founder_sources", "discovered_at DESC"),
            "identities": ("github_identity_candidates", "confidence_score DESC"),
            "signals": ("founder_signals", "event_at DESC"),
            "relationships": ("founder_relationships", "strength DESC"),
            "scores": ("founder_score_snapshots", "calculated_at DESC"),
            "activity_daily": ("founder_activity_daily", "metric_date DESC"),
            "activity_monthly": ("founder_activity_monthly", "metric_month DESC"),
            "reviews": ("founder_analyst_reviews", "created_at DESC"),
            "pedigree": ("founder_pedigree_evidence", "confidence DESC, strength DESC"),
        }
        for key, (table, order) in tables.items():
            result[key] = [dict(item) for item in conn.execute(f"SELECT * FROM {table} WHERE candidate_id=? ORDER BY {order} LIMIT 200", (candidate_id,)).fetchall()]
    for item in result["signals"]:
        item["confirmed"] = item.get("analyst_disposition") == "confirmed"
    return result


def identity_confidence(person: dict[str, Any], profile: dict[str, Any]) -> tuple[int, list[str], list[str]]:
    """Explainable public-profile match score; contradictions block auto-accept."""
    evidence: list[str] = []; contradictions: list[str] = []; score = 0
    login = clean(profile.get("login", ""))
    profile_name = clean(profile.get("name", ""))
    expected_name = clean(person.get("full_name", ""))
    if profile.get("verified_cross_link"):
        score += 100; evidence.append("Exact cross-link from a verified public profile")
    elif expected_name and profile_name and _normalized_name(expected_name) == _normalized_name(profile_name):
        score += 40; evidence.append("Full name matches")
    elif expected_name and profile_name and all(part.lower() in profile_name.lower() for part in expected_name.split()):
        score += 28; evidence.append("Name components match")
    employer = clean(person.get("current_employer", ""))
    company = clean(profile.get("company", ""))
    if employer and company:
        if employer.lower() in company.lower() or company.lower() in employer.lower():
            score += 22; evidence.append("Employer matches GitHub profile")
        elif len(company) > 2:
            contradictions.append(f"Employer differs: GitHub says {company}")
    school = clean(person.get("school", ""))
    bio = clean(profile.get("bio", ""))
    if school and school.lower() in bio.lower():
        score += 16; evidence.append("School matches GitHub biography")
    website = stable_domain(person.get("personal_website", ""))
    blog = stable_domain(profile.get("blog", ""))
    if website and blog:
        if website == blog:
            score += 35; evidence.append("Personal website matches")
        else:
            contradictions.append("Personal website does not match")
    location = clean(person.get("geographic_location", ""))
    github_location = clean(profile.get("location", ""))
    if location and github_location and (
        location.lower() in github_location.lower() or github_location.lower() in location.lower()
    ):
        score += 8; evidence.append("Location matches")
    if login and login.lower() in {clean(person.get("github_username", "")).lower(), clean(person.get("suggested_username", "")).lower()}:
        score += 20; evidence.append("Username matches supplied profile")
    if contradictions:
        score = min(score, 89)
    return min(100, score), evidence, contradictions


def identity_state(score: int, contradictions: list[str], candidate_count: int = 1, exact_cross_link: bool = False) -> str:
    if candidate_count > 1 and score < 90:
        return "ambiguous"
    if exact_cross_link and not contradictions:
        return "verified"
    if score >= 90 and not contradictions:
        return "verified"
    if score >= 70:
        return "needs_review"
    return "not_found"


class GitHubError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, retryable: bool = True):
        super().__init__(message); self.status = status; self.retryable = retryable


class GitHubClient:
    """Small official-API client with pagination, retries and rate awareness."""
    api_root = "https://api.github.com"

    def __init__(self, token: str | None = None, *, transport=None, timeout: float = 15.0, retries: int = 3):
        self.token = token if token is not None else os.getenv("GITHUB_TOKEN", "")
        self.transport = transport or self._urlopen
        self.timeout = timeout; self.retries = retries

    def _urlopen(self, url: str, headers: dict[str, str]):
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return response.status, dict(response.headers), response.read()

    def request(self, path: str) -> tuple[Any, dict[str, str]]:
        if not self.token:
            raise GitHubError("GITHUB_TOKEN is not configured", retryable=False)
        url = path if path.startswith("http") else f"{self.api_root}{path}"
        headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {self.token}", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Glasswing-Founder-Signals"}
        for attempt in range(self.retries + 1):
            try:
                status, response_headers, body = self.transport(url, headers)
                if status >= 400:
                    raise GitHubError(f"GitHub returned HTTP {status}", status=status, retryable=status in {429, 500, 502, 503, 504})
                return json.loads(body.decode("utf-8")), {str(k).lower(): str(v) for k, v in response_headers.items()}
            except urllib.error.HTTPError as exc:
                retryable = exc.code in {403, 429, 500, 502, 503, 504}
                if not retryable or attempt == self.retries:
                    raise GitHubError(f"GitHub returned HTTP {exc.code}", status=exc.code, retryable=retryable) from exc
                retry_after = float(exc.headers.get("Retry-After", 2 ** attempt))
                time.sleep(min(retry_after, 30))
            except (urllib.error.URLError, TimeoutError, GitHubError) as exc:
                if isinstance(exc, GitHubError) and not exc.retryable:
                    raise
                if attempt == self.retries:
                    raise GitHubError(str(exc)) from exc
                time.sleep(min(2 ** attempt, 8))
        raise GitHubError("GitHub request failed")

    def paginated(self, path: str, *, maximum_pages: int = 10) -> list[Any]:
        separator = "&" if "?" in path else "?"; url = f"{path}{separator}per_page=100"
        output: list[Any] = []
        for _ in range(maximum_pages):
            payload, headers = self.request(url)
            output.extend(payload if isinstance(payload, list) else payload.get("items", []))
            link = headers.get("link", "")
            match = re.search(r'<([^>]+)>; rel="next"', link)
            if not match: break
            url = match.group(1)
        return output


class PublicResearchClient:
    """Free structured research sources; transport injection keeps it testable."""
    def __init__(self, *, transport=None, timeout: float = 15.0):
        self.transport = transport or self._urlopen
        self.timeout = timeout

    def _urlopen(self, url: str, headers: dict[str, str]):
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return response.status, dict(response.headers), response.read()

    def _get(self, url: str) -> dict[str, Any]:
        headers = {"Accept": "application/json", "User-Agent": "Glasswing-Founder-Signals/1.0 (public research enrichment)"}
        status, _, body = self.transport(url, headers)
        if status >= 400:
            raise RuntimeError(f"Public research provider returned HTTP {status}")
        value = json.loads(body.decode("utf-8"))
        return value if isinstance(value, dict) else {}

    def openalex_authors(self, name: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"search": name, "per_page": 5})
        return list(self._get(f"https://api.openalex.org/authors?{query}").get("results") or [])

    def semantic_scholar_authors(self, name: str) -> list[dict[str, Any]]:
        fields = "name,url,affiliations,paperCount,citationCount,hIndex,externalIds,papers.title,papers.year,papers.citationCount"
        query = urllib.parse.urlencode({"query": name, "limit": 5, "fields": fields})
        return list(self._get(f"https://api.semanticscholar.org/graph/v1/author/search?{query}").get("data") or [])

    def crossref_works(self, name: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"query.author": name, "rows": 20, "select": "DOI,title,author,is-referenced-by-count,published"})
        return list((self._get(f"https://api.crossref.org/works?{query}").get("message") or {}).get("items") or [])


def _name_is_exact(expected: str, actual: str) -> bool:
    return bool(expected and actual and _normalized_name(expected) == _normalized_name(actual))


def _target_overlap(candidate: dict[str, Any], values: Iterable[str]) -> list[str]:
    haystack = " ".join(clean(value).lower() for value in values if value)
    targets = [candidate.get("current_employer", ""), candidate.get("school", ""), candidate.get("research_laboratory", "")]
    return [clean(target) for target in targets if clean(target) and clean(target).lower() in haystack]


def enrich_pedigree(candidate_id: str, *, client: PublicResearchClient | None = None) -> dict[str, Any]:
    """Find cited pedigree evidence without treating a name-only hit as verified."""
    client = client or PublicResearchClient()
    settings = founder_settings()
    minimum_confidence = int(settings.get("pedigree_minimum_confidence", 80))
    with _store(True) as conn:
        row = conn.execute("SELECT * FROM founder_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
        if not row: raise ValueError("Candidate not found")
        candidate = dict(row)
    name = clean(candidate.get("full_name", ""))
    if not name: raise ValueError("Candidate has no full name")
    now = utc_now(); evidence_rows: list[dict[str, Any]] = []; provider_errors: list[dict[str, str]] = []

    try: openalex_authors = client.openalex_authors(name)
    except Exception as exc:
        openalex_authors = []; provider_errors.append({"provider":"openalex","error":str(exc)[:300]})
    for author in openalex_authors:
        if not _name_is_exact(name, clean(author.get("display_name", ""))): continue
        institutions = [clean(item.get("display_name", "")) for item in (author.get("last_known_institutions") or [])]
        for affiliation in author.get("affiliations") or []:
            institution = affiliation.get("institution") or {}
            if institution.get("display_name"): institutions.append(clean(institution["display_name"]))
        overlaps = _target_overlap(candidate, institutions)
        orcid = clean((author.get("ids") or {}).get("orcid", ""))
        confidence = min(96, 52 + (30 if overlaps else 0) + (14 if orcid else 0))
        author_id = clean(author.get("id", "")); works = int(author.get("works_count") or 0); citations = int(author.get("cited_by_count") or 0)
        evidence_rows.append({"provider":"openalex","external_id":author_id,"url":author_id,"type":"research_record","label":f"{works} publications · {citations:,} citations","evidence":f"Exact name match. Institutions: {', '.join(dict.fromkeys(institutions)) or 'not supplied'}." + (f" Matched: {', '.join(overlaps)}." if overlaps else ""),"strength":min(100, round(math.log1p(works)*10 + math.log1p(citations)*9)),"confidence":confidence,"verified":confidence >= minimum_confidence,"raw":author})

    try: semantic_authors = client.semantic_scholar_authors(name)
    except Exception as exc:
        semantic_authors = []; provider_errors.append({"provider":"semantic_scholar","error":str(exc)[:300]})
    for author in semantic_authors:
        if not _name_is_exact(name, clean(author.get("name", ""))): continue
        affiliations = [clean(value) for value in (author.get("affiliations") or [])]
        overlaps = _target_overlap(candidate, affiliations)
        orcid = clean((author.get("externalIds") or {}).get("ORCID", ""))
        confidence = min(96, 52 + (30 if overlaps else 0) + (14 if orcid else 0))
        author_id = clean(author.get("authorId", "")); papers = int(author.get("paperCount") or 0); citations = int(author.get("citationCount") or 0); h_index = int(author.get("hIndex") or 0)
        url = clean(author.get("url", "")) or f"https://www.semanticscholar.org/author/{author_id}"
        evidence_rows.append({"provider":"semantic_scholar","external_id":author_id,"url":url,"type":"research_record","label":f"h-index {h_index} · {papers} publications · {citations:,} citations","evidence":f"Exact name match. Affiliations: {', '.join(affiliations) or 'not supplied'}." + (f" Matched: {', '.join(overlaps)}." if overlaps else ""),"strength":min(100, h_index*4 + round(math.log1p(citations)*7)),"confidence":confidence,"verified":confidence >= minimum_confidence,"raw":author})

    try: crossref_works = client.crossref_works(name)
    except Exception as exc:
        crossref_works = []; provider_errors.append({"provider":"crossref","error":str(exc)[:300]})
    matched_works = []; crossref_affiliations = []; crossref_orcid = ""
    for work in crossref_works:
        for author in work.get("author") or []:
            author_name = clean(f'{author.get("given", "")} {author.get("family", "")}')
            if not _name_is_exact(name, author_name): continue
            matched_works.append(work)
            crossref_orcid = crossref_orcid or clean(author.get("ORCID", ""))
            crossref_affiliations.extend(clean(item.get("name", "")) for item in (author.get("affiliation") or []) if item.get("name"))
            break
    if matched_works:
        overlaps = _target_overlap(candidate, crossref_affiliations)
        confidence = min(96, 52 + (30 if overlaps else 0) + (14 if crossref_orcid else 0))
        citations = sum(int(work.get("is-referenced-by-count") or 0) for work in matched_works)
        external_id = crossref_orcid.replace("https://orcid.org/", "") or _normalized_name(name)
        url = crossref_orcid or f'https://search.crossref.org/?q={urllib.parse.quote(name)}'
        raw = {"ORCID":crossref_orcid,"works":matched_works[:20],"affiliations":crossref_affiliations}
        evidence_rows.append({"provider":"crossref","external_id":external_id,"url":url,"type":"publication_record","label":f"{len(matched_works)} matched works · {citations:,} citations","evidence":f"Exact author-name matches. Affiliations: {', '.join(dict.fromkeys(crossref_affiliations)) or 'not supplied'}." + (f" Matched: {', '.join(overlaps)}." if overlaps else ""),"strength":min(100,round(math.log1p(len(matched_works))*15+math.log1p(citations)*8)),"confidence":confidence,"verified":confidence >= minimum_confidence,"raw":raw})

    # Two independent academic graphs agreeing on ORCID upgrades both records.
    orcids = defaultdict(list)
    for item in evidence_rows:
        raw_ids = (item["raw"].get("ids") or {}) if item["provider"] == "openalex" else (item["raw"].get("externalIds") or {}) if item["provider"] == "semantic_scholar" else item["raw"]
        orcid = clean(raw_ids.get("orcid", "") or raw_ids.get("ORCID", "")).replace("https://orcid.org/", "")
        if orcid: orcids[orcid].append(item)
    for agreeing in orcids.values():
        if len({item["provider"] for item in agreeing}) >= 2:
            for item in agreeing: item["confidence"] = max(item["confidence"], 98); item["verified"] = True

    with _store() as conn:
        for item in evidence_rows:
            conn.execute("INSERT INTO founder_pedigree_evidence(evidence_id,candidate_id,provider,evidence_type,label,evidence,source_url,strength,confidence,verification_status,external_id,raw_json,checked_at,rule_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id,provider,evidence_type,external_id) DO UPDATE SET label=excluded.label,evidence=excluded.evidence,source_url=excluded.source_url,strength=excluded.strength,confidence=excluded.confidence,verification_status=excluded.verification_status,raw_json=excluded.raw_json,checked_at=excluded.checked_at,rule_version=excluded.rule_version", (str(uuid.uuid4()),candidate_id,item["provider"],item["type"],item["label"],item["evidence"][:1200],item["url"],item["strength"],item["confidence"],"verified" if item["verified"] else "needs_review",item["external_id"],_json(item["raw"]),now,PEDIGREE_VERSION))
            conn.execute("INSERT INTO founder_sources(source_id,candidate_id,source_type,source_url,raw_source_name,extraction_method,match_confidence,discovered_at) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id,source_url) DO UPDATE SET match_confidence=excluded.match_confidence,discovered_at=excluded.discovered_at", (str(uuid.uuid4()),candidate_id,item["provider"],item["url"],item["label"],"structured_public_api",item["confidence"],now))
        verified = [item for item in evidence_rows if item["verified"]]
        if verified:
            pedigree_score = max(item["strength"] for item in verified)
            conn.execute("UPDATE founder_candidates SET talent_score=CASE WHEN talent_score>? THEN talent_score ELSE ? END,last_verified_at=?,updated_at=? WHERE candidate_id=?", (pedigree_score,pedigree_score,now,now,candidate_id))
        conn.commit()
    recalculate_scores(candidate_id)
    if provider_errors and len(provider_errors) == 3:
        raise RuntimeError("All pedigree providers failed: " + "; ".join(f'{item["provider"]}: {item["error"]}' for item in provider_errors))
    return {"candidate_id":candidate_id,"matches":len(evidence_rows),"verified":sum(1 for item in evidence_rows if item["verified"]),"provider_errors":provider_errors}


def discover_talent(*, client: GitHubClient | None = None, maximum: int | None = None) -> dict[str, int]:
    """Conservative official-API discovery; every match remains reviewable."""
    client = client or GitHubClient(); settings = founder_settings()
    targets = [*settings.get("target_companies", []), *settings.get("target_universities", []), *settings.get("target_laboratories", [])]
    maximum = maximum or int(settings.get("maximum_people_per_run", 200)); discovered = review = 0
    seen: set[str] = set()
    for target in targets:
        if len(seen) >= maximum: break
        query = urllib.parse.quote(f'"{target}" in:bio type:user')
        for item in client.paginated(f"/search/users?q={query}", maximum_pages=1):
            login = clean(item.get("login", "")).lower()
            if not login or login in seen or len(seen) >= maximum: continue
            seen.add(login)
            profile, _ = client.request(f"/users/{login}")
            full_name = clean(profile.get("name", ""))
            if not full_name: continue
            bio = clean(profile.get("bio", "")); company = clean(profile.get("company", "")).lstrip("@").strip()
            affiliation = company or (target if target.lower() in bio.lower() else "")
            categories = [category for category in settings.get("technical_categories", []) if any(term in f"{bio} {profile.get('company','')}".lower() for term in category.lower().split())][:3]
            talent = min(85, 35 + min(25, int(math.log1p(int(profile.get("public_repos") or 0)) * 7)) + min(15, int(math.log1p(int(profile.get("followers") or 0)) * 3)))
            fit = min(90, 20 + 18 * len(categories))
            result = create_candidate({"full_name": full_name, "current_employer": affiliation, "current_title": "", "geographic_location": clean(profile.get("location", "")), "personal_website": clean(profile.get("blog", "")), "github_username": login, "github_profile_url": clean(profile.get("html_url", "")), "technical_categories": categories, "talent_score": talent, "glasswing_fit_score": fit, "identity_confidence": 70, "monitoring_status": "inactive", "analyst_status": "needs_research", "source_type": "github_official_api", "source_url": clean(profile.get("html_url", "")), "extraction_method": "github_user_search", "match_confidence": 70})
            if result["status"] == "created":
                discovered += 1; candidate_id = result["candidate_id"]
                with _store() as conn:
                    now = utc_now()
                    conn.execute("INSERT INTO github_identity_candidates(identity_id,candidate_id,username,profile_url,positive_score,confidence_score,evidence_json,contradictions_json,resolution_status,resolution_method,resolved_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id,username) DO NOTHING", (str(uuid.uuid4()), candidate_id, login, clean(profile.get("html_url", "")), 100, 100, _json(["The candidate record originated from this exact official GitHub API profile", f'Public GitHub bio matched configured target: {target}']), _json([]), "verified", "direct_github_source", now, now))
                    conn.execute("INSERT INTO verified_github_identities(candidate_id,username,github_user_id,confidence_score,verification_method,verified_at) VALUES (?,?,?,?,?,?) ON CONFLICT(candidate_id) DO NOTHING", (candidate_id, login, str(profile.get("id") or ""), 100, "direct_github_source", now))
                    conn.execute("UPDATE founder_candidates SET identity_confidence=100,monitoring_status='inactive' WHERE candidate_id=?", (candidate_id,))
                    conn.commit()
    return {"discovered": discovered, "identity_review": review, "searched_targets": len(targets)}


def reconcile_direct_github_discoveries() -> int:
    """Correct legacy discovery rows whose source already proves identity."""
    changed = 0; now = utc_now()
    with _store() as conn:
        rows = conn.execute("SELECT c.candidate_id,c.github_username,i.identity_id FROM founder_candidates c JOIN github_identity_candidates i ON i.candidate_id=c.candidate_id AND i.username=c.github_username WHERE c.source_type='github_official_api' AND i.resolution_method='automatic_discovery'").fetchall()
        for row in rows:
            conn.execute("UPDATE github_identity_candidates SET positive_score=100,confidence_score=100,resolution_status='verified',resolution_method='direct_github_source',resolved_at=? WHERE identity_id=?", (now, row["identity_id"]))
            conn.execute("INSERT INTO verified_github_identities(candidate_id,username,confidence_score,verification_method,verified_at) VALUES (?,?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET username=excluded.username,confidence_score=excluded.confidence_score,verification_method=excluded.verification_method,verified_at=excluded.verified_at", (row["candidate_id"], row["github_username"], 100, "direct_github_source", now))
            conn.execute("UPDATE founder_candidates SET identity_confidence=100,monitoring_status='inactive',updated_at=? WHERE candidate_id=?", (now, row["candidate_id"]))
            changed += 1
        conn.commit()
    return changed


def resolve_github_identity(candidate_id: str, *, client: GitHubClient | None = None) -> dict[str, Any]:
    client = client or GitHubClient()
    profile = candidate_profile(candidate_id)
    if not profile: raise ValueError("Candidate not found")
    query = urllib.parse.quote(f'{profile["full_name"]} in:fullname')
    candidates = client.paginated(f"/search/users?q={query}", maximum_pages=2)[:10]
    scored = []
    for item in candidates:
        details, _ = client.request(f'/users/{item["login"]}')
        score, evidence, contradictions = identity_confidence(profile, details)
        scored.append((score, details, evidence, contradictions))
    scored.sort(key=lambda value: value[0], reverse=True)
    now = utc_now()
    with _store() as conn:
        for score, details, evidence, contradictions in scored:
            state = identity_state(score, contradictions, len([item for item in scored if item[0] >= 70]))
            conn.execute(
                "INSERT INTO github_identity_candidates(identity_id,candidate_id,username,profile_url,positive_score,confidence_score,evidence_json,contradictions_json,resolution_status,resolution_method,resolved_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(candidate_id,username) DO UPDATE SET positive_score=excluded.positive_score,confidence_score=excluded.confidence_score,evidence_json=excluded.evidence_json,contradictions_json=excluded.contradictions_json,resolution_status=excluded.resolution_status,resolved_at=excluded.resolved_at",
                (str(uuid.uuid4()), candidate_id, details["login"], details.get("html_url", ""), score, score, _json(evidence), _json(contradictions), state, "automatic", now, now),
            )
        if scored and scored[0][0] >= 90 and not scored[0][3] and sum(item[0] >= 90 for item in scored) == 1:
            score, details, _, _ = scored[0]
            verify_identity(conn, candidate_id, details["login"], score, "automatic")
        conn.commit()
    return {"candidate_id": candidate_id, "candidates": [{"username": item[1]["login"], "confidence": item[0], "evidence": item[2], "contradictions": item[3]} for item in scored]}


def verify_identity(conn, candidate_id: str, username: str, confidence: int, method: str = "manual") -> None:
    now = utc_now(); username = clean(username).lstrip("@")
    conn.execute(
        "INSERT INTO verified_github_identities(candidate_id,username,confidence_score,verification_method,verified_at) VALUES (?,?,?,?,?) "
        "ON CONFLICT(candidate_id) DO UPDATE SET username=excluded.username,confidence_score=excluded.confidence_score,verification_method=excluded.verification_method,verified_at=excluded.verified_at",
        (candidate_id, username, confidence, method, now),
    )
    conn.execute("UPDATE founder_candidates SET github_username=?,github_profile_url=?,identity_confidence=?,monitoring_status='active',updated_at=? WHERE candidate_id=?", (username, f"https://github.com/{username}", confidence, now, candidate_id))
    conn.execute("UPDATE github_identity_candidates SET resolution_status=CASE WHEN username=? THEN 'verified' ELSE 'rejected' END,resolution_method=?,resolved_at=? WHERE candidate_id=?", (username, method, now, candidate_id))


def resolve_identity_manually(candidate_id: str, username: str, *, accepted: bool = True) -> None:
    with _store() as conn:
        if accepted: verify_identity(conn, candidate_id, username, 100, "manual")
        else: conn.execute("UPDATE github_identity_candidates SET resolution_status='rejected',resolution_method='manual',resolved_at=? WHERE candidate_id=? AND username=?", (utc_now(), candidate_id, username))
        conn.commit()


def ingest_github_events(candidate_id: str, username: str, events: Iterable[dict[str, Any]], *, collected_at: str | None = None) -> dict[str, int]:
    collected_at = collected_at or utc_now(); inserted = duplicate = 0
    with _store() as conn:
        for event in events:
            event_id = clean(event.get("id", "")) or f'{username}:{event.get("type", "")}:{event.get("created_at", "")}:{event.get("repo", {}).get("name", "")}'
            cursor = conn.execute(
                "INSERT INTO founder_github_events(event_id,candidate_id,username,event_type,repo_name,organization,event_created_at,collected_at,payload_json,source_url) VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING",
                (event_id, candidate_id, username, clean(event.get("type", "")), clean((event.get("repo") or {}).get("name", "")), clean((event.get("org") or {}).get("login", "")), clean(event.get("created_at", "")) or collected_at, collected_at, _json(event), f"https://github.com/{clean((event.get('repo') or {}).get('name', ''))}"),
            )
            if cursor.rowcount: inserted += 1
            else: duplicate += 1
        conn.commit()
    return {"inserted": inserted, "duplicates": duplicate}


def collect_candidate(candidate_id: str, *, client: GitHubClient | None = None) -> dict[str, Any]:
    client = client or GitHubClient(); now = utc_now()
    with _store(True) as conn:
        identity = conn.execute("SELECT * FROM verified_github_identities WHERE candidate_id=?", (candidate_id,)).fetchone()
    if not identity: raise ValueError("Candidate does not have a verified GitHub identity")
    username = str(identity["username"])
    events = client.paginated(f"/users/{urllib.parse.quote(username)}/events/public", maximum_pages=10)
    result = ingest_github_events(candidate_id, username, events, collected_at=now)
    repos = client.paginated(f"/users/{urllib.parse.quote(username)}/repos?sort=updated", maximum_pages=10)
    orgs = client.paginated(f"/users/{urllib.parse.quote(username)}/orgs", maximum_pages=5)
    with _store() as conn:
        for repo in repos:
            full_name = clean(repo.get("full_name", ""))
            repo_id = f'{candidate_id}:{repo.get("id") or full_name}'
            conn.execute(
                "INSERT INTO founder_github_repositories(repository_id,candidate_id,full_name,owner_login,description,topics_json,primary_language,homepage,created_at,updated_at,first_seen_at,last_seen_at,archived,is_fork) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(candidate_id,full_name) DO UPDATE SET description=excluded.description,topics_json=excluded.topics_json,primary_language=excluded.primary_language,homepage=excluded.homepage,updated_at=excluded.updated_at,last_seen_at=excluded.last_seen_at,archived=excluded.archived,is_fork=excluded.is_fork",
                (repo_id, candidate_id, full_name, clean((repo.get("owner") or {}).get("login", "")), clean(repo.get("description", "")), _json(repo.get("topics", [])), clean(repo.get("language", "")), clean(repo.get("homepage", "")), clean(repo.get("created_at", "")), clean(repo.get("updated_at", "")), now, now, bool(repo.get("archived", False)), bool(repo.get("fork", False))),
            )
        for org in orgs:
            # organization_id is the table's primary key while the row itself
            # is per candidate *and* per org -- so two people in the same
            # GitHub org produced the same id and the second one violated the
            # key. ON CONFLICT(candidate_id,login) never caught it, because
            # the collision is on the primary key rather than that unique
            # constraint. Ten of 174 people failed collection this way on the
            # first real run, all of them colleagues of somebody already
            # collected, which is the normal case for this dataset.
            login = clean(org.get("login", ""))
            org_id = f'{candidate_id}:{org.get("id") or login}'
            conn.execute(
                "INSERT INTO founder_github_organizations(organization_id,candidate_id,login,profile_url,first_seen_at,last_seen_at) VALUES (?,?,?,?,?,?) ON CONFLICT(candidate_id,login) DO UPDATE SET last_seen_at=excluded.last_seen_at,profile_url=excluded.profile_url",
                (org_id, candidate_id, login, clean(org.get("url", "")), now, now),
            )
        conn.execute("UPDATE verified_github_identities SET last_collected_at=? WHERE candidate_id=?", (now, candidate_id))
        conn.execute("UPDATE founder_candidates SET last_refreshed_at=?,updated_at=? WHERE candidate_id=?", (now, now, candidate_id))
        conn.commit()
    aggregate_activity(candidate_id)
    calculate_signals(candidate_id)
    recalculate_scores(candidate_id)
    return {**result, "repositories": len(repos), "organizations": len(orgs)}


def _parse_time(value: str) -> datetime:
    try: return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError): return datetime.now(timezone.utc)


def activity_windows(candidate_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc); start = now - timedelta(days=180)
    with _store(True) as conn:
        rows = conn.execute("SELECT event_type,repo_name,event_created_at,payload_json FROM founder_github_events WHERE candidate_id=? AND event_created_at>=?", (candidate_id, start.isoformat())).fetchall()
    events = [dict(row) for row in rows]
    def metrics(days: int, offset: int = 0) -> dict[str, Any]:
        high = now - timedelta(days=offset); low = high - timedelta(days=days)
        chosen = [row for row in events if low <= _parse_time(str(row["event_created_at"])) < high]
        types = Counter(str(row["event_type"]) for row in chosen)
        dates = {_parse_time(str(row["event_created_at"])).date().isoformat() for row in chosen}
        repos = {str(row["repo_name"]) for row in chosen if row["repo_name"]}
        return {"total": len(chosen), "active_days": len(dates), "repositories": len(repos), "pushes": types["PushEvent"], "pull_requests": types["PullRequestEvent"], "issues": types["IssuesEvent"], "releases": types["ReleaseEvent"], "forks": types["ForkEvent"], "new_repositories": types["CreateEvent"]}
    current = metrics(30); previous = metrics(30, 30)
    six = [metrics(30, offset) for offset in range(30, 181, 30)]
    baseline = sum(item["total"] for item in six) / len(six) if six else 0
    acceleration = current["total"] / baseline if baseline else None
    return {"days_7": metrics(7), "days_30": current, "days_90": metrics(90), "days_180": metrics(180), "previous_30": previous, "six_month_baseline": round(baseline, 2), "acceleration": round(acceleration, 2) if acceleration is not None else None, "baseline_status": "sufficient" if len(events) >= 10 and any(item["total"] for item in six) else "insufficient"}


def aggregate_activity(candidate_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc); windows = activity_windows(candidate_id, now=now)
    with _store(True) as conn:
        rows = conn.execute("SELECT event_type,repo_name,event_created_at FROM founder_github_events WHERE candidate_id=?", (candidate_id,)).fetchall()
    daily: dict[str, list[dict[str, Any]]] = defaultdict(list); monthly: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw); when = _parse_time(str(row["event_created_at"])); daily[when.date().isoformat()].append(row); monthly[when.strftime("%Y-%m")].append(row)
    with _store() as conn:
        for day, events in daily.items():
            data = {"events": len(events), "pushes": sum(e["event_type"] == "PushEvent" for e in events), "repositories": len({e["repo_name"] for e in events if e["repo_name"]})}
            conn.execute("INSERT INTO founder_activity_daily(candidate_id,metric_date,metrics_json,total_activity,active_repositories) VALUES (?,?,?,?,?) ON CONFLICT(candidate_id,metric_date) DO UPDATE SET metrics_json=excluded.metrics_json,total_activity=excluded.total_activity,active_repositories=excluded.active_repositories", (candidate_id, day, _json(data), len(events), data["repositories"]))
        for month, events in monthly.items():
            data = {"events": len(events), "repositories": len({e["repo_name"] for e in events if e["repo_name"]})}
            conn.execute("INSERT INTO founder_activity_monthly(candidate_id,metric_month,metrics_json,total_activity,active_days,baseline_status) VALUES (?,?,?,?,?,?) ON CONFLICT(candidate_id,metric_month) DO UPDATE SET metrics_json=excluded.metrics_json,total_activity=excluded.total_activity,active_days=excluded.active_days,baseline_status=excluded.baseline_status", (candidate_id, month, _json(data), len(events), len({_parse_time(str(e["event_created_at"])).date() for e in events}), windows["baseline_status"]))
        conn.commit()
    return windows


def _monthly_buckets(events: list[dict[str, Any]], now: datetime, windows: int = 7) -> list[int]:
    """Event totals in consecutive 30-day windows, most recent first."""
    counts = []
    for index in range(windows):
        high = now - timedelta(days=30 * index)
        low = high - timedelta(days=30)
        counts.append(sum(
            1 for row in events
            if low <= _parse_time(str(row["event_created_at"])) < high
        ))
    return counts


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values); middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2


def commit_spike(candidate_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    """A step change measured against this person's own history.

    ``activity_windows`` already reports an ``acceleration`` -- the current 30
    days over the *mean* of the previous six months -- and gates a signal on
    it exceeding 3x with at least 6 events. Two things go wrong with that, in
    opposite directions:

    * A mean is dragged by a single earlier burst, so one busy month last
      spring raises the bar for the rest of the year and hides the very
      transition this is looking for.
    * A pure ratio does not scale. Somebody averaging 400 events a month
      needs 1,200 to trip a 3x gate, which will never happen; somebody
      averaging one needs six, which happens by accident. The people this
      module exists to watch -- engineers inside OpenAI, Anthropic, MIT CSAIL
      -- are all in the first group.

    So the test here is a median-based z-score: how far the current month sits
    outside the person's *normal variation*, using the median absolute
    deviation of their own prior months. A prolific but steady committer has a
    small deviation, so a 50% lift is enormous for them; an erratic one has a
    large deviation, and the same lift correctly reads as noise. Absolute
    floors stop a dormant account registering a spike on a handful of events.
    """
    now = now or datetime.now(timezone.utc)
    with _store(True) as conn:
        rows = conn.execute(
            "SELECT event_type,repo_name,event_created_at FROM founder_github_events "
            "WHERE candidate_id=? AND event_created_at>=?",
            (candidate_id, (now - timedelta(days=240)).isoformat()),
        ).fetchall()
    events = [dict(row) for row in rows]
    # GitHub's public events API returns at most ~300 events and about 90
    # days, whichever comes first. So on a first collection there is no
    # six-month history at all: everything lands in the current window, the
    # prior ones read zero, and a z-score against that is arithmetic on
    # missing data rather than a measurement. It produced exactly that on the
    # first real run -- "286 events this month against a median of 0.5, 285.5
    # deviations outside their normal range" -- for two people whose accounts
    # we had simply never seen before.
    #
    # The baseline has to be built up by the daily collection over months.
    # Until the data we hold actually reaches back into the baseline window,
    # there is nothing to compare against and this says so.
    oldest = min(
        (_parse_time(str(row["event_created_at"])) for row in events),
        default=None,
    )
    covers_baseline = bool(oldest and oldest <= now - timedelta(days=90))
    buckets = _monthly_buckets(events, now)
    current, prior = buckets[0], buckets[1:]
    baseline = _median([float(value) for value in prior])
    deviation = _median([abs(value - baseline) for value in prior]) * 1.4826
    # A person with a perfectly flat history has zero deviation, which would
    # divide by nothing and call any change infinite. One event of slack is
    # the smallest honest floor.
    spread = max(deviation, 1.0)
    z = (current - baseline) / spread
    return {
        "current_30d": current,
        "history_status": "sufficient" if covers_baseline else "too_short",
        "oldest_event": oldest.isoformat() if oldest else "",
        "baseline_median": round(baseline, 2),
        "baseline_deviation": round(deviation, 2),
        "months_of_history": sum(1 for value in prior if value > 0),
        "ratio": round(current / baseline, 2) if baseline else None,
        "lift": round(current - baseline, 2),
        "z_score": round(z, 2),
        # Enough history to have a baseline at all, a real step outside its
        # variation, and enough absolute volume that it is not a rounding
        # error on a dormant account.
        "is_spike": bool(
            covers_baseline
            and sum(1 for value in prior if value > 0) >= 3
            # A median of 0.5 is not a baseline. Counting months that merely
            # contain *an* event let a near-empty history clear the gate.
            and baseline >= 5
            and z >= 3.0
            and current >= baseline + 10
            and current >= 10
        ),
    }


# A day job leaves a lot of traces. These floors are set from the live table:
# the six spurious hits had baselines of two and three events, while the real
# anchors -- openai, electron, domdomegg -- carry 400 to 900.
MIN_BASELINE_EVENTS = max(1, int(os.getenv("FOUNDER_MIN_BASELINE_EVENTS", "40")))
MIN_ANCHOR_EVENTS = max(1, int(os.getenv("FOUNDER_MIN_ANCHOR_EVENTS", "25")))
MIN_CURRENT_EVENTS = max(1, int(os.getenv("FOUNDER_MIN_CURRENT_EVENTS", "25")))


def focus_shift(candidate_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    """Whether the work has moved away from wherever it used to live.

    This is the part that distinguishes leaving from being busy. An engineer
    at OpenAI whose commit volume doubles inside ``openai/*`` is having a
    heavy quarter. One whose volume moves *out* of the org that used to hold
    most of it, into repositories that did not exist in their history, is
    doing something else.

    Deliberately self-calibrating: it does not try to match the stored
    employer name against a GitHub org, which fails on every abbreviation and
    rename ("anthropics" against "Anthropic", "openai" against "OpenAI").
    Whatever owner dominated the baseline *is* the day job for this purpose,
    whatever it is called.
    """
    now = now or datetime.now(timezone.utc)
    with _store(True) as conn:
        rows = conn.execute(
            "SELECT repo_name,event_created_at FROM founder_github_events "
            "WHERE candidate_id=? AND event_created_at>=?",
            (candidate_id, (now - timedelta(days=240)).isoformat()),
        ).fetchall()

    def owners(low: datetime, high: datetime) -> Counter:
        found: Counter = Counter()
        for row in rows:
            when = _parse_time(str(row["event_created_at"]))
            if not (low <= when < high):
                continue
            name = clean(str(row["repo_name"] or ""))
            if "/" in name:
                found[name.split("/", 1)[0].lower()] += 1
        return found

    current = owners(now - timedelta(days=30), now)
    baseline = owners(now - timedelta(days=210), now - timedelta(days=30))
    if not baseline or not current:
        return {"anchor": "", "baseline_share": None, "current_share": None,
                "new_owners": [], "is_shift": False, "current_events": sum(current.values())}
    anchor, anchor_count = baseline.most_common(1)[0]
    baseline_events = sum(baseline.values())
    baseline_share = anchor_count / baseline_events
    current_share = current.get(anchor, 0) / sum(current.values())
    new_owners = [owner for owner in current if owner not in baseline]
    return {
        "anchor": anchor,
        "baseline_share": round(baseline_share, 3),
        "current_share": round(current_share, 3),
        "baseline_events": baseline_events,
        "anchor_baseline_events": anchor_count,
        "new_owners": sorted(new_owners),
        "current_events": sum(current.values()),
        # A share needs a denominator before it means anything. Every one of
        # the first six hits had a baseline_share of exactly 0.5 or 0.667 --
        # one half and two thirds -- because the baseline held two or three
        # events in total, and one incidental repository was "most" of them.
        # The anchors it named were airtable, bradagi, heygen-com, takahirom:
        # not one was the person's employer.
        #
        # For contrast the genuine day jobs in the same table carry hundreds:
        # openai at 802 events, electron at 900, domdomegg at 434. So the
        # baseline has to look like a job before its collapse means anything,
        # and the current window has to be substantial enough that "the work
        # moved" is a claim about work rather than about three commits.
        "is_shift": bool(
            baseline_events >= MIN_BASELINE_EVENTS
            and anchor_count >= MIN_ANCHOR_EVENTS
            and baseline_share >= 0.5
            and current_share <= baseline_share / 2
            and new_owners
            and sum(current.values()) >= MIN_CURRENT_EVENTS
        ),
    }


def certificate_feed_matches(candidate_id: str, *, now: datetime | None = None, window_days: int = 180) -> list[dict[str, Any]]:
    """Domains on this person's repositories that the certificate feed has seen.

    This is the corroboration only this system can produce. The sourcing
    pipeline watches certificate transparency logs, so a domain appearing in
    the classification corpus means a TLS certificate was issued for it --
    which happens when somebody provisions it, typically weeks before there
    is a site to look at.

    A repository homepage on its own is weak: engineers link their blog, their
    employer, a docs site. The same domain turning up independently in the
    certificate feed, recently, is a different claim -- somebody stood
    something up. Matching the two is what turns "this person has a side
    project" into "this person registered infrastructure for it".

    Read-only against the classification store, and deliberately not a
    judgement about the company: what the screening pipeline concluded is
    reported alongside, not folded in.
    """
    now = now or datetime.now(timezone.utc)
    # Forks are excluded, and that exclusion is the difference between a
    # signal and noise. A fork inherits the upstream project's homepage, so
    # every fork of a popular AI repository manufactures a correlation with
    # that project's domain. Measured on the first live run: of 40 matches,
    # nearly all were forks -- MukundaKatta/dspy against dspy.ai,
    # sarahcstringer/twilio-csharp against twilio.com,
    # romainhuet/react-stripe-elements against stripe.com. None of those
    # people registered anything.
    with _store(True) as conn:
        repos = conn.execute(
            "SELECT full_name,homepage,created_at FROM founder_github_repositories "
            "WHERE candidate_id=? AND homepage<>'' AND is_fork=FALSE",
            (candidate_id,),
        ).fetchall()
    wanted: dict[str, dict[str, Any]] = {}
    for row in repos:
        domain = stable_domain(str(row["homepage"] or ""))
        if domain:
            wanted.setdefault(domain, {"repository": str(row["full_name"]), "repo_created_at": str(row["created_at"] or "")})
    if not wanted:
        return []

    # A domain that shows up on several people's repositories belongs to the
    # project they all forked or contributed to, not to any one of them.
    # is_fork only populates from the next collection onward, so on rows
    # already stored it is the only thing separating slack.com, twilio.com and
    # x.com -- the first four matches this ever produced, every one of them an
    # upstream homepage -- from somebody's actual new company.
    # Grouped after normalising, not before: "https://www.twilio.com/" and
    # "https://twilio.com" are the same domain and different strings, so
    # grouping on the raw homepage counted each as unique and caught nothing.
    owners_by_domain: dict[str, set[str]] = {}
    with _store(True) as conn:
        for row in conn.execute(
            "SELECT candidate_id, homepage FROM founder_github_repositories "
            "WHERE homepage <> ''"
        ).fetchall():
            domain = stable_domain(str(row["homepage"] or ""))
            if domain:
                owners_by_domain.setdefault(domain, set()).add(
                    str(row["candidate_id"])
                )
    shared = {
        domain for domain, owners in owners_by_domain.items()
        if len(owners) > 1
    }
    if shared:
        wanted = {
            domain: meta for domain, meta in wanted.items()
            if domain not in shared
        }
    if not wanted:
        return []

    # first_seen on the queue is when CertStream first handed us a certificate
    # for the domain. classified_at is when *we* got round to screening it,
    # which is a fact about our backlog and not about the domain -- reading it
    # as recency marked twilio.com and microsoft.com as fresh registrations,
    # and passed all 40 matches on the first live run.
    cutoff = (now - timedelta(days=window_days)).isoformat().replace("+00:00", "Z")
    queue_first_seen: dict[str, str] = {}
    try:
        # Local import: continuous_pipeline pulls in the scoring module, and
        # this file is imported by it in turn.
        from continuous_pipeline import connect_db as connect_queue

        queue = connect_queue()
        try:
            names = sorted(wanted)
            for start in range(0, len(names), 400):
                batch = names[start:start + 400]
                marks = ",".join("?" for _ in batch)
                for row in queue.execute(
                    f"SELECT domain,first_seen FROM domains WHERE domain IN ({marks})",
                    batch,
                ).fetchall():
                    queue_first_seen[str(row["domain"])] = str(row["first_seen"] or "")
        finally:
            queue.close()
    except Exception:
        # No queue reachable: fall back to the classification timestamp and
        # let the caller see it, rather than silently claiming recency.
        queue_first_seen = {}
    matches: list[dict[str, Any]] = []
    names = sorted(wanted)
    with _store(True) as conn:
        for start in range(0, len(names), 400):
            batch = names[start:start + 400]
            marks = ",".join("?" for _ in batch)
            for row in conn.execute(
                "SELECT domain,company_name,routing_bucket,fit_score,"
                "COALESCE(NULLIF(classified_at,''),updated_at) AS first_seen "
                f"FROM classifications WHERE domain IN ({marks})",
                batch,
            ).fetchall():
                domain = str(row["domain"])
                seen = queue_first_seen.get(domain) or str(row["first_seen"] or "")
                matches.append({
                    "domain": domain,
                    "repository": wanted[domain]["repository"],
                    "first_seen": seen,
                    "first_seen_source": (
                        "certificate_feed" if queue_first_seen.get(domain)
                        else "screened_at"
                    ),
                    "company_name": str(row["company_name"] or ""),
                    "routing_bucket": str(row["routing_bucket"] or ""),
                    "fit_score": int(row["fit_score"] or 0),
                    # Old registrations are the founder's blog or their
                    # employer. Recency is what makes this a formation signal.
                    "is_recent": bool(seen and seen >= cutoff),
                })
    matches.sort(key=lambda item: (not item["is_recent"], item["domain"]))
    return matches


COMMERCIAL_TERMS = ("design partner", "waitlist", "request access", "book a demo", "enterprise", "pricing")
HIRING_TERMS = ("founding engineer", "first engineer", "early team", "join our founding team")


def _insert_signal(conn, candidate_id: str, signal_type: str, event_at: str, evidence: str, source_url: str, strength: int, confidence: int, explanation: str, dedupe_suffix: str = "") -> bool:
    detected = utc_now(); key = f"{candidate_id}:{signal_type}:{event_at[:10]}:{dedupe_suffix}"
    cursor = conn.execute("INSERT INTO founder_signals(signal_id,candidate_id,signal_type,detected_at,event_at,evidence,source_url,strength,confidence,explanation,rule_version,dedupe_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(dedupe_key) DO NOTHING", (str(uuid.uuid4()), candidate_id, signal_type, detected, event_at, evidence[:1200], source_url, strength, confidence, explanation, SIGNAL_VERSION, key))
    return bool(cursor.rowcount)


def calculate_signals(candidate_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc); windows = activity_windows(candidate_id, now=now); added = 0
    settings = founder_settings(); weights = {**DEFAULT_SIGNAL_WEIGHTS, **settings.get("signal_weights", {})}
    with _store() as conn:
        if windows["acceleration"] is not None and windows["acceleration"] >= 3 and windows["days_30"]["total"] >= 6:
            added += _insert_signal(conn, candidate_id, "activity_acceleration", now.isoformat(), f'{windows["days_30"]["total"]} public events versus a {windows["six_month_baseline"]} monthly baseline', "", weights["activity_acceleration"], 75, "Public GitHub activity is more than three times the observed baseline.")
        # The two signals the module was missing. Kept beside the ratio rather
        # than replacing it: a 3x jump on a modest baseline is still worth
        # noting, it just should not be the only thing that can fire, because
        # it is unreachable for exactly the people here who matter most.
        spike = commit_spike(candidate_id, now=now)
        if spike["is_spike"]:
            added += _insert_signal(
                conn, candidate_id, "commit_step_change", now.isoformat(),
                f'{spike["current_30d"]} events this month against a median of '
                f'{spike["baseline_median"]} over the previous six, {spike["z_score"]} '
                f'deviations outside their own normal range',
                "", weights["commit_step_change"], 80,
                "Public activity stepped outside this person's own established "
                "variation, rather than exceeding a fixed threshold.",
            )
        for match in certificate_feed_matches(candidate_id, now=now):
            if not match["is_recent"]:
                continue
            verdict = (
                f'; screening called it {match["routing_bucket"]} at '
                f'{match["fit_score"]}' if match["routing_bucket"] else ""
            )
            added += _insert_signal(
                conn, candidate_id, "domain_in_certificate_feed",
                match["first_seen"] or now.isoformat(),
                f'{match["domain"]}, linked from {match["repository"]}, was seen '
                f'in the certificate feed on {match["first_seen"][:10]}{verdict}',
                f'https://{match["domain"]}',
                weights["domain_in_certificate_feed"], 90,
                "A domain this person's repository points at was independently "
                "observed being provisioned in certificate transparency logs.",
                match["domain"],
            )
        shift = focus_shift(candidate_id, now=now)
        if shift["is_shift"]:
            added += _insert_signal(
                conn, candidate_id, "focus_left_the_day_job", now.isoformat(),
                f'Work in {shift["anchor"]} fell from {int(shift["baseline_share"] * 100)}% '
                f'to {int(shift["current_share"] * 100)}% of their activity, moving to '
                f'{", ".join(shift["new_owners"][:4])}',
                f'https://github.com/{shift["new_owners"][0]}' if shift["new_owners"] else "",
                weights["focus_left_the_day_job"], 85,
                "The repositories that held most of this person's work no longer "
                "do, and the work has moved somewhere absent from their history.",
                shift["anchor"],
            )
        recent_repos = conn.execute("SELECT * FROM founder_github_repositories WHERE candidate_id=? AND created_at>=?", (candidate_id, (now - timedelta(days=30)).isoformat())).fetchall()
        for repo in recent_repos:
            added += _insert_signal(conn, candidate_id, "new_personal_repository", str(repo["created_at"]), f'Created {repo["full_name"]}: {repo["description"]}', f'https://github.com/{repo["full_name"]}', weights["new_personal_repository"], 85, "A new public repository may indicate a new independent project.", str(repo["full_name"]))
            text = f'{repo["description"]} {repo["readme_excerpt"]}'.lower()
            terms = [term for term in COMMERCIAL_TERMS if term in text]
            if terms:
                added += _insert_signal(conn, candidate_id, "commercial_language", str(repo["updated_at"] or now.isoformat()), f'{repo["full_name"]} includes: {", ".join(terms)}', f'https://github.com/{repo["full_name"]}', weights["commercial_language"], 90, "Commercial or customer-discovery language appeared in a public project.", str(repo["full_name"]))
            hiring = [term for term in HIRING_TERMS if term in text]
            if hiring:
                added += _insert_signal(conn, candidate_id, "founding_hiring_language", str(repo["updated_at"] or now.isoformat()), f'{repo["full_name"]} includes: {", ".join(hiring)}', f'https://github.com/{repo["full_name"]}', weights["founding_hiring_language"], 92, "Early-company hiring language appeared in a public project.", str(repo["full_name"]))
            if stable_domain(repo["homepage"]):
                added += _insert_signal(conn, candidate_id, "new_external_domain", str(repo["updated_at"] or now.isoformat()), f'{repo["full_name"]} links to {repo["homepage"]}', str(repo["homepage"]), weights["new_external_domain"], 88, "A project gained an external domain.", str(repo["full_name"]))
        recent_orgs = conn.execute("SELECT * FROM founder_github_organizations WHERE candidate_id=? AND first_seen_at>=?", (candidate_id, (now - timedelta(days=30)).isoformat())).fetchall()
        for org in recent_orgs:
            added += _insert_signal(conn, candidate_id, "new_organization", str(org["first_seen_at"]), f'First observed in GitHub organization {org["login"]}', f'https://github.com/{org["login"]}', weights["new_organization"], 85, "A newly observed GitHub organization can indicate team formation.", str(org["login"]))
        conn.commit()
    return {"added": added, "activity": windows}


def recalculate_scores(candidate_id: str) -> dict[str, Any]:
    settings = founder_settings(); score_weights = {**DEFAULT_SCORE_WEIGHTS, **settings.get("score_weights", {})}; signal_weights = {**DEFAULT_SIGNAL_WEIGHTS, **settings.get("signal_weights", {})}
    now = datetime.now(timezone.utc)
    with _store() as conn:
        candidate = conn.execute("SELECT * FROM founder_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
        if not candidate: raise ValueError("Candidate not found")
        signals = conn.execute("SELECT * FROM founder_signals WHERE candidate_id=? AND analyst_disposition<>'dismissed' ORDER BY event_at DESC", (candidate_id,)).fetchall()
        transition = 0.0; contributions = []
        for signal in signals:
            age = max(0, (now - _parse_time(str(signal["event_at"]))).days)
            decay = math.exp(-age / 90)
            points = float(signal_weights.get(str(signal["signal_type"]), signal["strength"])) * decay * (int(signal["confidence"]) / 100)
            transition += points; contributions.append({"signal": signal["signal_type"], "points": round(points, 1), "evidence": signal["evidence"]})
        recent_types = {str(signal["signal_type"]) for signal in signals if (now - _parse_time(str(signal["event_at"]))).days <= 30}
        if len(recent_types & {"new_organization", "new_personal_repository", "new_external_domain", "commercial_language"}) >= 3:
            transition += 15; contributions.append({"signal": "combination_bonus", "points": 15, "evidence": "Three related formation signals occurred within 30 days"})
        # Compressed rather than clipped. min(100, ...) put six candidates on
        # exactly 100 at once, and a score that saturates ranks nothing -- the
        # whole point of it is to say who to look at first. This is asymptotic:
        # it never quite reaches 100, so more evidence always outranks less,
        # however much of it there already is.
        transition_score = round(
            100 * (1 - math.exp(-max(0.0, transition) / TRANSITION_SCALE))
        )
        signal_confidence = round(sum(int(row["confidence"]) for row in signals[:5]) / max(len(signals[:5]), 1)) if signals else 0
        talent = int(candidate["talent_score"]); fit = int(candidate["glasswing_fit_score"]); identity = int(candidate["identity_confidence"])
        overall = round(talent * score_weights["talent"] + transition_score * score_weights["transition"] + fit * score_weights["glasswing_fit"] + identity * score_weights["identity"] + signal_confidence * score_weights["signal_confidence"])
        calculated = utc_now(); explanation = {"weights": score_weights, "signal_contributions": contributions[:10], "time_decay_half_life_days": round(90 * math.log(2), 1), "not_a_probability": True}
        conn.execute("UPDATE founder_candidates SET founder_transition_score=?,overall_priority_score=?,signal_confidence=?,updated_at=? WHERE candidate_id=?", (transition_score, overall, signal_confidence, calculated, candidate_id))
        conn.execute("INSERT INTO founder_score_snapshots(snapshot_id,candidate_id,talent_score,transition_score,glasswing_fit_score,identity_confidence,signal_confidence,overall_score,explanation_json,score_version,calculated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (str(uuid.uuid4()), candidate_id, talent, transition_score, fit, identity, signal_confidence, overall, _json(explanation), SIGNAL_VERSION, calculated))
        conn.commit()
    return {"talent_score": talent, "founder_transition_score": transition_score, "glasswing_fit_score": fit, "identity_confidence": identity, "signal_confidence": signal_confidence, "overall_priority_score": overall, "explanation": explanation}


def analyst_action(candidate_id: str, action: str, value: str = "", note: str = "", *, analyst: str = "admin") -> dict[str, Any]:
    now = utc_now()
    with _store() as conn:
        row = conn.execute("SELECT * FROM founder_candidates WHERE candidate_id=?", (candidate_id,)).fetchone()
        if not row: raise ValueError("Candidate not found")
        updates: dict[str, Any] = {}; previous = ""
        if action == "status":
            if value not in ANALYST_STATUSES: raise ValueError("Invalid analyst status")
            updates["analyst_status"] = value; previous = str(row["analyst_status"])
        elif action == "monitoring":
            if value not in {"active", "inactive", "muted", "stopped"}: raise ValueError("Invalid monitoring status")
            updates["monitoring_status"] = value; previous = str(row["monitoring_status"])
        elif action == "owner": updates["analyst_owner"] = clean(value); previous = str(row["analyst_owner"])
        elif action == "next_review": updates["next_review_at"] = clean(value); previous = str(row["next_review_at"])
        elif action == "outreach": updates["outreach_completed_at"] = now; updates["outreach_outcome"] = clean(value); previous = str(row["outreach_outcome"])
        elif action == "archive": updates["archived_at"] = now; updates["monitoring_status"] = "stopped"; previous = str(row["archived_at"])
        elif action == "note": pass
        else: raise ValueError("Invalid analyst action")
        if updates:
            assignments = ",".join(f"{key}=?" for key in updates); conn.execute(f"UPDATE founder_candidates SET {assignments},updated_at=? WHERE candidate_id=?", (*updates.values(), now, candidate_id))
        conn.execute("INSERT INTO founder_analyst_reviews(candidate_id,action,previous_value,new_value,note,analyst,created_at) VALUES (?,?,?,?,?,?,?)", (candidate_id, action, previous, value, clean(note), analyst, now))
        conn.commit()
    return candidate_profile(candidate_id) or {}


def review_signal(signal_id: str, disposition: str, note: str = "") -> None:
    if disposition not in {"confirmed", "dismissed", ""}: raise ValueError("Invalid signal disposition")
    with _store() as conn:
        signal = conn.execute("SELECT candidate_id FROM founder_signals WHERE signal_id=?", (signal_id,)).fetchone()
        if not signal: raise ValueError("Signal not found")
        conn.execute("UPDATE founder_signals SET analyst_disposition=? WHERE signal_id=?", (disposition, signal_id))
        conn.execute("INSERT INTO founder_analyst_reviews(candidate_id,action,new_value,note,created_at) VALUES (?,?,?,?,?)", (signal["candidate_id"], "signal_review", disposition, clean(note), utc_now()))
        conn.commit()
    recalculate_scores(str(signal["candidate_id"]))


def retract_superseded_signals(*, dry_run: bool = False) -> dict[str, Any]:
    """Withdraw signals whose detector has since been corrected.

    Signals are insert-only and their dedupe key carries the date, so fixing a
    detector stops tomorrow's false positive and leaves yesterday's in place,
    still counting toward the score and still reading on the page as evidence.

    Retraction is by rule version, not by re-running the corrected detector
    over old rows. Re-running would retract true signals that have merely aged
    out: focus_shift reads the last thirty days, so somebody who genuinely left
    their job in March has a new baseline by now and would fail the test that
    was right to fire back then. What a version bump says is narrower and
    checkable -- this row was produced by a rule we have since found to be
    wrong -- and it is the only claim available without the event counts, which
    were never stored on the row.

    Marked dismissed rather than deleted, so the row and its evidence stay
    readable, and logged as its own action: this is a rule withdrawing its own
    output, and recording it as an analyst review would put words in a human's
    mouth. Only explicit human decisions belong in that record as judgements.
    """
    retracted: list[dict[str, str]] = []
    with _store() as conn:
        for version, types in SUPERSEDED_SIGNALS.items():
            placeholders = ",".join("?" for _ in types)
            rows = conn.execute(
                "SELECT signal_id,candidate_id,signal_type,evidence FROM "
                f"founder_signals WHERE rule_version=? AND signal_type IN ({placeholders}) "
                "AND analyst_disposition<>'dismissed'",
                (version, *types),
            ).fetchall()
            for row in rows:
                retracted.append({
                    "signal_id": str(row["signal_id"]),
                    "candidate_id": str(row["candidate_id"]),
                    "signal_type": str(row["signal_type"]),
                    "evidence": str(row["evidence"] or ""),
                    "rule_version": version,
                })
        if dry_run or not retracted:
            return {"retracted": len(retracted), "signals": retracted,
                    "dry_run": dry_run}
        now = utc_now()
        for item in retracted:
            conn.execute(
                "UPDATE founder_signals SET analyst_disposition='dismissed' "
                "WHERE signal_id=?",
                (item["signal_id"],),
            )
            conn.execute(
                "INSERT INTO founder_analyst_reviews"
                "(candidate_id,action,previous_value,new_value,note,analyst,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    item["candidate_id"],
                    "signal_retracted_by_rule",
                    item["rule_version"],
                    SIGNAL_VERSION,
                    f'{item["signal_type"]} was produced by {item["rule_version"]}, '
                    f'whose detector {SIGNAL_VERSION} corrected.',
                    "",
                    now,
                ),
            )
        conn.commit()
    # Outside the write transaction: recalculate_scores opens its own.
    for candidate_id in sorted({item["candidate_id"] for item in retracted}):
        recalculate_scores(candidate_id)
    return {"retracted": len(retracted), "signals": retracted, "dry_run": False}


def create_monitoring_job(job_type: str, candidate_id: str = "") -> str:
    job_id = str(uuid.uuid4())
    with _store() as conn:
        conn.execute("INSERT INTO founder_monitoring_jobs(job_id,job_type,status,candidate_id,created_at) VALUES (?,?,?,?,?)", (job_id, job_type, "queued", candidate_id, utc_now())); conn.commit()
    return job_id


def run_monitoring_job(job_id: str, *, client: GitHubClient | None = None) -> dict[str, Any]:
    with _store() as conn:
        job = conn.execute("SELECT * FROM founder_monitoring_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not job: raise ValueError("Job not found")
        conn.execute("UPDATE founder_monitoring_jobs SET status='running',started_at=? WHERE job_id=?", (utc_now(), job_id)); conn.commit()
    job_type = str(job["job_type"]); candidate_id = str(job["candidate_id"]); attempted = succeeded = failed = 0; errors = []
    if job_type == "talent_discovery":
        try:
            result = discover_talent(client=client)
            attempted = int(result["searched_targets"]); succeeded = int(result["discovered"])
            ids = []
        except Exception as exc:
            attempted = 1; failed = 1; errors.append({"candidate_id": "", "error": str(exc)}); ids = []
    elif candidate_id: ids = [candidate_id]
    elif job_type == "pedigree_enrichment":
        with _store(True) as conn:
            limit = int(founder_settings().get("maximum_people_per_run", 200))
            ids = [str(row["candidate_id"]) for row in conn.execute("SELECT candidate_id FROM founder_candidates WHERE synthetic=FALSE AND archived_at='' ORDER BY last_verified_at,created_at LIMIT ?", (limit,)).fetchall()]
    else:
        with _store(True) as conn:
            limit = int(founder_settings().get("maximum_people_per_run", 200))
            # Collect whoever has a resolved GitHub identity, unless somebody
            # has deliberately stopped them.
            #
            # This used to require monitoring_status='active', which only
            # verify_identity ever sets -- the manual resolution path.
            # discover_talent and reconcile_direct_github_discoveries both
            # write identity_confidence=100 and monitoring_status='inactive'
            # in the same statement, so every person found automatically
            # arrived fully identified and permanently ineligible. Measured
            # live: 174 candidates, 174 with a verified identity, 0 active, 0
            # events ever collected, against five consecutive daily jobs that
            # all reported "completed".
            #
            # So the flag is an opt-out now rather than a precondition nobody
            # sets. Archiving (which writes 'stopped') and an analyst pause
            # both still take effect.
            ids = [str(row["candidate_id"]) for row in conn.execute(
                "SELECT DISTINCT c.candidate_id, c.last_refreshed_at FROM founder_candidates c "
                "JOIN verified_github_identities v ON v.candidate_id = c.candidate_id "
                "WHERE c.archived_at='' AND c.monitoring_status NOT IN ('stopped','paused') "
                "ORDER BY c.last_refreshed_at LIMIT ?", (limit,)
            ).fetchall()]
    for item in ids:
        attempted += 1
        try:
            if job_type == "identity_resolution": resolve_github_identity(item, client=client)
            elif job_type == "pedigree_enrichment": enrich_pedigree(item)
            elif job_type in {"github_collection", "full_refresh"}: collect_candidate(item, client=client)
            elif job_type == "score_recalculation": recalculate_scores(item)
            else: raise ValueError("Unsupported job type")
            succeeded += 1
        except Exception as exc:
            failed += 1; errors.append({"candidate_id": item, "error": str(exc)})
            with _store() as conn:
                conn.execute("INSERT INTO founder_collection_errors(job_id,candidate_id,source,error_type,message,retryable,occurred_at) VALUES (?,?,?,?,?,?,?)", (job_id, item, "github" if "github" in job_type else "local", type(exc).__name__, str(exc)[:1000], getattr(exc, "retryable", True), utc_now()))
                # A 404 on a user endpoint means that login no longer exists --
                # renamed or deleted account. Nothing about tomorrow makes it
                # exist again, so left alone it fails every run forever and
                # holds the job at "partial" permanently, which is how a status
                # stops being worth reading. Paused rather than archived: the
                # person may be real and merely renamed, and pausing is the
                # existing opt-out the eligibility query already honours.
                #
                # 404 specifically, not every non-retryable error. A 401 is
                # also non-retryable and means the token is bad -- it would
                # arrive for every candidate in the run and pause all 174
                # people on one expired secret.
                if getattr(exc, "status", 0) == 404:
                    conn.execute(
                        "UPDATE founder_candidates SET monitoring_status='paused' "
                        "WHERE candidate_id=? AND monitoring_status NOT IN ('stopped','paused')",
                        (item,),
                    )
                    conn.execute(
                        "INSERT INTO founder_analyst_reviews"
                        "(candidate_id,action,previous_value,new_value,note,analyst,created_at) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (item, "monitoring_paused_by_rule", "", "paused",
                         "GitHub returned 404 for this login; the account was "
                         "renamed or deleted. Resolve the identity to resume.",
                         "", utc_now()),
                    )
                conn.commit()
    # Withdraw what a corrected detector wrote before it was corrected. Here
    # rather than in a migration because it needs the scores recomputed after
    # it, which is this module's work, and because the daily job is the thing
    # that reliably runs -- a one-off script only helps if somebody remembers
    # to run it. It is a no-op once the superseded rows are all dismissed.
    retraction: dict[str, Any] = {}
    if job_type in {"github_collection", "full_refresh", "score_recalculation"}:
        try:
            withdrawn = retract_superseded_signals()
            retraction = {"retracted": withdrawn["retracted"]}
        except Exception as exc:
            # A failed retraction must not fail the collection that ran before
            # it: the events are already stored and are the expensive part. It
            # is reported on the job rather than raised, and the next run
            # retries it.
            retraction = {"error": str(exc)[:500]}

    # A job that found nobody to work on is not a success. Reporting it as
    # "completed" is what let a collection job run every day for five days,
    # touch zero people and raise nothing -- the status was indistinguishable
    # from a run that did the work.
    if not attempted:
        status = "no_candidates"
    else:
        status = "completed" if not failed else "partial" if succeeded else "failed"
    with _store() as conn:
        conn.execute("UPDATE founder_monitoring_jobs SET status=?,completed_at=?,attempted=?,succeeded=?,failed=?,detail_json=?,error=? WHERE job_id=?", (status, utc_now(), attempted, succeeded, failed, _json({"errors": errors[:50], **({"retraction": retraction} if retraction else {})}), errors[0]["error"] if errors and not succeeded else "", job_id)); conn.commit()
    return {"job_id": job_id, "status": status, "attempted": attempted, "succeeded": succeeded, "failed": failed, "errors": errors}


def founder_overview() -> dict[str, Any]:
    with _store(True) as conn:
        counts = dict(conn.execute("SELECT count(*) AS total,sum(CASE WHEN monitoring_status='active' THEN 1 ELSE 0 END) AS monitored,sum(CASE WHEN overall_priority_score>=60 THEN 1 ELSE 0 END) AS priority,sum(CASE WHEN synthetic THEN 1 ELSE 0 END) AS synthetic FROM founder_candidates WHERE archived_at='' ").fetchone())
        counts["identity_review"] = int(conn.execute("SELECT count(*) AS total FROM github_identity_candidates WHERE resolution_status IN ('needs_review','ambiguous')").fetchone()["total"])
        counts["signals_30d"] = int(conn.execute("SELECT count(*) AS total FROM founder_signals WHERE event_at>=? AND analyst_disposition<>'dismissed'", ((datetime.now(timezone.utc)-timedelta(days=30)).isoformat(),)).fetchone()["total"])
        last_job = conn.execute("SELECT * FROM founder_monitoring_jobs ORDER BY created_at DESC LIMIT 1").fetchone()
    return {"counts": {key: int(value or 0) for key, value in counts.items()}, "github": {"configured": bool(os.getenv("GITHUB_TOKEN")), "last_job": dict(last_job) if last_job else None}}


def seed_demo_data() -> dict[str, int]:
    """Idempotent synthetic scenarios for immediate UI evaluation."""
    scenarios = [
        ("Maya Chen (Synthetic)", "Anthropic", "Research Engineer", 91, 88, "maya-synthetic", "priority"),
        ("Owen Brooks (Synthetic)", "NVIDIA", "Principal Engineer", 88, 84, "owen-synthetic", "monitoring"),
        ("Priya Raman (Synthetic)", "MIT CSAIL", "Research Scientist", 90, 92, "priya-synthetic", "priority"),
        ("Lucas Ford (Synthetic)", "Databricks", "Staff Engineer", 82, 78, "lucas-synthetic", "watch"),
        ("Elena Park (Synthetic)", "Anduril", "Technical Product Lead", 85, 89, "elena-synthetic", "contact_now"),
        ("Noah Davis (Synthetic)", "Google DeepMind", "Software Engineer", 86, 80, "noah-synthetic", "false_positive"),
        ("Avery Singh (Synthetic)", "Stanford", "Robotics Researcher", 89, 95, "avery-synthetic", "needs_research"),
        ("Jordan Lee (Synthetic)", "Microsoft Research", "Research Engineer", 84, 83, "jordan-synthetic", "monitoring"),
    ]
    created = 0; now = datetime.now(timezone.utc)
    for index, (name, employer, title, talent, fit, username, status) in enumerate(scenarios):
        with _store(True) as conn:
            exists = conn.execute("SELECT candidate_id FROM founder_candidates WHERE github_username=?", (username,)).fetchone()
        if exists:
            # Demo identities are intentionally fake and must never spend API
            # quota when a real GitHub token is configured later.
            with _store() as conn:
                conn.execute(
                    "UPDATE founder_candidates SET monitoring_status='inactive' "
                    "WHERE candidate_id=? AND synthetic=TRUE",
                    (exists["candidate_id"],),
                )
                conn.commit()
            continue
        result = create_candidate({"full_name": name, "current_employer": employer, "current_title": title, "school": "Stanford" if index % 2 else "MIT", "technical_categories": ["AI infrastructure" if index % 2 else "Robotics"], "github_username": username, "github_profile_url": f"https://github.com/{username}", "talent_score": talent, "glasswing_fit_score": fit, "identity_confidence": 96 if index != 6 else 74, "monitoring_status": "inactive", "analyst_status": status, "source_type": "synthetic_demo", "source_url": f"https://example.invalid/{username}", "synthetic": True})
        if result["status"] != "created": continue
        candidate_id = result["candidate_id"]; created += 1
        with _store() as conn:
            verify_identity(conn, candidate_id, username, 96 if index != 6 else 74, "synthetic")
            conn.execute("UPDATE founder_candidates SET monitoring_status='inactive' WHERE candidate_id=?", (candidate_id,))
            if index == 6:
                conn.execute("INSERT INTO github_identity_candidates(identity_id,candidate_id,username,profile_url,positive_score,confidence_score,evidence_json,contradictions_json,resolution_status,resolution_method,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (str(uuid.uuid4()), candidate_id, "avery-alt-synthetic", "https://github.com/avery-alt-synthetic", 73, 73, _json(["Name matches"]), _json(["Employer unavailable"]), "ambiguous", "synthetic", utc_now()))
            conn.commit()
        event_count = 3 if index in {1, 5} else 18 + index
        events = [{"id": f"synthetic-{index}-{number}", "type": "PushEvent" if number % 3 else "CreateEvent", "repo": {"name": f"{username}/venture-{number // 5}"}, "created_at": (now - timedelta(days=number % 20)).isoformat().replace("+00:00", "Z")} for number in range(event_count)]
        ingest_github_events(candidate_id, username, events)
        with _store() as conn:
            if index in {0, 2, 4, 7}:
                repo = f"{username}/vector-foundry"; conn.execute("INSERT INTO founder_github_repositories(repository_id,candidate_id,full_name,owner_login,description,topics_json,primary_language,readme_excerpt,homepage,created_at,updated_at,first_seen_at,last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (f"repo-{index}", candidate_id, repo, username, "Enterprise inference platform", _json(["ai-infrastructure"]), "Python", "Join our design partner waitlist. We are hiring a founding engineer.", f"https://{username}.example", (now-timedelta(days=8)).isoformat(), (now-timedelta(days=2)).isoformat(), utc_now(), utc_now()))
            if index in {0, 4}:
                conn.execute("INSERT INTO founder_github_organizations(organization_id,candidate_id,login,profile_url,first_seen_at,last_seen_at) VALUES (?,?,?,?,?,?)", (f"org-{index}", candidate_id, f"{username}-labs", f"https://github.com/{username}-labs", (now-timedelta(days=10)).isoformat(), utc_now()))
            conn.commit()
        aggregate_activity(candidate_id); calculate_signals(candidate_id); recalculate_scores(candidate_id)
    return {"created": created}
