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


SIGNAL_VERSION = "founder-signals-v1"
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
        "maximum_people_per_run", "automatic_monitoring_confidence", "event_retention_days",
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
                    conn.execute("INSERT INTO github_identity_candidates(identity_id,candidate_id,username,profile_url,positive_score,confidence_score,evidence_json,contradictions_json,resolution_status,resolution_method,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id,username) DO NOTHING", (str(uuid.uuid4()), candidate_id, login, clean(profile.get("html_url", "")), 70, 70, _json([f'Public GitHub bio matched configured target: {target}']), _json([]), "needs_review", "automatic_discovery", utc_now())); conn.commit()
                review += 1
    return {"discovered": discovered, "identity_review": review, "searched_targets": len(targets)}


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
                "INSERT INTO founder_github_repositories(repository_id,candidate_id,full_name,owner_login,description,topics_json,primary_language,homepage,created_at,updated_at,first_seen_at,last_seen_at,archived) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(candidate_id,full_name) DO UPDATE SET description=excluded.description,topics_json=excluded.topics_json,primary_language=excluded.primary_language,homepage=excluded.homepage,updated_at=excluded.updated_at,last_seen_at=excluded.last_seen_at,archived=excluded.archived",
                (repo_id, candidate_id, full_name, clean((repo.get("owner") or {}).get("login", "")), clean(repo.get("description", "")), _json(repo.get("topics", [])), clean(repo.get("language", "")), clean(repo.get("homepage", "")), clean(repo.get("created_at", "")), clean(repo.get("updated_at", "")), now, now, bool(repo.get("archived", False))),
            )
        for org in orgs:
            login = clean(org.get("login", "")); org_id = str(org.get("id") or login)
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
        transition_score = min(100, round(transition))
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
    else:
        with _store(True) as conn:
            limit = int(founder_settings().get("maximum_people_per_run", 200))
            ids = [str(row["candidate_id"]) for row in conn.execute("SELECT candidate_id FROM founder_candidates WHERE monitoring_status='active' AND archived_at='' ORDER BY last_refreshed_at LIMIT ?", (limit,)).fetchall()]
    for item in ids:
        attempted += 1
        try:
            if job_type == "identity_resolution": resolve_github_identity(item, client=client)
            elif job_type in {"github_collection", "full_refresh"}: collect_candidate(item, client=client)
            elif job_type == "score_recalculation": recalculate_scores(item)
            else: raise ValueError("Unsupported job type")
            succeeded += 1
        except Exception as exc:
            failed += 1; errors.append({"candidate_id": item, "error": str(exc)})
            with _store() as conn:
                conn.execute("INSERT INTO founder_collection_errors(job_id,candidate_id,source,error_type,message,retryable,occurred_at) VALUES (?,?,?,?,?,?,?)", (job_id, item, "github" if "github" in job_type else "local", type(exc).__name__, str(exc)[:1000], getattr(exc, "retryable", True), utc_now())); conn.commit()
    status = "completed" if not failed else "partial" if succeeded else "failed"
    with _store() as conn:
        conn.execute("UPDATE founder_monitoring_jobs SET status=?,completed_at=?,attempted=?,succeeded=?,failed=?,detail_json=?,error=? WHERE job_id=?", (status, utc_now(), attempted, succeeded, failed, _json({"errors": errors[:50]}), errors[0]["error"] if errors and not succeeded else "", job_id)); conn.commit()
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
