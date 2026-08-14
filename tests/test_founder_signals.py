from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import classification_store
import founder_signals as fs
import web_api
from fastapi.testclient import TestClient


def setup_store(monkeypatch, tmp_path):
    database = tmp_path / "founders.sqlite3"
    monkeypatch.setattr(classification_store, "CLASSIFICATION_DB", database)
    with classification_store.connect_store(database):
        pass
    return database


def person(name="Jane Smith", **values):
    return {
        "full_name": name,
        "current_employer": "Anthropic",
        "current_title": "Research Engineer",
        "school": "MIT",
        "technical_categories": ["AI infrastructure"],
        "talent_score": 88,
        "glasswing_fit_score": 92,
        **values,
    }


def test_additive_migration_creates_module_tables_and_defaults(monkeypatch, tmp_path):
    database = setup_store(monkeypatch, tmp_path)
    with classification_store.connect_store(database, read_only=True) as conn:
        tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "founder_candidates" in tables
        assert "founder_github_events" in tables
        assert "founder_signals" in tables
        assert conn.execute("SELECT count(*) total FROM founder_settings").fetchone()["total"] >= 10


def test_candidate_creation_and_conservative_dedup(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    first = fs.create_candidate(person(personal_website="https://jane.example"))
    assert first["status"] == "created"
    duplicate = fs.create_candidate(person(personal_website="https://jane.example"))
    assert duplicate["status"] == "needs_review"
    assert "same personal website" in duplicate["duplicates"][0]["reasons"]


def test_ambiguous_names_are_not_merged_without_corroboration(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    fs.create_candidate(person())
    other = fs.create_candidate(person(current_employer="NVIDIA", school="Stanford"))
    assert other["status"] == "created"


def test_csv_import_reports_rows_and_duplicates(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    content = b"Full Name,Employer,Title,GitHub\nJane Smith,Anthropic,Engineer,jane-smith\nJane Smith,Anthropic,Engineer,jane-smith\n"
    result = fs.import_people(content, "people.csv")
    assert result["rows"] == 2
    assert result["created"] == 1
    assert result["identity_review"] == 1


def test_identity_scoring_requires_evidence_and_preserves_contradictions():
    score, evidence, contradictions = fs.identity_confidence(
        person(personal_website="https://jane.dev", geographic_location="Boston"),
        {"login": "janes", "name": "Jane Smith", "company": "Anthropic", "blog": "https://jane.dev", "location": "Boston, MA", "bio": "MIT"},
    )
    assert score == 100
    assert "Personal website matches" in evidence
    bad_score, _, bad = fs.identity_confidence(person(), {"login": "janes", "name": "Jane Smith", "company": "Unrelated Corp"})
    assert bad_score <= 89
    assert bad


def test_multiple_plausible_accounts_are_ambiguous():
    assert fs.identity_state(82, [], candidate_count=2) == "ambiguous"
    assert fs.identity_state(95, []) == "verified"
    assert fs.identity_state(60, []) == "not_found"


def test_direct_github_discovery_identity_is_reconciled_without_monitoring(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person(
        github_username="jane-source",
        source_type="github_official_api",
        monitoring_status="inactive",
    ))["candidate_id"]
    with fs._store() as conn:
        conn.execute(
            "INSERT INTO github_identity_candidates(identity_id,candidate_id,username,confidence_score,resolution_status,resolution_method,created_at) VALUES (?,?,?,?,?,?,?)",
            ("identity", candidate_id, "jane-source", 70, "needs_review", "automatic_discovery", fs.utc_now()),
        )
        conn.commit()
    assert fs.reconcile_direct_github_discoveries() == 1
    profile = fs.candidate_profile(candidate_id)
    assert profile["identity_confidence"] == 100
    assert profile["monitoring_status"] == "inactive"
    assert fs.identity_review_queue() == []


def test_github_pagination_uses_next_link():
    calls = []
    def transport(url, _headers):
        calls.append(url)
        if len(calls) == 1:
            return 200, {"Link": '<https://api.github.com/items?page=2>; rel="next"'}, json.dumps([{"id": 1}]).encode()
        return 200, {}, json.dumps([{"id": 2}]).encode()
    client = fs.GitHubClient("token", transport=transport)
    assert [row["id"] for row in client.paginated("/items")] == [1, 2]


def test_github_retry_recovers_from_transient_failure(monkeypatch):
    calls = []
    monkeypatch.setattr(fs.time, "sleep", lambda _seconds: None)
    def transport(_url, _headers):
        calls.append(1)
        if len(calls) == 1:
            raise fs.GitHubError("temporary", status=503, retryable=True)
        return 200, {}, b'{"ok":true}'
    payload, _ = fs.GitHubClient("token", transport=transport, retries=1).request("/ok")
    assert payload == {"ok": True}
    assert len(calls) == 2


def test_event_ingestion_is_idempotent(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person())["candidate_id"]
    events = [{"id": "event-1", "type": "PushEvent", "repo": {"name": "jane/newco"}, "created_at": "2026-08-10T12:00:00Z"}]
    assert fs.ingest_github_events(candidate_id, "jane", events)["inserted"] == 1
    assert fs.ingest_github_events(candidate_id, "jane", events)["duplicates"] == 1


def test_sparse_activity_marks_baseline_insufficient(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person())["candidate_id"]
    assert fs.activity_windows(candidate_id)["baseline_status"] == "insufficient"


def test_activity_acceleration_and_signal_generation(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person())["candidate_id"]
    now = datetime.now(timezone.utc)
    events = []
    for number in range(12):
        events.append({"id": f"recent-{number}", "type": "PushEvent", "repo": {"name": "jane/newco"}, "created_at": (now-timedelta(days=number % 10)).isoformat()})
    for number in range(6):
        events.append({"id": f"old-{number}", "type": "PushEvent", "repo": {"name": "jane/old"}, "created_at": (now-timedelta(days=60+number*20)).isoformat()})
    fs.ingest_github_events(candidate_id, "jane", events)
    result = fs.calculate_signals(candidate_id, now=now)
    assert result["activity"]["acceleration"] >= 3
    assert result["added"] >= 1


def test_commercial_repository_creates_evidence_signal(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person())["candidate_id"]
    now = datetime.now(timezone.utc).isoformat()
    with fs._store() as conn:
        conn.execute("INSERT INTO founder_github_repositories(repository_id,candidate_id,full_name,description,readme_excerpt,homepage,created_at,updated_at,first_seen_at,last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?)", ("repo", candidate_id, "jane/newco", "Enterprise AI", "Join our design partner waitlist", "https://newco.ai", now, now, now, now)); conn.commit()
    fs.calculate_signals(candidate_id)
    profile = fs.candidate_profile(candidate_id)
    types = {row["signal_type"] for row in profile["signals"]}
    assert {"commercial_language", "new_external_domain"}.issubset(types)


def test_scores_are_explainable_and_time_decayed(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person(identity_confidence=95))["candidate_id"]
    with fs._store() as conn:
        fs._insert_signal(conn, candidate_id, "new_organization", datetime.now(timezone.utc).isoformat(), "Created a new org", "https://github.com/newco", 15, 90, "Team formation"); conn.commit()
    score = fs.recalculate_scores(candidate_id)
    assert score["overall_priority_score"] > 0
    assert score["explanation"]["not_a_probability"] is True
    assert score["explanation"]["signal_contributions"]


def test_analyst_actions_are_audited(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person())["candidate_id"]
    updated = fs.analyst_action(candidate_id, "status", "contact_now", "Strong signal")
    assert updated["analyst_status"] == "contact_now"
    assert updated["reviews"][0]["note"] == "Strong signal"


def test_settings_are_configurable(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    values = fs.update_settings({"target_companies": ["New Lab"], "minimum_alert_threshold": 72, "unknown": "ignored"})
    assert values["target_companies"] == ["New Lab"]
    assert values["minimum_alert_threshold"] == 72
    assert "unknown" not in values


def test_seed_data_is_idempotent(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    assert fs.seed_demo_data()["created"] == 8
    assert fs.seed_demo_data()["created"] == 0
    assert fs.founder_overview()["counts"]["synthetic"] == 8


def test_founder_api_reads_are_public_and_mutations_require_admin(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    client = TestClient(web_api.app)
    assert client.get("/api/founder-signals/overview").status_code == 200
    response = client.post(
        "/api/founder-signals/candidates",
        json={"values": {"full_name": "Protected Person"}},
    )
    assert response.status_code == 401
    assert client.post("/api/auth/login", json={"password": "Glasswing"}).status_code == 200
    response = client.post(
        "/api/founder-signals/candidates",
        json={"values": {"full_name": "Protected Person"}},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "created"
