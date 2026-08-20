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


def test_pedigree_enrichment_requires_affiliation_or_cross_source_identifier(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person(talent_score=40, monitoring_status="inactive"))["candidate_id"]

    class ResearchClient:
        def openalex_authors(self, _name):
            return [{"id":"https://openalex.org/A1","display_name":"Jane Smith","works_count":24,"cited_by_count":900,"last_known_institutions":[{"display_name":"MIT"}],"ids":{"orcid":"https://orcid.org/0000-0001"}}]
        def semantic_scholar_authors(self, _name):
            return [{"authorId":"S1","name":"Jane Smith","url":"https://www.semanticscholar.org/author/S1","affiliations":["MIT"],"paperCount":22,"citationCount":880,"hIndex":14,"externalIds":{"ORCID":"0000-0001"}}]
        def crossref_works(self, _name): return []

    result = fs.enrich_pedigree(candidate_id, client=ResearchClient())
    assert result == {"candidate_id": candidate_id, "matches": 2, "verified": 2, "provider_errors": []}
    profile = fs.candidate_profile(candidate_id)
    assert len(profile["pedigree"]) == 2
    assert all(item["verification_status"] == "verified" for item in profile["pedigree"])
    assert profile["talent_score"] > 40
    assert profile["monitoring_status"] == "inactive"


def test_name_only_pedigree_match_never_auto_verifies(monkeypatch, tmp_path):
    setup_store(monkeypatch, tmp_path)
    candidate_id = fs.create_candidate(person(current_employer="Anthropic", school="", talent_score=40))["candidate_id"]

    class ResearchClient:
        def openalex_authors(self, _name):
            return [{"id":"https://openalex.org/A2","display_name":"Jane Smith","works_count":100,"cited_by_count":5000,"last_known_institutions":[],"ids":{}}]
        def semantic_scholar_authors(self, _name): return []
        def crossref_works(self, _name): return []

    result = fs.enrich_pedigree(candidate_id, client=ResearchClient())
    assert result["verified"] == 0
    profile = fs.candidate_profile(candidate_id)
    assert profile["pedigree"][0]["verification_status"] == "needs_review"
    assert profile["talent_score"] == 40


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


def seed_events(database, candidate_id, per_month, *, owner="openai", now=None):
    """Place `per_month[i]` events in the i-th 30-day window back from now.

    per_month may hold either an int (all events under `owner`) or a dict of
    owner -> count, so a test can move work between organisations.
    """
    now = now or datetime.now(timezone.utc)
    with classification_store.connect_store(database) as conn:
        for index, spec in enumerate(per_month):
            counts = {owner: spec} if isinstance(spec, int) else spec
            when = now - timedelta(days=30 * index + 5)
            for repo_owner, total in counts.items():
                for n in range(total):
                    conn.execute(
                        "INSERT INTO founder_github_events(event_id,candidate_id,username,"
                        "event_type,repo_name,organization,event_created_at,collected_at) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        (f"{candidate_id}-{index}-{repo_owner}-{n}", candidate_id,
                         "someone", "PushEvent", f"{repo_owner}/service", repo_owner,
                         (when - timedelta(minutes=n)).isoformat(), fs.utc_now()),
                    )
        conn.commit()


def test_a_prolific_but_steady_committer_can_trip_the_spike():
    """The case the ratio gate cannot see. Somebody averaging 400 events a
    month needs 1,200 to reach 3x, which never happens -- and every person in
    this table is in that group. A step outside their own variation does.
    """
    counts = [600, 400, 405, 395, 402, 398, 401]
    assert max(counts[1:]) / min(counts[1:]) < 1.1  # a flat history
    ratio = counts[0] / (sum(counts[1:]) / 6)
    assert ratio < 3.0, "the old gate would not fire here"


def test_the_spike_is_measured_against_the_persons_own_variation(monkeypatch, tmp_path):
    database = setup_store(monkeypatch, tmp_path)
    steady = fs.create_candidate(person("Steady Sam"))["candidate_id"]
    seed_events(database, steady, [600, 400, 405, 395, 402, 398, 401])
    spike = fs.commit_spike(steady)
    assert spike["is_spike"] is True
    assert spike["ratio"] < 3.0, "and the ratio gate would have missed it"
    assert spike["z_score"] >= 3.0


def test_a_dormant_account_does_not_spike_on_a_handful_of_events(monkeypatch, tmp_path):
    database = setup_store(monkeypatch, tmp_path)
    quiet = fs.create_candidate(person("Quiet Quinn"))["candidate_id"]
    # 6 events against a baseline of ~1 clears the old 3x gate at 6 events.
    seed_events(database, quiet, [6, 1, 1, 0, 1, 1, 0])
    spike = fs.commit_spike(quiet)
    assert spike["is_spike"] is False


def test_one_earlier_burst_does_not_hide_a_later_transition(monkeypatch, tmp_path):
    """A mean is dragged by a single busy month, which raises the bar for the
    rest of the year and hides exactly the transition being looked for. A
    median is not."""
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(person("Bursty Bea"))["candidate_id"]
    seed_events(database, who, [90, 10, 12, 300, 11, 9, 10])
    spike = fs.commit_spike(who)
    assert spike["baseline_median"] <= 12, "the 300 must not set the baseline"
    assert spike["is_spike"] is True


def test_an_erratic_history_is_not_a_step_change(monkeypatch, tmp_path):
    """Same lift, but this person always swings that much -- so it is their
    normal, and calling it a transition would be noise."""
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(person("Erratic Eli"))["candidate_id"]
    seed_events(database, who, [120, 10, 200, 15, 180, 20, 150])
    assert fs.commit_spike(who)["is_spike"] is False


def test_busy_inside_the_day_job_is_not_a_departure(monkeypatch, tmp_path):
    """Volume doubling inside the employer's own org is a heavy quarter."""
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(person("Busy Bao"))["candidate_id"]
    seed_events(database, who, [200, 100, 100, 100, 100, 100, 100], owner="openai")
    shift = fs.focus_shift(who)
    assert shift["anchor"] == "openai"
    assert shift["is_shift"] is False


def test_work_moving_to_a_new_org_is_a_departure(monkeypatch, tmp_path):
    """The actual spin-out shape: the org that held most of the work no longer
    does, and the work has moved somewhere absent from their history."""
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(person("Leaving Lee"))["candidate_id"]
    seed_events(database, who, [
        {"openai": 8, "vectorforge": 70},
        {"openai": 100}, {"openai": 95}, {"openai": 105},
        {"openai": 98}, {"openai": 102}, {"openai": 99},
    ])
    shift = fs.focus_shift(who)
    assert shift["anchor"] == "openai"
    assert shift["baseline_share"] >= 0.9
    assert shift["current_share"] <= 0.2
    assert shift["new_owners"] == ["vectorforge"]
    assert shift["is_shift"] is True


def test_the_anchor_is_whatever_dominated_rather_than_a_matched_name(monkeypatch, tmp_path):
    """It never matches the stored employer against a GitHub org, because that
    fails on every abbreviation and rename -- "anthropics" against
    "Anthropic", "openai" against "OpenAI"."""
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(
        person("Renamed Ren", current_employer="Anthropic")
    )["candidate_id"]
    seed_events(database, who, [
        {"anthropics": 5, "newthing": 60},
        {"anthropics": 100}, {"anthropics": 100}, {"anthropics": 100},
        {"anthropics": 100}, {"anthropics": 100}, {"anthropics": 100},
    ])
    shift = fs.focus_shift(who)
    assert shift["anchor"] == "anthropics"
    assert shift["is_shift"] is True


def test_relocating_effort_is_a_departure_without_being_a_spike(monkeypatch, tmp_path):
    """The two detectors answer different questions and must stay independent.
    Here the work moves out of the day job but the total does not rise -- the
    same hours, spent elsewhere. That is a departure signal and not a step
    change, and conflating them would either lose it or invent a spike.
    """
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(person("Moving Mo"))["candidate_id"]
    seed_events(database, who, [
        {"openai": 8, "solstack": 90},
        {"openai": 100}, {"openai": 98}, {"openai": 102},
        {"openai": 99}, {"openai": 101}, {"openai": 100},
    ])
    assert fs.commit_spike(who)["is_spike"] is False
    assert fs.focus_shift(who)["is_shift"] is True


def test_both_detectors_reach_the_score(monkeypatch, tmp_path):
    """The full spin-out shape: working considerably more, and elsewhere."""
    database = setup_store(monkeypatch, tmp_path)
    who = fs.create_candidate(person("Spinning Sol"))["candidate_id"]
    seed_events(database, who, [
        {"openai": 8, "solstack": 190},
        {"openai": 100}, {"openai": 98}, {"openai": 102},
        {"openai": 99}, {"openai": 101}, {"openai": 100},
    ])
    result = fs.calculate_signals(who)
    assert result["added"] >= 2
    with classification_store.connect_store(database, read_only=True) as conn:
        kinds = {
            row["signal_type"] for row in conn.execute(
                "SELECT signal_type FROM founder_signals WHERE candidate_id=?",
                (who,),
            )
        }
    assert "commit_step_change" in kinds
    assert "focus_left_the_day_job" in kinds
    scored = fs.recalculate_scores(who)
    assert scored["founder_transition_score"] > 0
