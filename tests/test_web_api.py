from fastapi.testclient import TestClient
import pytest

import web_api
from types import SimpleNamespace


client = TestClient(web_api.app)


@pytest.fixture(autouse=True)
def admin_session(monkeypatch):
    monkeypatch.setenv("GLASSWING_ADMIN_PASSWORD", "Glasswing")
    response = client.post("/api/auth/login", json={"password": "Glasswing"})
    assert response.status_code == 200
    yield
    client.post("/api/auth/logout")


def test_admin_login_is_server_enforced():
    client.post("/api/auth/logout")
    assert client.get("/api/auth/status").json() == {"admin": False}
    assert client.post("/api/runtime/pipeline", json={"desired": False}).status_code == 401
    assert client.post("/api/auth/login", json={"password": "wrong"}).status_code == 401
    assert client.post("/api/auth/login", json={"password": "Glasswing"}).status_code == 200
    assert client.get("/api/auth/status").json() == {"admin": True}


def test_manual_domain_intake_rejects_incomplete_domain_immediately():
    response = client.post("/api/ai/domain", json={"domain": "hiai"})
    assert response.status_code == 400
    assert "complete domain" in response.json()["detail"]


def test_health_and_spa_are_served():
    assert client.get("/api/healthz").json() == {"status": "ok"}
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert response.headers["x-frame-options"] == "DENY"


def test_api_responses_are_not_cached():
    response = client.get("/api/healthz")
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_overview_uses_lightweight_classification_counts(monkeypatch):
    monkeypatch.setattr(web_api, "_count_cache", None)
    monkeypatch.setattr(
        web_api,
        "classification_bucket_counts",
        lambda: {"priority": 7, "review": 11, "reject": 3},
    )
    monkeypatch.setattr(
        web_api,
        "health_report",
        lambda *args, **kwargs: pytest.fail("homepage must not run full health checks"),
    )

    response = client.get("/api/overview")

    assert response.status_code == 200
    assert response.json() == {
        "overall": "Healthy",
        "counts": {"classifications": 21},
        "buckets": {
            "all": 21,
            "investable": 7,
            "review": 11,
            "denied": 3,
        },
    }


def test_health_summary_is_cached_but_explicit_refresh_bypasses(monkeypatch):
    calls = []
    monkeypatch.setattr(web_api, "_health_cache", None)
    monkeypatch.setattr(
        web_api,
        "health_report",
        lambda **kwargs: calls.append(kwargs) or {"sequence": len(calls)},
    )

    assert client.get("/api/health").json() == {"sequence": 1}
    assert client.get("/api/health").json() == {"sequence": 1}
    assert client.get("/api/health?refresh=1").json() == {"sequence": 2}
    assert calls == [
        {"include_integrity": False},
        {"include_integrity": False},
    ]


def test_landscape_submit_ignores_browser_blank_file_part(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setattr(web_api, "LANDSCAPE_ROOT", tmp_path)
    monkeypatch.setattr(
        web_api,
        "create_landscape_job",
        lambda website, template, materials, notes, count, job_id: captured.update(
            website=website, materials=materials, job_id=job_id
        ) or job_id,
    )
    monkeypatch.setattr(web_api, "record_admin_audit", lambda *args: None)

    response = client.post(
        "/api/competitive-landscapes",
        data={"website": "neyon.ai", "requested_count": "25", "notes": ""},
        files=[("materials", ("", b"", "application/octet-stream"))],
    )

    assert response.status_code == 200
    assert captured["website"] == "neyon.ai"
    assert captured["materials"] == []


def test_classifications_are_server_paginated(monkeypatch):
    monkeypatch.setattr(web_api, "_count_cache", None)
    monkeypatch.setattr(
        web_api,
        "classification_bucket_counts",
        lambda: {"review": 501, "reject": 10, "priority": 3},
    )
    captured = {}

    def query(buckets, **kwargs):
        captured.update({"buckets": buckets, **kwargs})
        return [{"domain": "example.ai", "routing_bucket": "review"}]

    monkeypatch.setattr(web_api, "query_classifications", query)
    monkeypatch.setattr(web_api, "hidden_domain_set", lambda domains: set())
    response = client.get("/api/classifications?bucket=review&page=3&page_size=100&sort=Fit%3A%20low%20to%20high")
    assert response.status_code == 200
    assert response.json()["total"] == 501
    assert captured["limit"] == 100
    assert captured["offset"] == 200
    assert captured["sort"] == "Fit: low to high"


def test_current_run_scope_reaches_counts_and_rows(monkeypatch):
    captured = {}
    monkeypatch.setattr(web_api, "_current_run_id", lambda: "run-123")
    monkeypatch.setattr(
        web_api, "classification_bucket_counts",
        lambda run_id="": captured.update(count_run_id=run_id) or {"priority": 2},
    )
    monkeypatch.setattr(
        web_api, "query_classifications",
        lambda buckets, **kwargs: captured.update(row_run_id=kwargs.get("run_id")) or [],
    )
    response = client.get(
        "/api/classifications?bucket=investable&scope=current&page_size=25"
    )
    assert response.status_code == 200
    assert response.json()["total"] == 2
    assert captured == {"count_run_id": "run-123", "row_run_id": "run-123"}


def test_yes_no_baskets_ignore_pipeline_run_scope(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        web_api,
        "query_training_labels",
        lambda label, **kwargs: captured.update(label=label, **kwargs) or ([], 0),
    )
    monkeypatch.setattr(web_api, "hidden_domain_set", lambda domains: set())

    response = client.get("/api/labels?label=yes&scope=current&page_size=25")

    assert response.status_code == 200
    assert captured["label"] == "yes"
    assert captured["run_id"] == ""


def test_human_label_endpoint_uses_protected_human_source(monkeypatch):
    captured = {}
    movement = {}

    def save(domain, label, rating, **kwargs):
        captured.update(domain=domain, label=label, rating=rating, **kwargs)

    monkeypatch.setattr(web_api, "save_basket_entry", save)
    monkeypatch.setattr(
        web_api, "record_manual_move",
        lambda domains, **kwargs: movement.update(domains=domains, **kwargs),
    )
    response = client.post(
        "/api/labels",
        json={"domain": "Example.ai", "label": "yes", "rating": 87},
    )
    assert response.status_code == 200
    assert captured["source"] == "human"
    assert captured["label"] == "yes"
    assert captured["rating"] == 87
    assert movement["target_bucket"] == "priority"
    assert movement["target_label"] == "yes"

    response = client.post(
        "/api/labels",
        json={"domain": "Reject.ai", "label": "no", "rating": 12,
              "reason": "manual_reject"},
    )
    assert response.status_code == 200
    assert movement["target_bucket"] == "reject"
    assert movement["target_label"] == "no"


def test_tracking_endpoints_persist_and_return_company_details(monkeypatch):
    tracked = set()
    monkeypatch.setattr(
        web_api,
        "classification_record_for_domain",
        lambda domain: {"domain": domain} if domain == "example.ai" else None,
    )
    monkeypatch.setattr(
        web_api,
        "track_domains",
        lambda domains: tracked.update(domains) or len(domains),
    )
    monkeypatch.setattr(
        web_api,
        "untrack_domains",
        lambda domains: len([domain for domain in domains if domain in tracked]),
    )
    monkeypatch.setattr(
        web_api,
        "list_tracked_domains",
        lambda limit=100: [
            {"domain": domain, "tracked_at": "2026-08-11T12:00:00Z"}
            for domain in tracked
        ],
    )
    monkeypatch.setattr(
        web_api,
        "query_classifications_by_domains",
        lambda domains: [
            {"domain": domain, "company_name": "Example", "fit_score": 91}
            for domain in domains
        ],
    )
    monkeypatch.setattr(web_api, "_with_claude", lambda rows: rows)
    monkeypatch.setattr(web_api, "record_admin_audit", lambda *args: None)

    response = client.post("/api/tracking", json={"domain": "Example.ai"})
    assert response.status_code == 200
    assert response.json()["tracked"] is True
    listing = client.get("/api/tracking").json()
    assert listing["total"] == 1
    assert listing["rows"][0]["domain"] == "example.ai"
    assert listing["rows"][0]["tracked"] is True


def test_tracking_status_and_kill_workflow(monkeypatch):
    status_change = {}
    basket = {}
    movement = {}
    hidden = []
    untracked = []
    monkeypatch.setattr(
        web_api,
        "set_tracking_status",
        lambda domain, status: status_change.update(domain=domain, status=status) or True,
    )
    monkeypatch.setattr(
        web_api,
        "classification_record_for_domain",
        lambda domain: {"domain": domain, "fit_score": 88},
    )
    monkeypatch.setattr(
        web_api,
        "save_basket_entry",
        lambda domain, label, rating, **kwargs: basket.update(
            domain=domain, label=label, rating=rating, **kwargs
        ),
    )
    monkeypatch.setattr(
        web_api,
        "record_manual_move",
        lambda domains, **kwargs: movement.update(domains=domains, **kwargs),
    )
    monkeypatch.setattr(web_api, "hide_domains", lambda domains: hidden.extend(domains))
    monkeypatch.setattr(web_api, "untrack_domains", lambda domains: untracked.extend(domains))
    monkeypatch.setattr(web_api, "record_admin_audit", lambda *args: None)

    response = client.patch(
        "/api/tracking/example.ai", json={"status": "reached_out"}
    )
    assert response.status_code == 200
    assert status_change == {"domain": "example.ai", "status": "reached_out"}

    response = client.patch(
        "/api/tracking/example.ai", json={"status": "meeting"}
    )
    assert response.status_code == 200
    assert status_change == {"domain": "example.ai", "status": "meeting"}

    response = client.post("/api/tracking/example.ai/kill")
    assert response.status_code == 200
    assert response.json()["rating"] == 88
    assert basket["label"] == "yes"
    assert basket["rating"] == 88
    assert movement["target_label"] == "yes"
    assert movement["target_bucket"] == "priority"
    assert hidden == ["example.ai"]
    assert untracked == ["example.ai"]


def test_worker_control_is_explicit_and_scoped(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        web_api,
        "request_services",
        lambda changes, requested_by: captured.update(
            changes=changes, requested_by=requested_by
        ),
    )
    response = client.post("/api/runtime/pipeline", json={"desired": False})
    assert response.status_code == 200
    assert captured["changes"] == {"pipeline": False}
    assert captured["requested_by"] == "react_web"


def test_certstream_status_exposes_pause_request(monkeypatch):
    monkeypatch.setattr(web_api, "shared_pipeline_backend_available", lambda: True)
    monkeypatch.setattr(
        web_api, "read_pipeline_status",
        lambda: {"running": True, "stage": "screening"},
    )
    monkeypatch.setattr(
        web_api, "read_control",
        lambda: {"desired": {"pipeline": False}},
    )
    response = client.get("/api/certstream/status")
    assert response.status_code == 200
    assert response.json()["running"] is True
    assert response.json()["desired_running"] is False
    assert response.json()["pause_requested"] is True


def test_certstream_status_reconciles_completed_domains_with_retained_candidates(monkeypatch):
    class QueueConnection:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return {
                "total": 190,
                "awaiting_first_attempt": 10,
                "processing": 4,
                "retrying": 5,
                "complete": 171,
                "failed": 0,
                "attempts": 210,
            }

        def close(self):
            pass

    monkeypatch.setattr(web_api, "shared_pipeline_backend_available", lambda: True)
    monkeypatch.setattr(
        web_api,
        "read_pipeline_status",
        lambda: {"running": True, "stage": "screening", "run_id": "run-1"},
    )
    monkeypatch.setattr(web_api, "connect_pipeline_db", QueueConnection)
    # "all" deliberately exceeds investable+review here: it also covers hard
    # denials and unreadable error pages, neither of which is a retained
    # candidate. Confirmed live: a run reporting 13 retained was really 7 ghost
    # rows plus 6 rejects, i.e. zero real candidates.
    monkeypatch.setattr(
        web_api,
        "_bucket_counts_for_run",
        lambda _run_id: {
            "all": 13, "investable": 1, "review": 2, "denied": 6, "unreadable": 7,
        },
    )
    monkeypatch.setattr(web_api, "read_control", lambda: {"desired": {"pipeline": True}})

    response = client.get("/api/certstream/status")

    assert response.status_code == 200
    assert response.json()["current_run_queue"]["complete"] == 171
    assert response.json()["retained_in_buckets"] == 3
    assert response.json()["discarded_after_screening"] == 168


def test_unknown_bucket_and_worker_are_rejected():
    assert client.get("/api/classifications?bucket=wat").status_code == 400
    assert client.post("/api/runtime/wat", json={"desired": True}).status_code == 404


def test_zone_stats_report_retained_and_discarded_separately(monkeypatch):
    class ZoneConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(web_api, "zone_connect", lambda *args, **kwargs: ZoneConnection())
    monkeypatch.setattr(
        web_api,
        "source_bucket_counts",
        lambda _prefixes: {"priority": 4, "review": 1},
    )
    monkeypatch.setattr(
        web_api,
        "zone_diagnostics",
        lambda conn: {
            "queued_domains": 1000,
            "attempted": 300,
            "low_priority_quarantined": 400,
            "network_success": 40,
            "network_failed": 20,
            "preflight_rejected": 240,
            "preflight_promising": 60,
            "success": 5,
            "searchable_pages": 5,
        },
    )
    monkeypatch.setattr(
        web_api,
        "zone_worker_state",
        lambda conn: {
            "crawler_status": "crawling_com",
            "crawler_phase": "preflight",
            "last_batch_domains_per_second": "42.5",
            "rolling_domains_per_second": "30.25",
        },
    )

    result = client.get("/api/corpus/zone/stats").json()

    assert result["pending"] == 300
    assert result["processed"] == 700
    assert result["retained"] == 5
    assert result["discarded"] == 695
    assert result["failed"] == 20
    assert result["live"] == 300
    assert result["live_rate"] == 100.0
    assert result["relevance_rate"] == 20.0
    assert result["batch_rate"] == 30.25
    assert result["instantaneous_batch_rate"] == 42.5
    assert result["investable"] == 4
    assert result["needs_review"] == 1


def test_zone_candidates_return_only_qualified_retained_queue(monkeypatch):
    monkeypatch.setattr(
        web_api,
        "source_bucket_counts",
        lambda prefixes: {"priority": 2, "review": 3, "reject": 90},
    )
    captured = {}

    def query(prefixes, buckets, **kwargs):
        captured.update(prefixes=prefixes, buckets=buckets, **kwargs)
        return [{"domain": "candidate.app", "routing_bucket": "review"}]

    monkeypatch.setattr(web_api, "query_source_classifications", query)
    monkeypatch.setattr(web_api, "hidden_domain_set", lambda domains: set())
    monkeypatch.setattr(web_api, "tracked_domain_set", lambda domains: set())
    monkeypatch.setattr(web_api, "_with_claude", lambda rows: rows)

    result = client.get("/api/corpus/zone/candidates?limit=25").json()

    assert result["total"] == 5
    assert result["investable"] == 2
    assert result["needs_review"] == 3
    assert captured["buckets"] == ("priority", "review")


def test_crawler_company_tables_are_source_scoped_and_paged(monkeypatch):
    monkeypatch.setattr(web_api, "source_bucket_counts", lambda prefixes: {"review": 240})
    captured = {}

    def query(prefixes, buckets, **kwargs):
        captured.update(prefixes=prefixes, buckets=buckets, **kwargs)
        return [{"domain": "crawler.ai", "routing_bucket": "review"}]

    monkeypatch.setattr(web_api, "query_source_classifications", query)
    response = client.get("/api/corpus/ai?bucket=review&page=2&page_size=100")
    assert response.status_code == 200
    assert response.json()["total"] == 240
    assert captured["prefixes"] == ("crawler_cache",)
    assert captured["offset"] == 100


def test_search_snippets_strip_untrusted_html_but_keep_highlights():
    rows = web_api._safe_corpus_rows([{
        "snippet": '<script>alert(1)</script><mark>match</mark><img src=x>',
    }])
    assert rows[0]["snippet"] == "alert(1)<mark>match</mark>"


def test_ai_search_always_returns_a_rows_array(monkeypatch):
    monkeypatch.setattr(web_api, "search_companies", lambda *args, **kwargs: [])
    monkeypatch.setattr(web_api, "search_stats", lambda *args, **kwargs: {"domains": 12})
    response = client.get("/api/ai/search?q=robotics")
    assert response.status_code == 200
    assert response.json()["rows"] == []
    assert response.json()["stats"]["domains"] == 12


def test_ai_corpus_metrics_expose_real_outcomes_and_worker_heartbeat(monkeypatch):
    class _Cursor:
        def fetchone(self):
            return {"updated_at": "2026-08-10T20:00:00Z"}

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args):
            return _Cursor()

    monkeypatch.setattr(web_api, "ai_connect", lambda *args, **kwargs: _Connection())
    monkeypatch.setattr(
        web_api,
        "ai_diagnostics",
        lambda conn: {
            "domains": 10,
            "pending": 2,
            "crawled": 8,
            "successful": 6,
            "failed": 2,
        },
    )
    monkeypatch.setattr(
        web_api,
        "ai_worker_state",
        lambda conn: {"crawler_status": "error_retrying", "last_crawl_error": "deadlock"},
    )
    monkeypatch.setattr(web_api, "search_stats", lambda *args: {"indexed_pages": 14})

    response = client.get("/api/corpus/ai/stats")

    assert response.status_code == 200
    assert response.json() == {
        "available": 10,
        "pending": 2,
        "processed": 8,
        "crawled": 6,
        "failed": 2,
        "searchable": 14,
        "crawler_status": "error_retrying",
        "worker_status": "unknown",
        "last_error": "deadlock",
        "updated_at": "2026-08-10T20:00:00Z",
    }


def test_manual_claude_review_does_not_repeat_completed_domain(monkeypatch):
    monkeypatch.setattr(
        web_api,
        "claude_decisions_for_domains",
        lambda domains: {"reviewed.ai": {"domain": "reviewed.ai", "decision": "KEEP", "fit_score": 91}},
    )
    response = client.post("/api/claude/review/reviewed.ai")
    assert response.status_code == 200
    assert response.json()["status"] == "already_reviewed"
    assert response.json()["decision"] == "KEEP"


def test_manual_claude_review_requires_explicit_budget_override(monkeypatch):
    monkeypatch.setattr(web_api, "claude_decisions_for_domains", lambda domains: {})
    monkeypatch.setattr(web_api, "classification_record_for_domain", lambda domain: {"domain": domain})
    monkeypatch.setattr(web_api, "connect_pipeline_db", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(web_api, "claude_allowance", lambda connection: 0)
    captured = {}
    monkeypatch.setattr(
        web_api, "enqueue_claude_review",
        lambda domain, override_budget=False: captured.update(
            domain=domain, override_budget=override_budget
        ) or {"domain": domain, "status": "queued", "override_budget": override_budget},
    )
    monkeypatch.setattr(web_api, "record_admin_audit", lambda *args, **kwargs: None)

    limited = client.post("/api/claude/review/limit.ai", json={})
    assert limited.status_code == 429
    assert limited.json()["detail"]["message"] == "Hit daily limit"

    overridden = client.post(
        "/api/claude/review/limit.ai",
        json={"override_daily_limit": True},
    )
    assert overridden.status_code == 200
    assert captured == {"domain": "limit.ai", "override_budget": True}


def test_run_filters_cannot_change_while_certstream_is_running(monkeypatch):
    monkeypatch.setattr(web_api, "shared_pipeline_backend_available", lambda: False)
    monkeypatch.setattr(web_api, "read_local_pipeline_status", lambda: {"running": True})
    response = client.post(
        "/api/pipeline/filters",
        json={
            "selected_extensions": [".ai"],
            "include_keywords": [],
            "exclude_keywords": [],
            "minimum_score": 55,
            "claude_daily_limit": 400,
        },
    )
    assert response.status_code == 409
    assert "Pause CertStream" in response.text


def test_paused_cloud_worker_allows_filter_changes(monkeypatch, tmp_path):
    monkeypatch.setattr(web_api, "shared_pipeline_backend_available", lambda: True)
    monkeypatch.setattr(
        web_api, "read_control",
        lambda: {"desired": {"pipeline": False}},
    )
    monkeypatch.setattr(web_api, "read_pipeline_config", lambda: {})
    monkeypatch.setattr(web_api, "publish_pipeline_config", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_api, "record_admin_audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_api, "RUN_CONFIG", tmp_path / "run_config.json")
    response = client.post(
        "/api/pipeline/filters",
        json={
            "selected_extensions": [".ai"],
            "include_keywords": [],
            "exclude_keywords": [],
            "minimum_score": 55,
            "claude_daily_limit": 400,
        },
    )
    assert response.status_code == 200


def test_review_view_never_surfaces_unreadable_sites():
    """A site we could not read is not a review candidate.

    Confirmed live: alsharif.tech ("525: SSL handshake failed") and
    domedia.tech ("403 Forbidden") both appeared in the working review table.
    Ghost rows are now retained rather than purged so the queue can recheck
    them, which makes filtering them out of this view load-bearing rather than
    incidental.
    """
    assert "ghost" not in web_api.BUCKETS["review"]
    assert "ghost" not in web_api.BUCKETS["investable"]
    assert "ghost" not in web_api.BUCKETS["denied"]
    # Still reachable when something explicitly asks for everything.
    assert "ghost" in web_api.BUCKETS["all"]


def test_review_view_still_contains_the_real_candidate_buckets():
    for bucket in ("review", "coming_soon", "insufficient", "screened_out", "comp"):
        assert bucket in web_api.BUCKETS["review"], bucket


def test_run_bucket_counts_exclude_unreadable_sites_from_review():
    """"review" is derived as a remainder, so ghost must be subtracted too.

    Confirmed live: a run whose only classified rows were 7 ghosts and 6
    rejects reported 13 "candidates retained".
    """
    import web_api as api

    def fake_counts(run_id=""):
        return {"priority": 2, "review": 5, "reject": 6, "ghost": 7}

    original = api.classification_bucket_counts
    api.classification_bucket_counts = fake_counts
    try:
        counts = api._bucket_counts_for_run("run-x")
    finally:
        api.classification_bucket_counts = original

    assert counts["investable"] == 2
    assert counts["denied"] == 6
    assert counts["unreadable"] == 7
    # 20 total - 2 investable - 6 denied - 7 unreadable = 5
    assert counts["review"] == 5, "ghost rows leaked into the review count"


def test_unreadable_pages_are_never_persisted_to_a_bucket():
    """An error page must never end up in any bucket, and must not delete data."""
    from pathlib import Path

    source = Path("filter.py").read_text(encoding="utf-8")
    section = source.split("def persist_screening_output", 1)[1].split(
        "upsert_classifications(persistable_output)", 1
    )[0]
    # Excluded from what gets written...
    assert "& ~unreadable" in section
    # ...and from what gets purged, so a bad fetch cannot delete a good record.
    assert "& ~unreadable\n    )" in section
