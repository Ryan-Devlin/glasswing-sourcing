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


def test_unknown_bucket_and_worker_are_rejected():
    assert client.get("/api/classifications?bucket=wat").status_code == 400
    assert client.post("/api/runtime/wat", json={"desired": True}).status_code == 404


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
