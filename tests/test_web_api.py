from fastapi.testclient import TestClient
import pytest

import web_api


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


def test_human_label_endpoint_uses_protected_human_source(monkeypatch):
    captured = {}

    def save(domain, label, rating, **kwargs):
        captured.update(domain=domain, label=label, rating=rating, **kwargs)

    monkeypatch.setattr(web_api, "save_basket_entry", save)
    response = client.post(
        "/api/labels",
        json={"domain": "Example.ai", "label": "yes", "rating": 87},
    )
    assert response.status_code == 200
    assert captured["source"] == "human"
    assert captured["label"] == "yes"
    assert captured["rating"] == 87


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
