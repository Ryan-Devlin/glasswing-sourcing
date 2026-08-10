from datetime import datetime, timezone

import pandas as pd

from classification_store import (
    connect_store,
    hide_domains,
    upsert_classifications,
    upsert_claude_diligence,
)
from diligence_logic import DILIGENCE_VERSION
from daily_digest import (
    DigestSettings,
    qualified_candidates,
    python_candidates_for_claude,
    record_deliveries,
    render_html,
    render_plain_text,
)


NOW = datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)


def settings(**overrides) -> DigestSettings:
    values = {
        "recipients": ("team@example.com",),
        "smtp_host": "smtp.example.com",
        "smtp_port": 587,
        "smtp_username": "sender@example.com",
        "smtp_password": "secret",
        "sender": "sender@example.com",
        "use_starttls": True,
        "use_ssl": False,
        "min_score": 75,
        "min_confidence": 80,
        "min_claude_score": 70,
        "lookback_hours": 24,
        "max_companies": 5,
        "auto_claude": True,
        "claude_max_calls": 5,
        "app_url": "https://glasswing.example",
        "timezone_name": "America/New_York",
    }
    values.update(overrides)
    return DigestSettings(**values)


def seed(database):
    frame = pd.DataFrame([
        {
            "domain": "strong.ai",
            "page_title": "Strong Systems",
            "meta_description": "AI infrastructure for industrial robotics.",
            "routing_bucket": "priority",
            "routing_reason": "strong_investable_candidate",
            "diligence_rank_score": 88,
            "rule_thesis_pillar": "physical_ai",
            "rule_evidence": "Enterprise customers; proprietary robotics stack",
            "content_source": "crawler_cache",
        },
        {
            "domain": "borderline.ai",
            "routing_bucket": "priority",
            "diligence_rank_score": 70,
        },
        {
            "domain": "review.ai",
            "routing_bucket": "review",
            "diligence_rank_score": 90,
        },
    ])
    assert upsert_classifications(frame, database) == 3
    with connect_store(database) as conn:
        conn.execute(
            """
            UPDATE classifications
            SET updated_at=?, confidence_score=CASE domain
                WHEN 'strong.ai' THEN 96
                WHEN 'borderline.ai' THEN 95
                ELSE 99 END,
                priority_score=CASE domain
                WHEN 'strong.ai' THEN 92
                ELSE priority_score END
            """,
            ("2026-08-05T12:00:00Z",),
        )
        conn.commit()
    assert upsert_claude_diligence(
        [{
            "domain": "strong.ai",
            "llm_decision": "KEEP",
            "llm_fit_score": 87,
            "llm_product_summary": "Robotics infrastructure platform",
            "llm_reasoning": "Defensible enterprise robotics wedge",
            "llm_concerns": "Early customer evidence",
            "llm_diligence_version": DILIGENCE_VERSION,
            "llm_model": "claude-test",
        }],
        path=database,
    ) == 1


def test_digest_gate_and_delivery_deduplication(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    rows = qualified_candidates(settings(), now=NOW, database=database)
    assert [row["domain"] for row in rows] == ["strong.ai"]

    record_deliveries(
        "run-one",
        rows,
        settings().recipients,
        sent_at="2026-08-05T13:00:00Z",
        database=database,
    )
    assert qualified_candidates(settings(), now=NOW, database=database) == []

    upsert_claude_diligence(
        [{
            "domain": "strong.ai",
            "llm_decision": "KEEP",
            "llm_fit_score": 90,
            "llm_diligence_version": "old-prompt",
        }],
        path=database,
    )
    assert qualified_candidates(settings(), now=NOW, database=database) == []


def test_hidden_and_mark_no_companies_never_enter_digest(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    hide_domains(["strong.ai"], path=database)
    assert qualified_candidates(settings(), now=NOW, database=database) == []

    with connect_store(database) as conn:
        conn.execute("DELETE FROM hidden_domains")
        conn.execute(
            """
                INSERT INTO training_labels(domain,label,rating,source,reason,labeled_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(domain) DO UPDATE SET
                    label=excluded.label,rating=excluded.rating,
                    source=excluded.source,reason=excluded.reason,
                    labeled_at=excluded.labeled_at
            """,
            ("strong.ai", "no", 2, "human", "not a fit", "2026-08-05T12:30:00Z"),
        )
        conn.commit()
    assert qualified_candidates(settings(), now=NOW, database=database) == []


def test_digest_content_is_evidence_backed_and_escaped(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    rows = qualified_candidates(settings(), now=NOW, database=database)
    plain = render_plain_text(rows, settings(), date_label="August 5, 2026")
    rich = render_html(rows, settings(), date_label="August 5, 2026")

    assert "Strong Systems" in plain
    assert "Score 88" in plain
    assert "Claude: KEEP" in plain
    assert "Claude score 87" in plain
    assert "Enterprise customers" in plain
    assert "https://glasswing.example/?q=strong.ai" in plain
    assert "AI infrastructure for industrial robotics" in rich
    assert "Review in Glasswing" in rich


def test_rejected_and_stale_claude_results_are_never_emailed(tmp_path):
    # No MAYBE exists anymore -- every Claude call resolves to KEEP or
    # REJECT. Only KEEP should ever qualify for the digest.
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    upsert_claude_diligence(
        [{
            "domain": "strong.ai",
            "llm_decision": "REJECT",
            "llm_fit_score": 90,
            "llm_diligence_version": DILIGENCE_VERSION,
        }],
        path=database,
    )
    assert qualified_candidates(settings(), now=NOW, database=database) == []


def test_only_undiligenced_python_candidates_enter_claude_gate(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    assert python_candidates_for_claude(
        settings(), now=NOW, database=database
    ) == []
    with connect_store(database) as conn:
        conn.execute("DELETE FROM claude_diligence WHERE domain='strong.ai'")
        conn.commit()
    candidates = python_candidates_for_claude(
        settings(), now=NOW, database=database
    )
    assert [row["domain"] for row in candidates] == ["strong.ai"]
    assert candidates[0]["diligence_rank_score"] == 88

def test_first_run_lookback_prevents_historical_flood(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    with connect_store(database) as conn:
        conn.execute(
            "UPDATE classifications SET updated_at='2026-08-01T12:00:00Z'"
        )
        conn.execute(
            "UPDATE claude_diligence SET updated_at='2026-08-01T12:00:00Z'"
        )
        conn.commit()
    assert qualified_candidates(settings(), now=NOW, database=database) == []


def test_send_via_resend_api_posts_json_with_bearer_auth(monkeypatch):
    import json as json_module

    import daily_digest

    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b'{"id": "abc"}'

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.headers)
        captured["body"] = json_module.loads(request.data)
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daily_digest.urllib.request, "urlopen", fake_urlopen)

    row = {
        "domain": "strong.ai",
        "company_name": "Strong Systems",
        "fit_score": 92,
        "confidence_score": 96,
        "content_source": "crawler_cache",
        "claude_fit_score": 87,
        "claude_summary": "Robotics infrastructure platform",
        "claude_reasoning": "Defensible enterprise robotics wedge",
        "thesis_pillar": "physical_ai",
        "routing_reason": "strong_investable_candidate",
        "final_url": "",
        "meta_description": "",
        "rule_evidence": "",
    }
    daily_digest.send_via_resend_api(
        [row],
        settings(
            smtp_host="smtp.resend.com",
            smtp_password="re_secret_key",
            sender="onboarding@resend.dev",
            recipients=("team@example.com",),
        ),
        date_label="August 5, 2026",
    )

    assert captured["url"] == daily_digest.RESEND_API_URL
    assert captured["headers"]["Authorization"] == "Bearer re_secret_key"
    assert captured["body"]["from"] == "onboarding@resend.dev"
    assert captured["body"]["to"] == ["team@example.com"]
    assert "strong.ai" in captured["body"]["text"]
    assert "strong.ai" in captured["body"]["html"]


def test_run_digest_routes_resend_host_through_api_not_smtp(tmp_path, monkeypatch):
    import daily_digest

    database = tmp_path / "classifications.sqlite3"
    seed(database)

    api_calls = []
    smtp_calls = []
    monkeypatch.setattr(
        daily_digest,
        "send_via_resend_api",
        lambda *a, **k: api_calls.append(1),
    )
    monkeypatch.setattr(
        daily_digest, "send_message", lambda *a, **k: smtp_calls.append(1)
    )

    daily_digest.run_digest(
        settings(smtp_host="smtp.resend.com", smtp_password="re_secret_key"),
        now=NOW,
        database=database,
    )

    assert api_calls == [1]
    assert smtp_calls == []


def test_send_via_sendgrid_api_posts_json_with_bearer_auth(monkeypatch):
    import json as json_module

    import daily_digest

    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self):
            return b""

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.headers)
        captured["body"] = json_module.loads(request.data)
        return FakeResponse()

    monkeypatch.setattr(daily_digest.urllib.request, "urlopen", fake_urlopen)

    row = {
        "domain": "strong.ai",
        "company_name": "Strong Systems",
        "fit_score": 92,
        "confidence_score": 96,
        "content_source": "crawler_cache",
        "claude_fit_score": 87,
        "claude_summary": "Robotics infrastructure platform",
        "claude_reasoning": "Defensible enterprise robotics wedge",
        "thesis_pillar": "physical_ai",
        "routing_reason": "strong_investable_candidate",
        "final_url": "",
        "meta_description": "",
        "rule_evidence": "",
    }
    daily_digest.send_via_sendgrid_api(
        [row],
        settings(
            smtp_host="smtp.sendgrid.net",
            smtp_password="SG.secret_key",
            sender="digest@glasswing.vc",
            recipients=("team@example.com",),
        ),
        date_label="August 5, 2026",
    )

    assert captured["url"] == daily_digest.SENDGRID_API_URL
    assert captured["headers"]["Authorization"] == "Bearer SG.secret_key"
    assert captured["body"]["from"] == {"email": "digest@glasswing.vc"}
    assert captured["body"]["personalizations"] == [
        {"to": [{"email": "team@example.com"}]}
    ]


def test_run_digest_routes_sendgrid_host_through_api_not_smtp(tmp_path, monkeypatch):
    import daily_digest

    database = tmp_path / "classifications.sqlite3"
    seed(database)

    api_calls = []
    smtp_calls = []
    monkeypatch.setattr(
        daily_digest,
        "send_via_sendgrid_api",
        lambda *a, **k: api_calls.append(1),
    )
    monkeypatch.setattr(
        daily_digest, "send_message", lambda *a, **k: smtp_calls.append(1)
    )

    daily_digest.run_digest(
        settings(smtp_host="smtp.sendgrid.net", smtp_password="SG.secret_key"),
        now=NOW,
        database=database,
    )

    assert api_calls == [1]
    assert smtp_calls == []
