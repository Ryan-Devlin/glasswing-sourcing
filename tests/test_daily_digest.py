from datetime import datetime, timezone

import pandas as pd

from classification_store import connect_store, hide_domains, upsert_classifications
from daily_digest import (
    DigestSettings,
    qualified_candidates,
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
        "lookback_hours": 24,
        "max_companies": 20,
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
    assert "Enterprise customers" in plain
    assert "https://glasswing.example/?q=strong.ai" in plain
    assert "AI infrastructure for industrial robotics" in rich
    assert "Review in Glasswing" in rich


def test_first_run_lookback_prevents_historical_flood(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    seed(database)
    with connect_store(database) as conn:
        conn.execute(
            "UPDATE classifications SET updated_at='2026-08-01T12:00:00Z'"
        )
        conn.commit()
    assert qualified_candidates(settings(), now=NOW, database=database) == []
