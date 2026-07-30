import json

import pandas as pd

from classification_store import (
    classification_map,
    connect_store,
    record_manual_move,
    upsert_classifications,
)


def test_store_separates_fit_confidence_and_priority(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    frame = pd.DataFrame([
        {
            "domain": "strong.ai",
            "page_title": "Strong Systems",
            "meta_description": "Enterprise robotics infrastructure",
            "homepage_excerpt": "Technical platform " * 80,
            "word_count": 160,
            "pages_scraped": 2,
            "http_status": 200,
            "crawl_status": "success",
            "content_source": "crawler_cache",
            "routing_bucket": "priority",
            "routing_reason": "strong_investable_candidate",
            "diligence_rank_score": 84,
            "rule_evidence": "robotics; enterprise platform",
        },
        {
            "domain": "missing.ai",
            "fetch_error": "crawler_dns_error",
            "crawl_status": "dns_error",
            "routing_bucket": "ghost",
            "routing_reason": "ghost_or_inaccessible_site",
            "diligence_rank_score": 0,
        },
    ])
    assert upsert_classifications(frame, database) == 2

    with connect_store(database, read_only=True) as conn:
        strong = conn.execute(
            "SELECT * FROM classifications WHERE domain='strong.ai'"
        ).fetchone()
        missing = conn.execute(
            "SELECT * FROM classifications WHERE domain='missing.ai'"
        ).fetchone()

    assert strong["availability_status"] == "live"
    assert strong["fit_score"] == 84
    assert strong["confidence_score"] > 80
    assert strong["priority_score"] >= 55
    assert missing["availability_status"] == "dns_error"
    assert missing["confidence_score"] == 0


def test_manual_move_is_transactional_and_audited(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    upsert_classifications(
        pd.DataFrame([
            {
                "domain": "move.ai",
                "routing_bucket": "review",
                "rule_score": 67,
                "word_count": 120,
                "homepage_excerpt": "Enterprise software platform",
            }
        ]),
        database,
    )

    moved = record_manual_move(
        ["move.ai"],
        target_label="Investable Candidate",
        target_bucket="priority",
        scores={"move.ai": 67},
        note="human review",
        path=database,
    )
    assert moved == 1
    assert classification_map(path=database) == {"move.ai": "priority"}

    with connect_store(database, read_only=True) as conn:
        decision = conn.execute(
            "SELECT * FROM manual_decisions WHERE domain='move.ai'"
        ).fetchone()
        history = conn.execute(
            "SELECT * FROM movement_log WHERE domain='move.ai'"
        ).fetchall()
        stored = conn.execute(
            "SELECT record_json FROM classifications WHERE domain='move.ai'"
        ).fetchone()

    assert decision["target_bucket"] == "priority"
    assert len(history) == 1
    assert history[0]["from_bucket"] == "review"
    assert json.loads(stored["record_json"])["manual_override"] is True
