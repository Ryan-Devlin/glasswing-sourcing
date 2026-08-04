import json
import os

import pandas as pd
import pytest

import classification_store
from classification_store import (
    classification_map,
    connect_store,
    hidden_domain_set,
    hide_domains,
    list_hidden_domains,
    record_manual_move,
    import_private_sqlite_into_shared,
    unhide_domains,
    upsert_classifications,
)


def test_pipeline_database_url_is_cloud_fallback(monkeypatch):
    monkeypatch.delenv("CLASSIFICATION_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PIPELINE_DATABASE_URL", "postgresql://shared/example")

    assert (
        classification_store.classification_database_env_var()
        == "PIPELINE_DATABASE_URL"
    )
    assert classification_store.using_classification_postgres() is True


def test_dedicated_classification_database_takes_precedence(monkeypatch):
    monkeypatch.setenv(
        "CLASSIFICATION_DATABASE_URL",
        "postgresql://classification/example",
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://web/example")
    monkeypatch.setenv("PIPELINE_DATABASE_URL", "postgresql://pipeline/example")

    assert (
        classification_store.classification_database_env_var()
        == "CLASSIFICATION_DATABASE_URL"
    )


def _live_postgres_url() -> str | None:
    """Only used by the opt-in Postgres parity test below.

    Skips (rather than failing) whenever DATABASE_URL isn't set or isn't
    reachable, so the normal test run never depends on a live database --
    this is purely for manually verifying the Postgres backend once
    credentials are in place.
    """
    url = os.getenv("TEST_DATABASE_URL", "").strip()
    if not url:
        return None
    try:
        import psycopg

        with psycopg.connect(url, connect_timeout=5):
            pass
    except Exception:
        return None
    return url


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


def test_hide_domains_is_a_view_preference_not_a_deletion(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    frame = pd.DataFrame([
        {"domain": "keep.ai", "routing_bucket": "priority"},
        {"domain": "toggle.ai", "routing_bucket": "priority"},
    ])
    assert upsert_classifications(frame, database) == 2

    assert hidden_domain_set(["keep.ai", "toggle.ai"], path=database) == set()

    hide_domains(["toggle.ai"], path=database)
    assert hidden_domain_set(["keep.ai", "toggle.ai"], path=database) == {
        "toggle.ai"
    }

    # Still present in the real dataset -- hiding is bookkeeping, not deletion.
    with connect_store(database, read_only=True) as conn:
        row = conn.execute(
            "SELECT * FROM classifications WHERE domain='toggle.ai'"
        ).fetchone()
    assert row is not None

    unhide_domains(["toggle.ai"], path=database)
    assert hidden_domain_set(["keep.ai", "toggle.ai"], path=database) == set()

    # Hiding the same domain twice (re-checking the box after an unhide, or
    # a duplicate call) must not error.
    hide_domains(["toggle.ai"], path=database)
    hide_domains(["toggle.ai"], path=database)
    assert hidden_domain_set(["toggle.ai"], path=database) == {"toggle.ai"}
    listed = list_hidden_domains(path=database)
    assert [row["domain"] for row in listed] == ["toggle.ai"]


def test_hidden_domain_queries_handle_more_than_sqlite_variable_limit(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    domains = [f"company-{number}.ai" for number in range(1200)]

    assert hide_domains(domains, path=database) == 1200
    assert hidden_domain_set(domains, path=database) == set(domains)
    assert unhide_domains(domains, path=database) == 1200
    assert hidden_domain_set(domains, path=database) == set()


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

    # A later automated crawl may refresh website fields, but it must never
    # erase a human decision while upserting that new evidence.
    upsert_classifications(
        pd.DataFrame([{
            "domain": "move.ai",
            "routing_bucket": "reject",
            "routing_reason": "automated_rejection",
            "rule_score": 2,
            "meta_description": "Refreshed website copy",
        }]),
        database,
    )
    with connect_store(database, read_only=True) as conn:
        preserved = conn.execute(
            "SELECT routing_bucket,manual_override,record_json "
            "FROM classifications WHERE domain='move.ai'"
        ).fetchone()
    assert preserved["routing_bucket"] == "priority"
    assert preserved["manual_override"] == 1
    assert json.loads(preserved["record_json"])["manual_override"] is True


def test_private_sqlite_import_only_merges_newer_records(tmp_path, monkeypatch):
    source = tmp_path / "private.sqlite3"
    import sqlite3

    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE classifications(record_json TEXT)")
        conn.executemany(
            "INSERT INTO classifications(record_json) VALUES (?)",
            [
                (json.dumps({
                    "domain": "old.ai",
                    "observed_at": "2026-07-01T00:00:00Z",
                }),),
                (json.dumps({
                    "domain": "new.ai",
                    "observed_at": "2026-08-04T00:00:00Z",
                }),),
            ],
        )

    class SharedConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql):
            class Result:
                @staticmethod
                def fetchone():
                    return {"latest": "2026-07-30T00:00:00Z"}

            return Result()

    imported = []
    monkeypatch.setattr(
        classification_store,
        "using_classification_postgres",
        lambda: True,
    )
    monkeypatch.setattr(
        classification_store,
        "connect_store",
        lambda *args, **kwargs: SharedConnection(),
    )
    monkeypatch.setattr(
        classification_store,
        "upsert_classifications",
        lambda frame: imported.extend(frame["domain"].tolist()) or len(frame),
    )

    assert import_private_sqlite_into_shared(source) == 1
    assert imported == ["new.ai"]


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_postgres_backend_matches_sqlite_behavior(monkeypatch):
    """Opt-in parity check: same scenario as the two tests above, on Postgres.

    Skipped by default (see _live_postgres_url) -- run explicitly with
    TEST_DATABASE_URL set to verify the cloud backend once credentials are
    available. Cleans up its own rows before and after so it never leaves
    residue in a database that may later hold real classifications.
    """
    url = _live_postgres_url()
    monkeypatch.setenv("DATABASE_URL", url)
    test_domains = ["pg-strong.ai", "pg-move.ai"]

    with connect_store() as conn:
        marks = ",".join("?" for _ in test_domains)
        conn.execute(f"DELETE FROM classifications WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM manual_decisions WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM movement_log WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM hidden_domains WHERE domain IN ({marks})", test_domains)
        conn.commit()

    try:
        frame = pd.DataFrame([{
            "domain": "pg-strong.ai",
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
        }, {
            "domain": "pg-move.ai",
            "routing_bucket": "review",
            "rule_score": 67,
            "word_count": 120,
            "homepage_excerpt": "Enterprise software platform",
        }])
        assert upsert_classifications(frame) == 2

        with connect_store(read_only=True) as conn:
            strong = conn.execute(
                "SELECT * FROM classifications WHERE domain=?", ("pg-strong.ai",)
            ).fetchone()
        assert strong["availability_status"] == "live"
        assert strong["fit_score"] == 84
        assert strong["confidence_score"] > 80
        assert strong["priority_score"] >= 55

        moved = record_manual_move(
            ["pg-move.ai"],
            target_label="Investable Candidate",
            target_bucket="priority",
            scores={"pg-move.ai": 67},
            note="human review",
        )
        assert moved == 1
        assert classification_map()["pg-move.ai"] == "priority"

        with connect_store(read_only=True) as conn:
            decision = conn.execute(
                "SELECT * FROM manual_decisions WHERE domain=?", ("pg-move.ai",)
            ).fetchone()
            history = conn.execute(
                "SELECT * FROM movement_log WHERE domain=?", ("pg-move.ai",)
            ).fetchall()
        assert decision["target_bucket"] == "priority"
        assert len(history) == 1
        assert history[0]["from_bucket"] == "review"

        assert hide_domains(["pg-strong.ai"]) == 1
        assert hidden_domain_set(test_domains) == {"pg-strong.ai"}
        assert list_hidden_domains()[0]["domain"] == "pg-strong.ai"
        assert unhide_domains(["pg-strong.ai"]) == 1
        assert hidden_domain_set(test_domains) == set()
    finally:
        with connect_store() as conn:
            marks = ",".join("?" for _ in test_domains)
            conn.execute(
                f"DELETE FROM classifications WHERE domain IN ({marks})", test_domains
            )
            conn.execute(
                f"DELETE FROM manual_decisions WHERE domain IN ({marks})", test_domains
            )
            conn.execute(
                f"DELETE FROM movement_log WHERE domain IN ({marks})", test_domains
            )
            conn.execute(
                f"DELETE FROM hidden_domains WHERE domain IN ({marks})", test_domains
            )
            conn.commit()
