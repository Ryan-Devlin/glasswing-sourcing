"""Opt-in parity test: continuous_pipeline's queue on Postgres.

Skipped by default (see _live_postgres_url) -- run explicitly with
TEST_DATABASE_URL set to verify the cloud backend once credentials are
available, exactly like tests/test_classification_store.py's Postgres test.
"""

import os
import sqlite3
from unittest.mock import patch

import pytest

import continuous_pipeline


def _live_postgres_url() -> str | None:
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


def _make_ai_search_db(path, crawled_domains):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE domains (domain TEXT PRIMARY KEY, last_crawled_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO domains(domain, last_crawled_at) VALUES (?, ?)",
        [(domain, "2026-01-01T00:00:00Z") for domain in crawled_domains],
    )
    conn.commit()
    conn.close()


def _make_zone_universe_db(path, paged_domains):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE pages (domain TEXT)")
    conn.executemany(
        "INSERT INTO pages(domain) VALUES (?)",
        [(domain,) for domain in paged_domains],
    )
    conn.commit()
    conn.close()


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_postgres_extension_counts_regex_matches_python(monkeypatch):
    """The SQL suffix expression must agree with domain_extension() exactly."""
    url = _live_postgres_url()
    monkeypatch.setenv(continuous_pipeline.PIPELINE_DATABASE_ENV_VAR, url)
    test_domains = [
        "ext-count-a.ai",
        "ext-count-b.io",
        "sub.ext-count-c.app",
        "ext-count-d.dev",
    ]

    def cleanup(conn):
        marks = ",".join("?" for _ in test_domains)
        conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
        conn.commit()

    with continuous_pipeline.connect_db() as conn:
        cleanup(conn)

    conn = continuous_pipeline.connect_db()
    try:
        before = continuous_pipeline.extension_counts(conn)
        for domain in test_domains:
            continuous_pipeline.register_domain_event(
                conn, domain, {".ai", ".io", ".app", ".dev"}, run_id="ext-count-test"
            )
        after = continuous_pipeline.extension_counts(conn)

        # Each domain must land in the bucket domain_extension() would pick.
        for domain in test_domains:
            extension = continuous_pipeline.domain_extension(domain)
            delta = (
                after[extension]["pending"] - before[extension]["pending"]
            )
            assert delta == 1, f"{domain} did not increment {extension}"
    finally:
        cleanup(conn)
        conn.close()


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_postgres_queue_matches_sqlite_behavior(monkeypatch, tmp_path):
    url = _live_postgres_url()
    monkeypatch.setenv(continuous_pipeline.PIPELINE_DATABASE_ENV_VAR, url)
    test_domains = ["pg-queue-a.ai", "pg-queue-b.ai", "pg-queue-c.ai"]

    with continuous_pipeline.connect_db() as conn:
        marks = ",".join("?" for _ in test_domains)
        conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM claude_calls WHERE domain IN ({marks})", test_domains)
        conn.commit()

    ai_search_db = tmp_path / "ai_search.sqlite3"
    zone_db = tmp_path / "zone_universe.sqlite3"
    _make_ai_search_db(ai_search_db, ["pg-queue-b.ai"])
    _make_zone_universe_db(zone_db, ["pg-queue-c.ai"])

    try:
        with patch.object(continuous_pipeline, "AI_SEARCH_DB", ai_search_db), \
             patch.object(continuous_pipeline, "ZONE_UNIVERSE_DB", zone_db):
            conn = continuous_pipeline.connect_db()
            try:
                for domain in test_domains:
                    result = continuous_pipeline.register_domain_event(
                        conn, domain, {".ai"}, run_id="pg-test"
                    )
                    assert result["is_new_domain"]

                claimed = continuous_pipeline.claim_batch(conn, size=10)
                # zone_universe evidence (pg-queue-c.ai) outranks ai_search
                # evidence (pg-queue-b.ai) outranks neither (pg-queue-a.ai).
                assert claimed.index("pg-queue-c.ai") < claimed.index("pg-queue-b.ai")
                assert claimed.index("pg-queue-b.ai") < claimed.index("pg-queue-a.ai")

                row = conn.execute(
                    "SELECT status, attempts FROM domains WHERE domain=?",
                    ("pg-queue-a.ai",),
                ).fetchone()
                assert row["status"] == "processing"
                assert row["attempts"] == 1

                continuous_pipeline.mark_success(conn, ["pg-queue-a.ai"])
                row = conn.execute(
                    "SELECT status FROM domains WHERE domain=?", ("pg-queue-a.ai",)
                ).fetchone()
                assert row["status"] == "complete"

                continuous_pipeline.mark_failure(
                    conn, ["pg-queue-b.ai"], "boom", max_attempts=3
                )
                row = conn.execute(
                    "SELECT status, last_error FROM domains WHERE domain=?",
                    ("pg-queue-b.ai",),
                ).fetchone()
                assert row["status"] == "pending"
                assert row["last_error"] == "boom"

                allowance = continuous_pipeline.claude_allowance(conn)
                assert allowance >= 0
            finally:
                conn.close()
    finally:
        with continuous_pipeline.connect_db() as conn:
            marks = ",".join("?" for _ in test_domains)
            conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
            conn.execute(
                f"DELETE FROM claude_calls WHERE domain IN ({marks})", test_domains
            )
            conn.commit()
