"""Opt-in parity test: pipeline_status_for_corpus's cross-backend path.

Skipped by default (see _live_postgres_url). Exercises the realistic
near-term scenario: ai_search's corpus on Postgres (AI_SEARCH_DATABASE_URL)
while the pipeline queue stays local SQLite -- Postgres has no ATTACH
equivalent for that mix, which is exactly what this path exists to handle.
"""

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from ai_search.database import add_domain, connect
from ai_search.og_bridge import pipeline_status_for_corpus


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


def _make_queue_db(path: Path, rows: list[tuple[str, str]]) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE domains (
            domain TEXT PRIMARY KEY, status TEXT NOT NULL
        )
        """
    )
    conn.executemany("INSERT INTO domains(domain, status) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_cross_backend_status_matches_expected_counts(monkeypatch, tmp_path):
    url = _live_postgres_url()
    monkeypatch.setenv("AI_SEARCH_DATABASE_URL", url)
    test_domains = ["pg-status-a.ai", "pg-status-b.ai", "pg-status-c.ai"]

    conn = connect("")
    try:
        marks = ",".join("?" for _ in test_domains)
        conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
        conn.commit()
    finally:
        conn.close()

    try:
        conn = connect("")
        try:
            for domain in test_domains:
                add_domain(conn, domain, "fixture")
            marks = ",".join("?" for _ in test_domains)
            conn.execute(
                f"UPDATE domains SET last_crawled_at=? WHERE domain IN ({marks})",
                ["2026-01-01T00:00:00Z", *test_domains],
            )
            conn.commit()
        finally:
            conn.close()

        queue_db = tmp_path / "queue.sqlite3"
        _make_queue_db(queue_db, [
            ("pg-status-a.ai", "complete"),
            ("pg-status-b.ai", "pending"),
            # pg-status-c.ai deliberately absent from the queue -- "not_queued".
        ])

        result = pipeline_status_for_corpus(
            Path("unused-search-db-path"), queue_db, crawled_only=True
        )

        assert result["complete"] == 1
        assert result["pending"] == 1
        assert result["not_queued"] == 1
        assert result["in_screening_registry"] == 2
    finally:
        conn = connect("")
        try:
            marks = ",".join("?" for _ in test_domains)
            conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
            conn.commit()
        finally:
            conn.close()
