"""Opt-in parity test: screenable_domains()'s cross-backend path.

Skipped by default (see _live_postgres_url). Exercises the realistic
near-term scenario: ai_search's corpus on Postgres (AI_SEARCH_DATABASE_URL)
while the pipeline queue stays local SQLite -- Postgres has no ATTACH
equivalent for that mix, which is exactly what this path exists to handle.
"""

import os
import sqlite3

import pytest

from ai_search.database import add_domain, connect, utc_now
from ai_search.og_bridge import screenable_domains


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


def _create_queue(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE domains(
            domain TEXT PRIMARY KEY, status TEXT, classification TEXT,
            last_processed_at TEXT, last_seen_run_id TEXT
        )
        """
    )
    conn.commit()
    return conn


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_stale_ghost_is_recovered_cross_backend(monkeypatch, tmp_path):
    url = _live_postgres_url()
    monkeypatch.setenv("AI_SEARCH_DATABASE_URL", url)
    test_domains = ["pg-screenable-new.ai", "pg-screenable-recovered.ai"]

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
                domain_id = conn.execute(
                    "SELECT id FROM domains WHERE domain=?", (domain,)
                ).fetchone()["id"]
                conn.execute(
                    """
                    INSERT INTO pages(
                        domain_id, url, final_url, title, cleaned_text,
                        word_count, crawled_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        domain_id, f"https://{domain}", f"https://{domain}",
                        "Test", "Enterprise robotics platform", 3,
                        "2026-07-29T12:00:00Z",
                    ),
                )
                conn.execute(
                    "UPDATE domains SET latest_crawl_status='success', "
                    "last_crawled_at=? WHERE id=?",
                    ("2026-07-29T12:00:00Z", domain_id),
                )
            conn.commit()
        finally:
            conn.close()

        queue_db = tmp_path / "queue.sqlite3"
        queue_conn = _create_queue(queue_db)
        queue_conn.execute(
            "INSERT INTO domains VALUES (?, 'complete', 'ghost', ?, 'old')",
            ("pg-screenable-recovered.ai", "2026-07-28T12:00:00Z"),
        )
        queue_conn.commit()
        queue_conn.close()

        domains = screenable_domains(
            "unused-path", exclude_queue_db=queue_db, include_stale=True,
        )
        assert "pg-screenable-new.ai" in domains
        assert "pg-screenable-recovered.ai" in domains

        domains_no_stale = screenable_domains(
            "unused-path", exclude_queue_db=queue_db, include_stale=False,
        )
        assert "pg-screenable-new.ai" in domains_no_stale
        assert "pg-screenable-recovered.ai" not in domains_no_stale
    finally:
        conn = connect("")
        try:
            marks = ",".join("?" for _ in test_domains)
            conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
            conn.commit()
        finally:
            conn.close()
