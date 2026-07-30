from __future__ import annotations

from pathlib import Path

from zone_universe.crawler import (
    CrawlOutcome,
    claim_due_retries,
    update_retry_state,
)
from zone_universe.database import connect


def test_retryable_failure_is_scheduled_then_success_clears_it(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "universe.sqlite3")
    failed = CrawlOutcome(
        domain="temporary.app",
        tld="app",
        status="timeout",
        error="TimeoutError",
    )
    update_retry_state(conn, failed, max_retries=3)
    conn.execute(
        "UPDATE crawl_retries SET next_attempt_at='2000-01-01T00:00:00Z'"
    )
    conn.commit()
    assert claim_due_retries(conn, tld="app", limit=10) == [
        "temporary.app"
    ]

    succeeded = CrawlOutcome(
        domain="temporary.app",
        tld="app",
        status="success",
    )
    update_retry_state(conn, succeeded, max_retries=3)
    conn.commit()
    assert conn.execute(
        "SELECT count(*) FROM crawl_retries"
    ).fetchone()[0] == 0
    conn.close()


def test_permanent_dns_failure_is_not_retried(tmp_path: Path) -> None:
    conn = connect(tmp_path / "universe.sqlite3")
    update_retry_state(
        conn,
        CrawlOutcome(
            domain="dead.app",
            tld="app",
            status="dns_error",
        ),
        max_retries=3,
    )
    conn.commit()
    assert conn.execute(
        "SELECT count(*) FROM crawl_retries"
    ).fetchone()[0] == 0
    conn.close()
