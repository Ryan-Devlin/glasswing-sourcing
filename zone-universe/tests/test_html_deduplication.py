from __future__ import annotations

from pathlib import Path

from ai_search.extract import extract_page
from zone_universe.crawler import CrawlOutcome, save_success
from zone_universe.database import connect


def test_identical_raw_html_shares_one_archive_blob(tmp_path: Path) -> None:
    database = tmp_path / "universe.sqlite3"
    archive = tmp_path / "html"
    html = "<html><title>Shared</title><body>Same template</body></html>"
    conn = connect(database)
    for domain in ("one.app", "two.app"):
        save_success(
            conn,
            CrawlOutcome(
                domain=domain,
                tld="app",
                status="success",
                final_url=f"https://{domain}",
                html=html,
                page=extract_page(html, f"https://{domain}"),
            ),
            retain_raw_html=True,
            archive_root=archive,
        )
    conn.commit()
    rows = conn.execute(
        """
        SELECT raw_html_hash,html_archive_path,html_archive_offset,
               html_archive_length
        FROM pages ORDER BY domain
        """
    ).fetchall()
    assert len(rows) == 2
    assert tuple(rows[0]) == tuple(rows[1])
    assert conn.execute(
        "SELECT count(*) FROM html_blobs"
    ).fetchone()[0] == 1
    conn.close()


def test_deferred_save_then_backfill_ends_up_archived(tmp_path: Path) -> None:
    # crawl_once's actual pattern at ~170M .com/.app names: save metadata
    # immediately with retain_raw_html=False (no archive -- too many
    # domains to archive every crawl outright), then re-save the same
    # outcome with retain_raw_html=True only for domains that clear
    # html_archive_minimum_score. Confirms that second call actually
    # backfills the archive pointer on the existing row, not a no-op.
    database = tmp_path / "universe.sqlite3"
    archive = tmp_path / "html"
    html = "<html><title>Winner</title><body>Real content</body></html>"
    conn = connect(database)
    outcome = CrawlOutcome(
        domain="winner.app",
        tld="app",
        status="success",
        final_url="https://winner.app",
        html=html,
        page=extract_page(html, "https://winner.app"),
    )

    save_success(conn, outcome, retain_raw_html=False, archive_root=archive)
    conn.commit()
    deferred_row = conn.execute(
        "SELECT html_archive_path, html_archive_length, cleaned_text "
        "FROM pages WHERE domain='winner.app'"
    ).fetchone()
    assert deferred_row["html_archive_path"] == ""
    assert deferred_row["html_archive_length"] == 0
    # Metadata needed for scoring is saved regardless of the deferral.
    assert "Real content" in deferred_row["cleaned_text"]
    assert conn.execute(
        "SELECT count(*) FROM html_blobs"
    ).fetchone()[0] == 0

    save_success(conn, outcome, retain_raw_html=True, archive_root=archive)
    conn.commit()
    backfilled_row = conn.execute(
        "SELECT html_archive_path, html_archive_length "
        "FROM pages WHERE domain='winner.app'"
    ).fetchone()
    assert backfilled_row["html_archive_path"] != ""
    assert backfilled_row["html_archive_length"] > 0
    assert conn.execute(
        "SELECT count(*) FROM html_blobs"
    ).fetchone()[0] == 1
    conn.close()
