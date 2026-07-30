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
