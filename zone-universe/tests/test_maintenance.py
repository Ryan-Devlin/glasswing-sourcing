from __future__ import annotations

from pathlib import Path

from ai_search.extract import extract_page
from zone_universe.crawler import CrawlOutcome, save_success
from zone_universe.database import connect
from zone_universe.maintenance import prune_noise


def test_prune_noise_removes_old_pages_and_fts_rows(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    for domain in ("normalbrand.app", "00160c.app"):
        html = f"<html><title>{domain}</title><body>Company</body></html>"
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
            retain_raw_html=False,
        )
    conn.commit()
    result = prune_noise(conn)
    assert result["removed_pages"] == 1
    assert result["domains"] == ["00160c.app"]
    assert [
        str(row["domain"])
        for row in conn.execute("SELECT domain FROM pages")
    ] == ["normalbrand.app"]
    # Full-text corpus search is retired; new crawls do not populate its index.
    assert conn.execute(
        "SELECT count(*) FROM page_search"
    ).fetchone()[0] == 0
    conn.close()
