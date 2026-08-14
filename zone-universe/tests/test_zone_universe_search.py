from __future__ import annotations

from pathlib import Path

from ai_search.extract import extract_page
from zone_universe.crawler import CrawlOutcome, save_success
from zone_universe.database import connect
from zone_universe.search import compile_query, raw_html, search_pages


def _save(
    database: Path,
    archive: Path,
    domain: str,
    title: str,
    body: str,
) -> None:
    html = (
        "<!doctype html><html><head>"
        f"<title>{title}</title>"
        "</head><body>"
        f"<main>{body}</main>"
        "</body></html>"
    )
    page = extract_page(html, f"https://{domain}")
    conn = connect(database)
    save_success(
        conn,
        CrawlOutcome(
            domain=domain,
            tld=domain.rsplit(".", 1)[-1],
            status="success",
            requested_url=f"https://{domain}",
            final_url=f"https://{domain}",
            http_status=200,
            html=html,
            page=page,
        ),
        retain_raw_html=True,
        archive_root=archive,
    )
    conn.commit()
    conn.close()


def test_unquoted_multiword_input_becomes_an_exact_phrase() -> None:
    assert compile_query("quantum data") == '"quantum data"'
    assert compile_query('"quantum data" -consulting') == (
        '"quantum data" NOT "consulting"'
    )


def test_retired_search_index_is_not_written_and_raw_html_is_discarded(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    archive = tmp_path / "html"
    _save(
        database,
        archive,
        "match.app",
        "Quantum Data Platform",
        "Enterprise systems for quantum data teams.",
    )
    _save(
        database,
        archive,
        "near.app",
        "Quantum Platform",
        "Quantum tools for distributed data engineering teams.",
    )

    results = search_pages(
        "quantum data",
        database=database,
        minimum_words=0,
    )
    assert results == []
    retained = raw_html("match.app", database=database)
    assert retained == ""
    conn = connect(database, read_only=True)
    stored = conn.execute(
        """
        SELECT raw_html_zlib,html_archive_path,html_archive_length
        FROM pages WHERE domain='match.app'
        """
    ).fetchone()
    assert stored["raw_html_zlib"] is None
    assert stored["html_archive_path"] == ""
    assert stored["html_archive_length"] == 0
    conn.close()
