"""Opt-in parity test: zone_universe's schema + tsvector search on Postgres.

Skipped by default (see _live_postgres_url) -- run explicitly with
TEST_DATABASE_URL set, mirroring ai_search's equivalent test. Uses
ZONE_UNIVERSE_DATABASE_URL (zone_universe's own env-var gate) so this never
activates just because DATABASE_URL happens to be set.
"""

import os

import pytest
from ai_search.extract import ExtractedPage

from zone_universe.crawler import CrawlOutcome, save_success
from zone_universe.database import connect
from zone_universe.search import compile_query_postgres, search_pages


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


def _page(*, title, description, headings, body) -> ExtractedPage:
    return ExtractedPage(
        title=title,
        description=description,
        headings=headings,
        body=body,
        cleaned_text=body,
        canonical_url="",
        language="en",
        emails="[]",
        social_links="[]",
        json_ld="[]",
        internal_links=(),
        word_count=len(body.split()) or 1,
        content_hash="",
        is_parked=False,
        is_coming_soon=False,
    )


def _seed(conn, *, domain, title, description, headings, body):
    save_success(
        conn,
        CrawlOutcome(
            domain=domain,
            tld="app",
            status="success",
            final_url=f"https://{domain}",
            html=f"<html>{body}</html>",
            page=_page(title=title, description=description, headings=headings, body=body),
            document_count=1,
        ),
        retain_raw_html=False,
    )
    conn.commit()


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
class TestZoneUniverseSearchPostgres:
    def setup_method(self):
        url = _live_postgres_url()
        os.environ["ZONE_UNIVERSE_DATABASE_URL"] = url
        self.test_domains = ["pg-zone-strong.app", "pg-zone-weak.app"]
        conn = connect("")
        try:
            marks = ",".join("?" for _ in self.test_domains)
            conn.execute(f"DELETE FROM pages WHERE domain IN ({marks})", self.test_domains)
            conn.commit()
        finally:
            conn.close()

    def teardown_method(self):
        conn = connect("")
        try:
            marks = ",".join("?" for _ in self.test_domains)
            conn.execute(f"DELETE FROM pages WHERE domain IN ({marks})", self.test_domains)
            conn.commit()
        finally:
            conn.close()
        os.environ.pop("ZONE_UNIVERSE_DATABASE_URL", None)

    def test_domain_match_outranks_body_only_match_and_has_snippet(self):
        conn = connect("")
        try:
            _seed(
                conn,
                domain="pg-zone-strong.app",
                title="Quantum Data Platform",
                description="Enterprise quantum data infrastructure",
                headings="Quantum Data",
                body="Our quantum data platform serves scientific teams.",
            )
            _seed(
                conn,
                domain="pg-zone-weak.app",
                title="Generic Company",
                description="A generic company website",
                headings="About us",
                body="We occasionally mention quantum data in passing.",
            )
        finally:
            conn.close()

        results = search_pages("quantum data", database="")
        domains = [row["domain"] for row in results]
        assert "pg-zone-strong.app" in domains
        assert "pg-zone-weak.app" in domains
        assert domains.index("pg-zone-strong.app") < domains.index("pg-zone-weak.app")
        assert "<mark>" in results[domains.index("pg-zone-strong.app")]["snippet"]

    def test_negation_excludes_matching_domain(self):
        conn = connect("")
        try:
            _seed(
                conn,
                domain="pg-zone-strong.app",
                title="Quantum Data Platform",
                description="",
                headings="",
                body="",
            )
        finally:
            conn.close()

        results = search_pages("quantum -platform", database="")
        assert results == []

    def test_tld_filter(self):
        conn = connect("")
        try:
            _seed(
                conn,
                domain="pg-zone-strong.app",
                title="Quantum Data Platform",
                description="",
                headings="",
                body="",
            )
        finally:
            conn.close()

        assert search_pages("quantum data", tld="com", database="") == []
        assert len(search_pages("quantum data", tld="app", database="")) == 1

    def test_compile_query_postgres_matches_compile_query_semantics(self):
        expression, params = compile_query_postgres('"exact phrase" word -bad')
        assert "&&" in expression
        assert "!!" in expression
        assert params == ["exact phrase", "word", "bad"]
