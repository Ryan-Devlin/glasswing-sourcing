"""Opt-in parity test: ai_search's schema + tsvector search on Postgres.

Skipped by default (see _live_postgres_url) -- run explicitly with
TEST_DATABASE_URL set, mirroring tests/test_classification_store.py's
Postgres test. Uses AI_SEARCH_DATABASE_URL (ai_search's own env-var gate,
deliberately separate from the classification store's DATABASE_URL) so this
never activates just because DATABASE_URL happens to be set.
"""

import os

import pytest

from ai_search.database import add_domain, connect, index_page
from ai_search.search import SearchOptions, compile_query_postgres, search


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


def _seed_page(conn, *, domain, title, description, headings, body, word_count=200, parked=False):
    add_domain(conn, domain, "fixture")
    domain_row = conn.execute(
        "SELECT id FROM domains WHERE domain=?", (domain,)
    ).fetchone()
    conn.execute(
        "UPDATE domains SET is_parked=?, last_crawled_at=? WHERE id=?",
        (int(parked), "2026-01-01T00:00:00Z", domain_row["id"]),
    )
    conn.execute(
        """
        INSERT INTO pages(
            domain_id, url, final_url, title, meta_description, headings,
            cleaned_text, word_count, crawled_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            domain_row["id"], f"https://{domain}", f"https://{domain}",
            title, description, headings, body, word_count,
            "2026-01-01T00:00:00Z",
        ),
    )
    row = conn.execute(
        "SELECT id FROM pages WHERE domain_id=? AND final_url=?",
        (domain_row["id"], f"https://{domain}"),
    ).fetchone()
    index_page(conn, row["id"])
    conn.commit()


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
class TestAiSearchPostgres:
    def setup_method(self):
        url = _live_postgres_url()
        os.environ["AI_SEARCH_DATABASE_URL"] = url
        test_domains = ["pg-search-strong.ai", "pg-search-weak.ai", "pg-search-parked.ai"]
        conn = connect("")
        try:
            marks = ",".join("?" for _ in test_domains)
            conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", test_domains)
            conn.commit()
        finally:
            conn.close()
        self.test_domains = test_domains

    def teardown_method(self):
        conn = connect("")
        try:
            marks = ",".join("?" for _ in self.test_domains)
            conn.execute(f"DELETE FROM domains WHERE domain IN ({marks})", self.test_domains)
            conn.commit()
        finally:
            conn.close()
        os.environ.pop("AI_SEARCH_DATABASE_URL", None)

    def test_domain_match_outranks_body_only_match(self):
        conn = connect("")
        try:
            _seed_page(
                conn,
                domain="pg-search-strong.ai",
                title="Quantum Data Platform",
                description="Enterprise quantum data infrastructure",
                headings="Quantum Data",
                body="Our quantum data platform serves scientific research teams.",
            )
            _seed_page(
                conn,
                domain="pg-search-weak.ai",
                title="Generic Company",
                description="A generic company website",
                headings="About us",
                body="We occasionally mention quantum data in passing on this page.",
            )
            results = search(conn, "quantum data", SearchOptions(exclude_parked=True))
        finally:
            conn.close()

        domains = [row["domain"] for row in results]
        assert "pg-search-strong.ai" in domains
        assert "pg-search-weak.ai" in domains
        assert domains.index("pg-search-strong.ai") < domains.index("pg-search-weak.ai")
        assert "<mark>" in results[domains.index("pg-search-strong.ai")]["snippet"]

    def test_unquoted_multiword_is_exact_phrase(self):
        conn = connect("")
        try:
            _seed_page(
                conn,
                domain="pg-search-strong.ai",
                title="Quantum Data Platform",
                description="",
                headings="",
                body="",
            )
            _seed_page(
                conn,
                domain="pg-search-weak.ai",
                title="Data about Quantum Physics",
                description="",
                headings="",
                body="",
            )
            # Unquoted "quantum data" should match the exact adjacent phrase
            # (strong) but not a page that merely contains both words apart
            # (weak has "Data" then "Quantum" -- reversed, not adjacent).
            results = search(conn, "quantum data", SearchOptions())
        finally:
            conn.close()

        domains = [row["domain"] for row in results]
        assert "pg-search-strong.ai" in domains
        assert "pg-search-weak.ai" not in domains

    def test_negation_excludes_matching_domain(self):
        conn = connect("")
        try:
            _seed_page(
                conn,
                domain="pg-search-strong.ai",
                title="Quantum Data Platform",
                description="",
                headings="",
                body="",
            )
            results = search(conn, 'quantum -platform', SearchOptions())
        finally:
            conn.close()

        assert results == []

    def test_exclude_parked_filter(self):
        conn = connect("")
        try:
            _seed_page(
                conn,
                domain="pg-search-parked.ai",
                title="Quantum Data Platform",
                description="",
                headings="",
                body="",
                parked=True,
            )
            results = search(conn, "quantum data", SearchOptions(exclude_parked=True))
        finally:
            conn.close()

        assert results == []

    def test_compile_query_postgres_matches_compile_query_semantics(self):
        expression, params = compile_query_postgres('"exact phrase" word -bad')
        assert "&&" in expression
        assert "!!" in expression
        assert params == ["exact phrase", "word", "bad"]
