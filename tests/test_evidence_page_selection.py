import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ai_search import crawler as ai_crawler
from ai_search.database import add_domain, connect
from ai_search.extract import extract_page
from zone_universe import crawler as zone_crawler
from zone_universe.database import connect as connect_zone


HOMEPAGE_HTML = """
<html><body><nav>
  <a href="/research">Research</a>
  <a href="/founders">Founders</a>
  <a href="/product">Product</a>
</nav><main>Enterprise robotics platform</main></body></html>
"""


def fetched_page(url: str, html: str) -> tuple[str, str, int, str, int]:
    return html, url, 200, "text/html", 5


class FakeResponse:
    def __init__(self, url: str, status: int = 200):
        self.url = url
        self.status = status
        self.released = False

    def release(self) -> None:
        self.released = True


class EvidencePageSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_ai_crawler_replaces_failed_preferred_page(self):
        with tempfile.TemporaryDirectory() as directory:
            conn = connect(Path(directory) / "search.sqlite3")
            add_domain(conn, "example.ai")
            row = conn.execute(
                """
                SELECT id,domain,crawl_failures
                FROM domains WHERE domain='example.ai'
                """
            ).fetchone()
            settings = SimpleNamespace(
                max_pages_per_domain=3,
                recrawl_days=14,
            )

            async def fake_fetch(session, url, ignored_settings):
                if url == "https://example.ai":
                    return fetched_page(url, HOMEPAGE_HTML)
                if url == "https://example.ai/research":
                    raise ValueError("http_404")
                if url == "https://example.ai/founders":
                    return fetched_page(
                        url,
                        "<html><body>Founders from MIT and NASA</body></html>",
                    )
                if url == "https://example.ai/product":
                    return fetched_page(
                        url,
                        "<html><body>Autonomous factory product</body></html>",
                    )
                raise AssertionError(f"Unexpected request: {url}")

            with patch.object(
                ai_crawler,
                "_fetch",
                side_effect=fake_fetch,
            ):
                status = await ai_crawler.crawl_domain(
                    conn,
                    None,
                    row,
                    settings,
                )

            self.assertEqual(status, "success")
            pages = {
                row[0]
                for row in conn.execute(
                    "SELECT final_url FROM pages"
                ).fetchall()
            }
            self.assertEqual(
                pages,
                {
                    "https://example.ai",
                    "https://example.ai/founders",
                    "https://example.ai/product",
                },
            )
            attempts = {
                row[0]
                for row in conn.execute(
                    "SELECT requested_url FROM crawl_attempts"
                ).fetchall()
            }
            self.assertIn("https://example.ai/research", attempts)
            telemetry = conn.execute(
                """
                SELECT last_document_count,last_enrichment_reason
                FROM domains WHERE domain='example.ai'
                """
            ).fetchone()
            self.assertEqual(int(telemetry[0]), 3)
            self.assertEqual(
                telemetry[1],
                "homepage_plausible_company",
            )
            conn.close()

    async def test_zone_crawler_replaces_failed_preferred_page(self):
        settings = SimpleNamespace(
            dns_preflight=False,
            max_pages_per_domain=3,
        )

        async def fake_fetch(session, requested, ignored_settings):
            if requested == "https://example.app":
                return (
                    FakeResponse(requested),
                    HOMEPAGE_HTML,
                    extract_page(HOMEPAGE_HTML, requested),
                )
            if requested == "https://example.app/research":
                raise ValueError("http_404")
            if requested == "https://example.app/founders":
                html = "<html><body>Founders from MIT and NASA</body></html>"
                return (
                    FakeResponse(requested),
                    html,
                    extract_page(html, requested),
                )
            if requested == "https://example.app/product":
                html = "<html><body>Autonomous factory product</body></html>"
                return (
                    FakeResponse(requested),
                    html,
                    extract_page(html, requested),
                )
            raise AssertionError(f"Unexpected request: {requested}")

        with patch.object(
            zone_crawler,
            "_fetch_html_page",
            side_effect=fake_fetch,
        ):
            outcome = await zone_crawler.crawl_domain(
                None,
                "example.app",
                settings,
            )

        self.assertEqual(outcome.status, "success")
        self.assertEqual(outcome.document_count, 3)
        self.assertEqual(
            outcome.enrichment_reason,
            "homepage_plausible_company",
        )
        self.assertIn("Founders from MIT and NASA", outcome.html)
        self.assertIn("Autonomous factory product", outcome.html)
        self.assertNotIn("SOURCE URL: https://example.app/research", outcome.html)
        with tempfile.TemporaryDirectory() as directory:
            conn = connect_zone(Path(directory) / "universe.sqlite3")
            zone_crawler.save_success(
                conn,
                outcome,
                retain_raw_html=False,
            )
            conn.commit()
            saved = conn.execute(
                """
                SELECT document_count,enrichment_reason
                FROM pages WHERE domain='example.app'
                """
            ).fetchone()
            self.assertEqual(int(saved[0]), 3)
            self.assertEqual(
                saved[1],
                "homepage_plausible_company",
            )
            conn.close()

    async def test_zone_crawler_stops_after_parked_homepage(self):
        settings = SimpleNamespace(
            dns_preflight=False,
            max_pages_per_domain=3,
        )
        homepage = """
        <html><body>
          This domain is for sale.
          <a href="/product">Product</a>
        </body></html>
        """
        requests = []

        async def fake_fetch(session, requested, ignored_settings):
            requests.append(requested)
            if requested != "https://forsale.app":
                raise AssertionError(
                    "Parked homepage should not trigger evidence requests"
                )
            return (
                FakeResponse(requested),
                homepage,
                extract_page(homepage, requested),
            )

        with patch.object(
            zone_crawler,
            "_fetch_html_page",
            side_effect=fake_fetch,
        ):
            outcome = await zone_crawler.crawl_domain(
                None,
                "forsale.app",
                settings,
            )

        self.assertEqual(requests, ["https://forsale.app"])
        self.assertEqual(outcome.document_count, 1)
        self.assertEqual(
            outcome.enrichment_reason,
            "homepage_parked_or_for_sale",
        )
        self.assertIsNotNone(outcome.page)
        self.assertTrue(outcome.page.is_parked)


if __name__ == "__main__":
    unittest.main()
