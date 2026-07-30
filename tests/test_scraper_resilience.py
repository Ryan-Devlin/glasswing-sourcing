import asyncio
import unittest
from unittest.mock import patch

import filter as screening_filter
from continuous_pipeline import adaptive_screening_profile


class ScraperResilienceTests(unittest.TestCase):
    def test_apex_success_does_not_open_www_connection(self):
        calls = []

        async def fake_fetch(session, url, timeout):
            calls.append(url)
            return screening_filter.Fetch(url=url, status=200, text="company"), []

        with patch.object(screening_filter, "fetch_html", fake_fetch):
            result = asyncio.run(screening_filter._fetch_candidate_group(
                object(),
                ["https://example.ai", "https://www.example.ai"],
                15,
            ))
        self.assertEqual(calls, ["https://example.ai"])
        self.assertEqual(result[0].status, 200)

    def test_profile_uses_bounded_workers_and_usable_timeout(self):
        profile = adaptive_screening_profile(
            65,
            base_batch=100,
            base_workers=38,
            base_timeout=6,
        )
        self.assertEqual(profile["workers"], 4)
        self.assertEqual(profile["batch_size"], 10)
        self.assertGreaterEqual(profile["timeout"], 15)

    def test_timeout_rows_are_retried_before_persistence(self):
        calls = 0

        async def fake_process(row, session, sem, timeout, homepage_only=False):
            nonlocal calls
            calls += 1
            if calls == 1:
                return {"domain": "example.ai", "fetch_error": "TimeoutError: "}
            return {
                "domain": "example.ai",
                "fetch_error": "",
                "http_status": 200,
                "routing_bucket": "review",
            }

        with patch.object(screening_filter, "process", fake_process):
            output, _ = asyncio.run(screening_filter.screen_records(
                [{"registrable": "example.ai"}],
                workers=38,
                timeout=6,
                homepage_only=True,
                persist=False,
            ))

        self.assertEqual(calls, 2)
        self.assertEqual(output.iloc[0]["http_status"], 200)
        self.assertEqual(output.iloc[0]["fetch_error"], "")

    def test_systemic_timeout_batch_is_not_persisted(self):
        async def always_timeout(row, session, sem, timeout, homepage_only=False):
            return {"domain": row["registrable"], "fetch_error": "TimeoutError: "}

        rows = [{"registrable": f"failed-{index}.ai"} for index in range(5)]
        with patch.object(screening_filter, "process", always_timeout):
            with self.assertRaisesRegex(RuntimeError, "systemic_scraper_timeout"):
                asyncio.run(screening_filter.screen_records(
                    rows,
                    workers=4,
                    timeout=15,
                    homepage_only=True,
                    persist=True,
                ))


if __name__ == "__main__":
    unittest.main()
