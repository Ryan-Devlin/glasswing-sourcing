import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import filter as screening_filter
import continuous_pipeline
from continuous_pipeline import adaptive_screening_profile


class ScraperResilienceTests(unittest.TestCase):
    def test_standalone_pipeline_can_import_both_crawler_packages(self):
        project_root = Path(continuous_pipeline.__file__).resolve().parent
        self.assertIn(
            str(project_root / "ai-domain-search-engine" / "src"),
            sys.path,
        )
        self.assertIn(
            str(project_root / "zone-universe" / "src"),
            sys.path,
        )
        from ai_search import database as ai_database
        from zone_universe import database as zone_database

        self.assertTrue(callable(ai_database.connect))
        self.assertTrue(callable(zone_database.connect))

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

    def test_mostly_timed_out_batch_skips_the_slow_retry_pass(self):
        # Confirmed live: retrying a mostly-timed-out batch at a fixed 2
        # workers could take longer than the caller's outer deadline all by
        # itself (e.g. 75 domains / 2 workers * 25s+ each), turning a real
        # network problem into an apparent hang with zero progress. When
        # most of a batch times out on the first pass, skip the slow retry
        # and let the systemic-timeout check below fail fast instead.
        calls = 0

        async def always_timeout(row, session, sem, timeout, homepage_only=False):
            nonlocal calls
            calls += 1
            return {"domain": row["registrable"], "fetch_error": "TimeoutError: "}

        rows = [{"registrable": f"failed-{index}.ai"} for index in range(10)]
        with patch.object(screening_filter, "process", always_timeout):
            with self.assertRaisesRegex(RuntimeError, "systemic_scraper_timeout"):
                asyncio.run(screening_filter.screen_records(
                    rows,
                    workers=32,
                    timeout=15,
                    homepage_only=True,
                    persist=True,
                ))

        self.assertEqual(calls, 10)

    def test_client_timeout_bounds_the_whole_request_not_just_each_phase(self):
        # Confirmed live: an entire 75-domain batch hung for 600+ seconds
        # with zero exception and zero progress. total=None meant no ceiling
        # bounded a request as a whole -- sock_read resets on every
        # successful chunk, so a server trickling bytes just under that
        # interval could "make progress" forever without ever tripping
        # connect/sock_connect/sock_read, stalling the entire batch (every
        # request runs inside one asyncio.gather()) on a single site.
        source = Path("filter.py").read_text(encoding="utf-8")
        self.assertNotIn("total=None,", source)
        self.assertIn("total=selected_timeout * 2,", source)

    def test_connector_uses_a_native_async_dns_resolver(self):
        # Confirmed live: two whole 75-domain batches hung 600+ seconds each,
        # back to back, even after the total= fix -- aiohttp's default
        # resolver runs getaddrinfo() in a thread-pool executor, and
        # cancelling the Task waiting on it does not stop that thread. A
        # domain whose DNS never resolves and never errors burns one
        # executor thread forever; enough of those (accumulated across
        # repeated failed batches) exhausts the pool and hangs every future
        # lookup regardless of any per-request timeout. AsyncResolver (via
        # aiodns) does DNS natively on the event loop, so a cancelled lookup
        # is actually cancelled.
        source = Path("filter.py").read_text(encoding="utf-8")
        connector_section = source.split(
            "aiohttp.TCPConnector(", 1
        )[1].split("\n\n", 1)[0]
        self.assertIn("resolver=aiohttp.AsyncResolver()", connector_section)

    def test_aiodns_is_a_declared_dependency(self):
        requirements = Path("requirements.txt").read_text(encoding="utf-8")
        self.assertIn("aiodns", requirements)


if __name__ == "__main__":
    unittest.main()
