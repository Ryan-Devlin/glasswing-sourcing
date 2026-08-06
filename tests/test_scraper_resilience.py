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

    def test_persist_screening_output_does_not_block_the_event_loop(self):
        # Confirmed live: the batch outer deadline fired 40-79s late twice
        # in a row even after capping response size and ruling out slow
        # per-domain scoring (no SLOW_SCORING was ever logged).
        # persist_screening_output runs a batch's worth of synchronous
        # Postgres writes -- calling it directly inside screen_records
        # (an async function) blocks the whole event loop for however long
        # that takes, which is exactly the kind of delay that would make
        # asyncio.wait_for's own cancellation fire late.
        source = Path("filter.py").read_text(encoding="utf-8")
        section = source.split("output = pd.DataFrame(succeeded_rows)", 1)[1].split(
            "\n\n", 1
        )[0]
        self.assertIn("_SCORING_EXECUTOR", section)
        self.assertIn("persist_screening_output, output", section)

    def test_process_offloads_scoring_to_a_real_thread(self):
        # Confirmed live: the batch outer deadline fired late by 35-80s
        # across multiple attempts, even after ruling out a single huge
        # download and any individual slow-scoring domain (no SLOW_SCORING
        # was ever logged). The remaining explanation is cumulative cost:
        # BeautifulSoup parsing plus a dozen+ regex-based *_assessment
        # calls, run in series for every domain in a 75-domain batch on the
        # single asyncio event-loop thread, with no single domain slow
        # enough to trip an outlier alarm but the total adding up to real
        # delay. Offloading each domain's scoring to asyncio.to_thread lets
        # the OS thread pool actually run this CPU-bound work in parallel
        # instead of serializing it on one thread.
        source = Path("filter.py").read_text(encoding="utf-8")
        process_body = source.split("async def process(", 1)[1].split(
            "\nasync def ", 1
        )[0].split("\ndef ", 1)[0]
        self.assertIn("def _score_and_bucket_domain():", process_body)
        self.assertIn(
            "run_in_executor(\n        _SCORING_EXECUTOR, _score_and_bucket_domain\n    )",
            process_body,
        )

    def test_all_timed_out_batch_persists_nothing_but_reports_the_timeouts(self):
        # Confirmed live: this used to raise and discard the whole batch
        # once 80%+ timed out -- fine when every row is a timeout, but the
        # same all-or-nothing check also discarded a batch's real,
        # successfully-fetched companies whenever timeouts crossed that
        # threshold. It no longer raises; nothing here succeeded, so
        # nothing is persisted, but the timed-out domains are reported for
        # the caller's normal retry path instead of just being lost.
        async def always_timeout(row, session, sem, timeout, homepage_only=False):
            return {"domain": row["registrable"], "fetch_error": "TimeoutError: "}

        rows = [{"registrable": f"failed-{index}.ai"} for index in range(5)]
        with patch.object(screening_filter, "process", always_timeout):
            output, stats = asyncio.run(screening_filter.screen_records(
                rows,
                workers=4,
                timeout=15,
                homepage_only=True,
                persist=False,
            ))

        self.assertTrue(output.empty)
        self.assertEqual(
            set(stats["timed_out_domains"]),
            {f"failed-{index}.ai" for index in range(5)},
        )

    def test_partial_timeout_batch_persists_the_real_successes(self):
        # The concrete cost of the old all-or-nothing behavior: a batch
        # where most domains are brand-new and not yet live (a normal
        # CertStream reality, not a bug) would discard the handful that
        # genuinely did succeed right along with the timeouts.
        async def mostly_timeout(row, session, sem, timeout, homepage_only=False):
            domain = row["registrable"]
            if domain == "real-company.ai":
                return {
                    "domain": domain,
                    "fetch_error": "",
                    "http_status": 200,
                    "routing_bucket": "review",
                }
            return {"domain": domain, "fetch_error": "TimeoutError: "}

        rows = [{"registrable": f"failed-{index}.ai"} for index in range(9)]
        rows.append({"registrable": "real-company.ai"})
        with patch.object(screening_filter, "process", mostly_timeout):
            output, stats = asyncio.run(screening_filter.screen_records(
                rows,
                workers=32,
                timeout=15,
                homepage_only=True,
                persist=False,
            ))

        self.assertEqual(list(output["domain"]), ["real-company.ai"])
        self.assertNotIn("real-company.ai", stats["timed_out_domains"])
        self.assertEqual(len(stats["timed_out_domains"]), 9)

    def test_mostly_timed_out_batch_skips_the_slow_retry_pass(self):
        # Confirmed live: retrying a mostly-timed-out batch at a fixed 2
        # workers could take longer than the caller's outer deadline all by
        # itself (e.g. 75 domains / 2 workers * 25s+ each), turning a real
        # network problem into an apparent hang with zero progress. When
        # most of a batch times out on the first pass, skip the slow retry
        # instead.
        calls = 0

        async def always_timeout(row, session, sem, timeout, homepage_only=False):
            nonlocal calls
            calls += 1
            return {"domain": row["registrable"], "fetch_error": "TimeoutError: "}

        rows = [{"registrable": f"failed-{index}.ai"} for index in range(10)]
        with patch.object(screening_filter, "process", always_timeout):
            asyncio.run(screening_filter.screen_records(
                rows,
                workers=32,
                timeout=15,
                homepage_only=True,
                persist=False,
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

    def test_read_html_capped_stops_at_the_byte_limit(self):
        # Confirmed live: fetch_html previously used response.text() with no
        # size cap at all -- a batch's outer deadline fired 40s late
        # (220.5s against a 180s ceiling), which asyncio.wait_for can only
        # do if the event loop was blocked by synchronous work that long.
        # Downloading and then BeautifulSoup-parsing a pathologically large
        # page (before the 45,000-char text truncation ever applies) is
        # exactly that kind of blocking work.
        class FakeContent:
            async def iter_chunked(self, size):
                for _ in range(20):
                    yield b"x" * size

        class FakeResponse:
            content = FakeContent()
            charset = "utf-8"

        html = asyncio.run(
            screening_filter._read_html_capped(FakeResponse(), maximum_bytes=100_000)
        )
        self.assertEqual(len(html), 100_000)

    def test_fetch_html_uses_the_capped_reader_not_response_text(self):
        source = Path("filter.py").read_text(encoding="utf-8")
        fetch_html_body = source.split("async def fetch_html(", 1)[1].split(
            "\nasync def ", 1
        )[0]
        self.assertIn("_read_html_capped(", fetch_html_body)
        self.assertNotIn("response.text(", fetch_html_body)


if __name__ == "__main__":
    unittest.main()
