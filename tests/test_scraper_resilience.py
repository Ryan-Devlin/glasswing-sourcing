import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

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

    def test_apex_and_www_are_raced_for_low_latency(self):
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
        self.assertCountEqual(
            calls,
            ["https://example.ai", "https://www.example.ai"],
        )
        self.assertEqual(result[0].status, 200)

    def test_profile_uses_bounded_workers_and_usable_timeout(self):
        profile = adaptive_screening_profile(
            65,
            base_batch=100,
            base_workers=38,
            base_timeout=6,
        )
        self.assertEqual(profile["workers"], 4)
        # batch_size must match workers -- confirmed live: a fixed 10
        # against a 4-worker cap meant ~3 sequential rounds per batch
        # (up to 75s at this mode's 25s ceiling) before the last domain
        # in even the first batch after a reset got attempted.
        self.assertEqual(profile["batch_size"], 4)
        self.assertGreaterEqual(profile["timeout"], 15)

    def test_backlog_profiles_cap_memory_intensive_concurrency(self):
        surge = adaptive_screening_profile(
            10_000, base_batch=75, base_workers=28, base_timeout=12
        )
        catch_up = adaptive_screening_profile(
            500, base_batch=75, base_workers=28, base_timeout=12
        )

        # Surge concurrency must stay one wave (batch_size == workers) and
        # keep enough headroom over the live arrival rate to drain a backlog,
        # while staying well under the 40 that was previously blamed for mass
        # timeouts. Tunable via STREAM_SURGE_WORKERS without a redeploy.
        self.assertEqual(surge["batch_size"], surge["workers"])
        self.assertGreaterEqual(surge["workers"], 16)
        self.assertLessEqual(surge["workers"], 32)
        self.assertLessEqual(surge["timeout"], 8)
        self.assertEqual(catch_up["batch_size"], 8)
        self.assertEqual(catch_up["workers"], 8)
        self.assertLessEqual(catch_up["timeout"], 10)

    def test_html_payload_default_is_bounded_for_many_parallel_sites(self):
        self.assertLessEqual(screening_filter._FETCH_MAX_RESPONSE_BYTES, 500_000)

    def test_pipeline_recycles_above_memory_budget(self):
        with patch.dict("os.environ", {"GLASSWING_PIPELINE_RECYCLE_RSS_MB": "1200"}):
            self.assertFalse(continuous_pipeline.should_recycle_worker(1199))
            self.assertTrue(continuous_pipeline.should_recycle_worker(1201))

    def test_backlog_call_skips_duplicate_immediate_timeout_retry(self):
        source = Path("continuous_pipeline.py").read_text(encoding="utf-8")
        call = source.split("screening_filter.screen_records(", 1)[1].split(
            "timeout=outer_deadline_seconds", 1
        )[0]
        self.assertIn('retry_timeouts=profile["mode"] == "normal"', call)

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


class ClaudeDecisionViewsTests(unittest.TestCase):
    """"If a domain is ever passed through Claude and accepted, it appends
    straight to the master investable and the local investable" -- MAYBE
    must never count, and each source's local view must only ever contain
    that source's own domains."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.filtered = root / "filtered"
        self.filtered.mkdir()
        self.llm = root / "llm"
        self.llm.mkdir()
        self.ai_db = root / "ai_search.sqlite3"
        self.zone_db = root / "zone_universe.sqlite3"

        with sqlite3.connect(self.ai_db) as conn:
            conn.execute(
                "CREATE TABLE domains (domain TEXT, last_crawled_at TEXT)"
            )
            conn.execute(
                "INSERT INTO domains VALUES ('ai-keep.ai', '2026-08-08')"
            )
        with sqlite3.connect(self.zone_db) as conn:
            conn.execute("CREATE TABLE pages (domain TEXT)")
            conn.execute("INSERT INTO pages VALUES ('zone-reject.com')")

        pd.DataFrame([
            {"domain": "ai-keep.ai", "llm_decision": "KEEP"},
            {"domain": "cert-keep.ai", "llm_decision": "KEEP"},
            {"domain": "cert-maybe.ai", "llm_decision": "MAYBE"},
        ]).to_csv(self.llm / "llm_approved_master.csv", index=False)
        pd.DataFrame([
            {"domain": "zone-reject.com", "llm_decision": "REJECT"},
        ]).to_csv(self.llm / "llm_denied_master.csv", index=False)

        self.patches = [
            patch.object(screening_filter, "FILTERED", self.filtered),
            patch.object(screening_filter, "LLM", self.llm),
            patch.object(
                screening_filter,
                "crawler_database_paths",
                return_value=(self.ai_db, self.zone_db),
            ),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    def _domains(self, filename: str) -> set[str]:
        path = self.filtered / filename
        if not path.exists() or path.stat().st_size == 0:
            return set()
        frame = pd.read_csv(path, dtype=str).fillna("")
        return set(frame.get("domain", []))

    def test_maybe_never_counts_as_master_investable(self):
        screening_filter.persist_claude_decision_views()
        master = self._domains("master_investable_claude.csv")
        self.assertIn("ai-keep.ai", master)
        self.assertIn("cert-keep.ai", master)
        self.assertNotIn("cert-maybe.ai", master)

    def test_master_hard_denied_is_reject_only(self):
        screening_filter.persist_claude_decision_views()
        self.assertEqual(
            self._domains("master_hard_denied_claude.csv"),
            {"zone-reject.com"},
        )

    def test_local_views_only_contain_that_sources_own_domains(self):
        screening_filter.persist_claude_decision_views()
        self.assertEqual(
            self._domains("crawled_investable_claude.csv"), {"ai-keep.ai"}
        )
        self.assertEqual(
            self._domains("zone_crawled_hard_denied_claude.csv"),
            {"zone-reject.com"},
        )
        # cert-keep.ai belongs to neither crawler's own corpus database --
        # CertStream's local view is everything left over.
        self.assertEqual(
            self._domains("certstream_investable_claude.csv"),
            {"cert-keep.ai"},
        )
        self.assertNotIn(
            "ai-keep.ai", self._domains("certstream_investable_claude.csv")
        )


class ClaudePipelineIsFixedNotEditableTests(unittest.TestCase):
    """"I don't want users to be able to edit the Claude stuff" -- the
    score gate and budget are fixed constants in code, not a dashboard
    setting or a shared-config lookup."""

    def test_allowance_ignores_run_config_entirely(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE claude_calls (called_at TEXT, domain TEXT)")
        conn.commit()
        # No load_run_config() call at all -- even a config that would have
        # disabled Claude under the old auto_claude toggle must not matter.
        with patch.object(
            continuous_pipeline, "load_run_config",
            return_value={"auto_claude": False, "claude_hourly_limit": 0},
        ):
            allowance = continuous_pipeline.claude_allowance(conn)
        self.assertEqual(allowance, continuous_pipeline.CLAUDE_HOURLY_LIMIT)

    def test_fixed_constants_match_the_specified_pipeline(self):
        self.assertEqual(continuous_pipeline.CLAUDE_MINIMUM_SCORE, 60)
        self.assertGreater(
            continuous_pipeline.CLAUDE_EXCEPTIONAL_SCORE,
            continuous_pipeline.CLAUDE_MINIMUM_SCORE,
        )

    def test_maybe_run_claude_command_uses_fixed_constants(self):
        source = Path("continuous_pipeline.py").read_text(encoding="utf-8")
        body = source.split("def maybe_run_claude(", 1)[1].split(
            "\ndef ", 1
        )[0]
        self.assertIn("str(CLAUDE_MINIMUM_SCORE)", body)
        self.assertIn("str(CLAUDE_EXCEPTIONAL_SCORE)", body)
        self.assertIn("CLAUDE_BATCH_MAX", body)
        self.assertNotIn('config.get("claude_', body)
        self.assertNotIn('config.get("auto_claude"', body)

    def test_dashboard_has_no_editable_claude_controls(self):
        dashboard = Path("dashboard.py").read_text(encoding="utf-8")
        self.assertNotIn("Claude minimum score", dashboard)
        self.assertNotIn("claude_minimum_score", dashboard)
        self.assertNotIn("claude_hourly_limit", dashboard)
        self.assertNotIn("claude_daily_limit", dashboard)
        self.assertNotIn("claude_batch_max", dashboard)
        self.assertIn("Fixed, not user-editable", dashboard)


if __name__ == "__main__":
    unittest.main()

    def test_surge_workers_are_tunable_without_a_redeploy(self):
        with patch.dict("os.environ", {"STREAM_SURGE_WORKERS": "12"}):
            profile = adaptive_screening_profile(
                10_000, base_batch=75, base_workers=28, base_timeout=12
            )
        self.assertEqual(profile["workers"], 12)
        self.assertEqual(profile["batch_size"], 12)

        # An absurd value must still be clamped rather than trusted.
        with patch.dict("os.environ", {"STREAM_SURGE_WORKERS": "500"}):
            profile = adaptive_screening_profile(
                10_000, base_batch=75, base_workers=28, base_timeout=12
            )
        self.assertEqual(profile["workers"], 40)

    def test_candidate_frame_is_built_outside_the_worker_process(self):
        """The worker must never hold the whole-corpus candidate frame.

        Confirmed live: building it in-process put the always-on worker at
        ~1.78 GB, past its own 1200 MB GLASSWING_PIPELINE_RECYCLE_RSS_MB
        ceiling, so the post-batch check os.execv'd the process after roughly
        every batch. The dashboard counted ~24 domains -- exactly one surge
        wave -- then reset every per-run counter to zero, on repeat.
        """
        source = Path("continuous_pipeline.py").read_text(encoding="utf-8")
        # The in-process load must be gone entirely, import included.
        self.assertNotIn("frame_from_store", source)
        self.assertIn("build_claude_candidates.py", source)
        self.assertTrue(Path("build_claude_candidates.py").exists())

    def test_candidate_builder_reports_an_empty_store_distinctly(self):
        # rc=2 means "nothing to do", which must not be treated as a failure
        # warning, and must not leave a stale file behind to be mistaken for
        # real candidates.
        builder = Path("build_claude_candidates.py").read_text(encoding="utf-8")
        self.assertIn("return 2", builder)
        self.assertIn("unlink(missing_ok=True)", builder)

        source = Path("continuous_pipeline.py").read_text(encoding="utf-8")
        self.assertIn("if build.returncode == 2:", source)
