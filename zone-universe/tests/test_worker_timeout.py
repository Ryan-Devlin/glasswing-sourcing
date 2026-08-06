from __future__ import annotations

from pathlib import Path


class TestCrawlOnceHasAnOuterDeadline:
    def setup_method(self):
        self.worker = Path(
            Path(__file__).resolve().parents[1]
            / "src"
            / "zone_universe"
            / "worker.py"
        ).read_text(encoding="utf-8")

    def test_crawl_once_call_is_wrapped_with_a_timeout(self):
        # Confirmed live: the worker got stuck self-reporting "paused by
        # user"/"caught up" indefinitely -- Quick checks and Admitted to
        # full crawl both frozen at 0 for hours despite millions of queued
        # candidates and a "Desired: running" state. Per-request timeouts
        # inside crawl_once bound one HTTP fetch, not the call as a whole,
        # so a hang anywhere else blocked the entire worker forever.
        before, after = self.worker.split("crawl_once(\n", 1)
        assert before.rstrip().endswith("asyncio.wait_for(")
        assert "timeout=600.0" in after.split("\n\n", 1)[0]

    def test_timed_out_crawl_is_caught_by_the_existing_error_handler(self):
        after = self.worker.split("crawl_once(\n", 1)[1]
        before_except = after.split("except Exception as exc:", 1)[0]
        assert "made_progress = made_progress or bool(counts)" in before_except
