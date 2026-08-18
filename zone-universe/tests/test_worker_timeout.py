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


class TestDailySnapshotDoesNotBlockCrawling:
    def setup_method(self):
        self.worker = Path(
            Path(__file__).resolve().parents[1]
            / "src"
            / "zone_universe"
            / "worker.py"
        ).read_text(encoding="utf-8")

    def test_snapshot_cycle_is_scheduled_rather_than_awaited_inline(self):
        # Confirmed live: 71,988 queued .app domains sat untouched behind
        # crawler_status=streaming_daily_com_snapshot with the crawler already
        # resumed. A .com stream runs about an hour, so awaiting it inside the
        # loop stalls every crawl for that whole window, once a day.
        # The one await lives in the background cycle, never in the crawl loop.
        assert self.worker.count("await download_scheduled_snapshots(") == 1
        cycle_body = self.worker.split(
            "async def scheduled_snapshot_cycle", 1
        )[1].split("\n    try:", 1)[0]
        assert "await download_scheduled_snapshots(" in cycle_body
        after_check = self.worker.split(
            "next_snapshot_check = now_monotonic + SNAPSHOT_CHECK_SECONDS", 1
        )[1]
        assert after_check.lstrip().startswith(
            "snapshot_task = asyncio.create_task("
        )

    def test_snapshot_cycle_uses_its_own_database_connection(self):
        # The crawl loop commits on `conn` continuously. Sharing a connection
        # would let either side commit the other's half-finished transaction.
        cycle = self.worker.split("async def scheduled_snapshot_cycle", 1)[1]
        body = cycle.split("\n    try:", 1)[0]
        assert "with connect(settings.database) as snapshot_conn:" in body
        assert "snapshot_conn, settings" in body

    def test_only_one_snapshot_cycle_is_in_flight_at_a_time(self):
        # asyncio.to_thread cannot be cancelled, so a second concurrent cycle
        # would double the disk the import needs and race it in the database.
        assert (
            "if snapshot_task is None and now_monotonic >= next_snapshot_check:"
            in self.worker
        )

    def test_a_cancelled_snapshot_is_not_recorded_as_a_download_error(self):
        after = self.worker.split("snapshot_task.result()", 1)[1]
        assert after.split("\n", 2)[1].strip() == "except asyncio.CancelledError:"

    def test_shutdown_cancels_an_in_flight_snapshot(self):
        finally_block = self.worker.split("    finally:", 1)[1]
        assert "snapshot_task.cancel()" in finally_block
