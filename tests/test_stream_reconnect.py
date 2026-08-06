import unittest
from pathlib import Path


class StreamReconnectTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = Path("continuous_pipeline.py").read_text(
            encoding="utf-8"
        )
        self.dashboard = Path("dashboard.py").read_text(encoding="utf-8")

    def test_busy_listener_does_not_enforce_pong_deadline(self):
        self.assertIn("ping_interval=20", self.pipeline)
        self.assertIn("ping_timeout=None", self.pipeline)

    def test_successful_reconnect_clears_active_error(self):
        self.assertIn('stream_connection_state="connected"', self.pipeline)
        self.assertIn('last_error=""', self.pipeline)
        self.assertIn("stream_reconnect_count", self.pipeline)
        self.assertIn("last_stream_disconnect", self.pipeline)

    def test_listener_does_not_own_processing_stage(self):
        listener = self.pipeline.split("async def listener(", 1)[1].split(
            "def read_status", 1
        )[0]
        self.assertNotIn('stage="listening"', listener)
        self.assertNotIn('stage="reconnecting"', listener)

    def test_reconnect_is_not_presented_as_pipeline_crash(self):
        self.assertIn(
            'connection_state in {"connecting", "reconnecting"}',
            self.dashboard,
        )
        self.assertIn("reconnecting automatically", self.dashboard)


if __name__ == "__main__":
    unittest.main()


class ScreeningBatchTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = Path("continuous_pipeline.py").read_text(
            encoding="utf-8"
        )

    def test_screen_records_call_has_an_outer_deadline(self):
        # Confirmed live: the worker got stuck reporting stage="screening"
        # indefinitely (Classified/Failed both frozen at 0, no exception
        # ever raised, no log output) -- the per-domain fetch timeout bounds
        # one HTTP request, not the batch call as a whole, so a hang
        # anywhere else inside screen_records blocked the entire worker
        # forever with zero visibility.
        before, after = self.pipeline.split(
            "screening_filter.screen_records(", 1
        )
        self.assertTrue(before.rstrip().endswith("asyncio.wait_for("))
        self.assertIn("timeout=600.0", after.split("\n\n", 1)[0])

    def test_timed_out_batch_is_caught_by_the_existing_failure_handler(self):
        # asyncio.wait_for raises TimeoutError, which must still land in the
        # same except Exception block that already calls mark_failure and
        # updates the Failed counter -- otherwise a timeout would itself go
        # unhandled and crash the worker instead of just failing one batch.
        after = self.pipeline.split("screening_filter.screen_records(", 1)[1]
        before_except = after.split("except Exception as exc:", 1)[0]
        self.assertIn("mark_success(conn, domains)", before_except)
