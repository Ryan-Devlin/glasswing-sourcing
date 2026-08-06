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

    def test_listener_refreshes_extensions_instead_of_caching_forever(self):
        # listener() is created once per container lifetime (main() never
        # restarts it) -- confirmed live: computing allowed_extensions only
        # once at the top meant changing Extensions in the dashboard had
        # zero effect until the whole container redeployed. It must be
        # re-read periodically, not just once before the receive loop.
        listener = self.pipeline.split("async def listener(", 1)[1].split(
            "\nasync def ", 1
        )[0]
        self.assertIn("extensions_refreshed_at", listener)
        self.assertGreaterEqual(
            listener.count("selected_extensions()"), 2
        )


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
        self.assertIn("timeout=outer_deadline_seconds", after.split("\n\n", 1)[0])
        self.assertIn("outer_deadline_seconds = 180.0", self.pipeline)

    def test_timeout_error_reports_which_batch_and_settings_hit_the_deadline(self):
        # Confirmed live: 30s and then 90s both still cut off real batches
        # before they finished once claim_batch started surfacing fresh,
        # live domains (real HTTP work) instead of the old dead/parked
        # backlog (instant failures) -- a bare "TimeoutError:" with no
        # detail made it impossible to tell a genuine hang apart from a
        # batch that just needed a bit more room without another
        # deploy-and-wait cycle.
        after_screen_records = self.pipeline.split(
            "screening_filter.screen_records(", 1
        )[1]
        section = after_screen_records.split(
            "except asyncio.TimeoutError:", 1
        )[1]
        section = section.split("except Exception as exc:", 1)[0]
        self.assertIn("outer_deadline_", section)
        self.assertIn("batch=", section)
        self.assertIn("per_domain_timeout=", section)

    def test_timed_out_batch_is_caught_by_the_existing_failure_handler(self):
        # asyncio.wait_for raises TimeoutError, which must still land in the
        # same except Exception block that already calls mark_failure and
        # updates the Failed counter -- otherwise a timeout would itself go
        # unhandled and crash the worker instead of just failing one batch.
        after = self.pipeline.split("screening_filter.screen_records(", 1)[1]
        before_except = after.split("except Exception as exc:", 1)[0]
        self.assertIn("mark_success(conn, domains)", before_except)
