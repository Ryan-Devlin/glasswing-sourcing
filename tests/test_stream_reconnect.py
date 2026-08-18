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

    def test_listener_disconnects_while_pipeline_is_paused(self):
        listener = self.pipeline.split("async def listener(", 1)[1].split(
            "async def worker(", 1
        )[0]
        self.assertIn('stream_connection_state="paused"', listener)
        self.assertIn('read_control()["desired"].get', listener)


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
        self.assertIn(
            '20.0 if profile["mode"] in {"surge", "catch_up"} else 120.0',
            self.pipeline,
        )

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
        self.assertIn("mark_success(conn, succeeded_domains)", before_except)
        self.assertIn("mark_failure(", before_except)

    def test_no_separate_auto_rejected_counter(self):
        # 2026-08-08: reverted -- a Python "reject" is retained and shows
        # up as real Hard Denied now (see automated_discard_mask in
        # filter.py), so there is no separate "auto-rejected" concept for
        # the worker to track. Three buckets only, no fourth accounting
        # category standing in for one of them behind the scenes.
        self.assertNotIn("auto_rejected", self.pipeline)


class IdleFrameWatchdogTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = Path("continuous_pipeline.py").read_text(
            encoding="utf-8"
        )
        self.listener = self.pipeline.split("async def listener(", 1)[1].split(
            "\ndef read_status(", 1
        )[0]

    def test_silent_but_connected_socket_is_dropped_and_reconnected(self):
        # ping_timeout=None is deliberate (a certificate burst delays pongs
        # on a healthy feed) but it removes the only guard that would ever
        # drop a half-open socket. A feed server that accepts the connection
        # and then broadcasts nothing left the listener reporting
        # connected=True with zero domains arriving, forever: the 1s recv
        # timeout just continued the loop. The watchdog is the replacement.
        self.assertIn("idle_timeout_seconds", self.listener)
        self.assertIn("STREAM_IDLE_TIMEOUT_SECONDS", self.listener)
        self.assertIn("idle_seconds >= idle_timeout_seconds", self.listener)

    def test_watchdog_does_not_kill_the_screening_worker(self):
        # The upstream daemon can os._exit and let the platform restart it;
        # this process also owns the screening worker, so the watchdog must
        # only drop the socket, never take an in-flight batch down.
        self.assertNotIn("os._exit(", self.listener)

    def test_arriving_frames_reset_the_silence_clock(self):
        # Frames arriving is exactly what separates "slow under load" from
        # "dead socket" -- a busy listener must never trip its own watchdog.
        self.assertIn("last_frame_at = time.monotonic()", self.listener)

    def test_silence_is_published_so_frozen_is_visible_from_slow(self):
        self.assertIn("stream_seconds_since_frame", self.listener)
        self.assertIn("stream_idle_watchdog_count", self.listener)

    def test_ping_timeout_stays_disabled(self):
        # The watchdog replaces the pong deadline; it must not be an excuse
        # to re-enable one and start dropping healthy busy connections.
        self.assertIn("ping_timeout=None", self.listener)


class ReconnectBackoffTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = Path("continuous_pipeline.py").read_text(
            encoding="utf-8"
        )
        self.listener = self.pipeline.split("async def listener(", 1)[1].split(
            "\ndef read_status(", 1
        )[0]

    def test_reconnect_backs_off_exponentially_to_a_ceiling(self):
        # A flat 5s retry against a feed server that is down (rather than
        # blipping) is a hot loop that rewrites the status file every 5s
        # indefinitely.
        self.assertIn("reconnect_backoff", self.listener)
        self.assertIn("reconnect_max_seconds", self.listener)
        self.assertIn(
            "min(\n                reconnect_backoff * 2, reconnect_max_seconds\n            )",
            self.listener,
        )

    def test_backoff_sleeps_the_backoff_not_the_flat_interval(self):
        self.assertIn("await asyncio.sleep(reconnect_backoff)", self.listener)
        self.assertNotIn("await asyncio.sleep(reconnect_seconds)", self.listener)

    def test_backoff_resets_only_once_a_connection_delivers_a_frame(self):
        # Connecting successfully to a silent feed is not proof of health,
        # so the reset hangs off a received frame, not off connect().
        self.assertIn("connection_delivered_frame", self.listener)
        self.assertIn(
            "reconnect_backoff = reconnect_seconds", self.listener
        )
