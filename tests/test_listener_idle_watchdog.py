"""Behavioural test for the listener's dead-socket watchdog.

The sibling assertions in ``test_stream_reconnect.py`` match strings in the
source, which proves the watchdog code is *present* but not that it *works*.
These tests run the real ``listener()`` against a real local websocket server
that accepts the connection and then behaves badly, and assert on what the
listener actually does.

The failure being reproduced: ``ping_timeout=None`` is set deliberately (a
certificate burst delays pongs on a healthy feed), so nothing in the websocket
layer will ever drop a half-open socket, and ``recv()``'s 1s timeout merely
continues the loop. Before the watchdog, a server that accepted a connection
and then said nothing left the listener parked forever, reporting
``connected=True`` with zero domains arriving.
"""

import asyncio
import json
import unittest
from unittest import mock

import websockets

import continuous_pipeline


class ListenerHarness:
    """Runs the real listener against a scripted local websocket server."""

    def __init__(self, *, silent: bool, idle_timeout: str = "1"):
        self.silent = silent
        self.idle_timeout = idle_timeout
        self.connections = 0
        self.status_writes: list[dict] = []
        # Handlers wait on this rather than sleeping, so shutting the server
        # down cannot block on an un-cancellable sleep in a live handler.
        self._shutdown = asyncio.Event()

    async def _handler(self, websocket):
        self.connections += 1
        try:
            if self.silent:
                # Accept, then broadcast nothing at all -- the dead-feed mode
                # that ping_timeout=None leaves undetectable.
                await self._shutdown.wait()
            else:
                while not self._shutdown.is_set():
                    await websocket.send(json.dumps({"data": ["example.ai"]}))
                    await asyncio.sleep(0.1)
        except websockets.exceptions.ConnectionClosed:
            pass

    def _record(self, **updates):
        self.status_writes.append(dict(updates))

    async def run(self, seconds: float):
        server = await websockets.serve(self._handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        stop = asyncio.Event()
        env = {
            "CERTSTREAM_URL": f"ws://127.0.0.1:{port}",
            "STREAM_IDLE_TIMEOUT_SECONDS": self.idle_timeout,
            "STREAM_RECONNECT_SECONDS": "0.1",
            "STREAM_RECONNECT_MAX_SECONDS": "0.2",
            "REPEAT_STATUS_FLUSH_SECONDS": "0.5",
        }
        try:
            with mock.patch.dict("os.environ", env), \
                 mock.patch.object(continuous_pipeline, "RUNTIME_MODE", "local"), \
                 mock.patch.object(continuous_pipeline, "write_status", self._record), \
                 mock.patch.object(continuous_pipeline, "read_status", lambda: {}), \
                 mock.patch.object(
                     continuous_pipeline, "selected_extensions", lambda: {".ai"}
                 ), \
                 mock.patch.object(
                     continuous_pipeline, "using_postgres", lambda *_a, **_k: False
                 ), \
                 mock.patch.object(
                     continuous_pipeline,
                     "register_domain_event",
                     lambda *a, **k: {"accepted": False},
                 ):
                task = asyncio.create_task(continuous_pipeline.listener(None, stop))
                await asyncio.sleep(seconds)
                stop.set()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.TimeoutError:
                    task.cancel()
                    raise AssertionError(
                        "listener did not return after stop was set"
                    )
        finally:
            # Release the handlers first: a live handler keeps wait_closed()
            # pending, which would hang the suite instead of failing it.
            self._shutdown.set()
            server.close()
            await asyncio.wait_for(server.wait_closed(), timeout=5)

    def key(self, name):
        return [w[name] for w in self.status_writes if name in w]


class SilentFeedTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchdog_drops_and_reopens_a_silent_socket(self):
        harness = ListenerHarness(silent=True, idle_timeout="1")
        await harness.run(seconds=3.0)

        fired = harness.key("stream_idle_watchdog_count")
        self.assertTrue(
            fired,
            "watchdog never fired against a server that accepted the "
            "connection and then sent nothing",
        )
        self.assertGreaterEqual(fired[-1], 1)
        self.assertGreater(
            harness.connections,
            1,
            "socket was never reopened -- the listener stayed parked on the "
            "dead connection instead of reconnecting",
        )

    async def test_watchdog_reports_the_silence_and_the_reason(self):
        harness = ListenerHarness(silent=True, idle_timeout="1")
        await harness.run(seconds=3.0)

        self.assertTrue(harness.key("stream_seconds_since_frame"))
        self.assertTrue(harness.key("last_stream_idle_watchdog_at"))
        errors = [e for e in harness.key("last_error") if e]
        self.assertTrue(
            any("idle-frame watchdog" in e for e in errors),
            f"no diagnosable reason published; got {errors}",
        )

    async def test_listener_reports_disconnected_while_reconnecting(self):
        # The bug's signature was connected=True with nothing arriving, so
        # the watchdog must clear that flag rather than leave it stale.
        harness = ListenerHarness(silent=True, idle_timeout="1")
        await harness.run(seconds=3.0)
        self.assertIn(False, harness.key("connected"))


    async def test_the_bug_itself_reproduces_when_the_watchdog_cannot_fire(self):
        # The control case, and the reason the other assertions mean
        # something. A very long idle timeout is the pre-change listener:
        # the watchdog can never trip, so nothing ever notices the silence.
        # It must park on the one dead connection forever -- if this ever
        # starts reconnecting on its own, the tests above are passing for
        # some other reason and no longer prove anything about the watchdog.
        harness = ListenerHarness(silent=True, idle_timeout="99999")
        await harness.run(seconds=3.0)

        self.assertEqual(harness.connections, 1)
        self.assertEqual(harness.key("stream_idle_watchdog_count"), [])
        # And the signature that made this invisible in production: it kept
        # insisting it was connected the whole time.
        self.assertIn(True, harness.key("connected"))


class LiveFeedTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_feed_delivering_frames_never_trips_the_watchdog(self):
        # The property that makes the watchdog safe: arriving frames reset
        # the clock, so a busy listener cannot kill its own healthy socket.
        # Idle timeout is 1s here while the server sends every 0.1s.
        harness = ListenerHarness(silent=False, idle_timeout="1")
        await harness.run(seconds=3.0)

        self.assertEqual(
            harness.key("stream_idle_watchdog_count"),
            [],
            "watchdog fired against a feed that was delivering frames",
        )
        self.assertEqual(
            harness.connections,
            1,
            "healthy connection was dropped and reopened",
        )
