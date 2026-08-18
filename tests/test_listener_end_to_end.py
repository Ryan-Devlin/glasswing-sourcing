"""The listener, end to end, against a live local feed.

The bug this exists to prevent was invisible: a healthy feed, the UI showing
"Running", every counter at zero, and no error anywhere. An exception raised
while processing a frame was caught by the listener's reconnect handler and
read as a connection fault, so the worker reconnected forever instead of
working.

Nothing in the unit tests could see that -- each piece was individually
correct. Only running the real listener against a real socket and asserting
that domains reach the database catches it.
"""

import asyncio
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import websockets

import continuous_pipeline as cp

EXTENSIONS = {".ai", ".io", ".app", ".com", ".dev"}

# One realistic burst: a company that shipped, a bare registration, both farms,
# an untracked TLD, a bare public suffix, and a platform certificate.
FRAMES = [
    {"data": ["exa.ai", "www.exa.ai", "app.exa.ai", "api.exa.ai"]},
    {"data": ["quietco.ai", "www.quietco.ai"]},
    {"data": ["000091fj.app", "004936gg.app", "0003939pgl1.app"]},
    {"data": ["auth-8510-system.ai", "private-1971-system.ai"]},
    {"data": ["something.net", "other.org"]},
    {"data": ["com.ai"]},
    {"data": [f"tenant{i}.com" for i in range(14)]},
]


def queue_db(directory):
    conn = sqlite3.connect(Path(directory) / "queue.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE domains (
            domain TEXT PRIMARY KEY, first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT,
            last_processed_at TEXT, classification TEXT,
            last_error TEXT DEFAULT '', seen_count INTEGER NOT NULL DEFAULT 1,
            first_seen_run_id TEXT NOT NULL DEFAULT '',
            last_seen_run_id TEXT NOT NULL DEFAULT ''
        )
    """)
    cp.ensure_domain_registry_schema(conn)
    return conn


class ListenerEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.conn = queue_db(self._dir.name)
        self.status = []
        self.connections = 0
        self._shutdown = asyncio.Event()

    async def asyncTearDown(self):
        self.conn.close()
        self._dir.cleanup()

    async def _handler(self, websocket):
        self.connections += 1
        try:
            while not self._shutdown.is_set():
                for frame in FRAMES:
                    await websocket.send(json.dumps(frame))
                    await asyncio.sleep(0.02)
        except websockets.exceptions.ConnectionClosed:
            pass

    async def _run(self, seconds=2.5):
        server = await websockets.serve(self._handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        stop = asyncio.Event()
        env = {
            "CERTSTREAM_URL": f"ws://127.0.0.1:{port}",
            "STREAM_IDLE_TIMEOUT_SECONDS": "60",
            "STREAM_RECONNECT_SECONDS": "0.1",
            "INTAKE_STATUS_FLUSH_SECONDS": "1",
        }
        try:
            with mock.patch.dict("os.environ", env), \
                 mock.patch.object(cp, "RUNTIME_MODE", "local"), \
                 mock.patch.object(cp, "write_status",
                                   lambda **kw: self.status.append(kw)), \
                 mock.patch.object(cp, "read_status", lambda: {}), \
                 mock.patch.object(cp, "selected_extensions",
                                   lambda: set(EXTENSIONS)), \
                 mock.patch.object(cp, "using_postgres", lambda *a, **k: False):
                task = asyncio.create_task(cp.listener(self.conn, stop))
                await asyncio.sleep(seconds)
                stop.set()
                await asyncio.wait_for(task, timeout=5)
        finally:
            self._shutdown.set()
            server.close()
            await asyncio.wait_for(server.wait_closed(), timeout=5)

    def latest(self, key, default=None):
        for entry in reversed(self.status):
            if key in entry:
                return entry[key]
        return default

    async def test_a_healthy_feed_is_processed_without_reconnecting(self):
        # The exact signature of the regression: a processing exception read
        # as a connection fault produced a reconnect storm on a healthy feed.
        await self._run()
        self.assertEqual(self.connections, 1)
        errors = [e for e in (self.latest("last_error"),) if e]
        self.assertEqual(errors, [], f"listener reported an error: {errors}")

    async def test_real_domains_reach_the_queue_with_scores(self):
        # "Running with every counter at zero" is the failure this catches.
        await self._run()
        rows = {
            r["domain"]: r["intake_score"]
            for r in self.conn.execute(
                "SELECT domain, intake_score FROM domains"
            )
        }
        self.assertIn("exa.ai", rows)
        self.assertIn("quietco.ai", rows)
        # The company that shipped an app must outrank the bare registration.
        self.assertGreater(rows["exa.ai"], rows["quietco.ai"])

    async def test_garbage_never_reaches_the_queue(self):
        await self._run()
        queued = {
            r["domain"] for r in self.conn.execute("SELECT domain FROM domains")
        }
        for junk in ("000091fj.app", "004936gg.app", "auth-8510-system.ai",
                     "private-1971-system.ai", "something.net", "com.ai"):
            with self.subTest(junk=junk):
                self.assertNotIn(junk, queued)

    async def test_platform_certificates_are_queued_at_the_floor(self):
        await self._run()
        rows = {
            r["domain"]: r["intake_score"]
            for r in self.conn.execute(
                "SELECT domain, intake_score FROM domains"
            )
        }
        self.assertIn("tenant0.com", rows)
        self.assertLess(rows["tenant0.com"], -1)

    async def test_intake_counters_are_published(self):
        await self._run()
        self.assertGreater(self.latest("hostnames_seen", 0), 0)
        self.assertGreater(self.latest("hostnames_filtered", 0), 0)

    async def test_every_guard_is_attributed_in_the_published_tally(self):
        await self._run()
        drops = self.latest("intake_drops", {})
        self.assertIsInstance(drops, dict)
        for reason in ("machine_generated", "templated_bulk",
                       "extension_not_tracked", "no_registrable_label"):
            with self.subTest(reason=reason):
                self.assertGreater(drops.get(reason, 0), 0)


if __name__ == "__main__":
    unittest.main()
