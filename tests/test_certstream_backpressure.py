"""The listener has to drain the feed socket, not merely be connected to it.

The failure this covers produced no error anywhere on our side. The client
stayed connected, every counter climbed, ``last_error`` was empty -- and
certstream-server-go was discarding almost the whole feed, because a websocket
that is not read stops reading its transport and the server's answer to a
client it cannot hand a certificate to is to drop the certificate:

    Not providing client '...' with cert because client's buffer is full.
    The client can't keep up. Skipped certs: 4465001

Measured live 2026-08-19, against 333,815 hostnames delivered in the same
window: the pipeline was seeing roughly 3% of the feed. So these tests are
about the two things that decide whether the socket gets drained -- how much
slack the buffer has, and how long the listener parks before calling recv()
again -- and not about any visible symptom, because there wasn't one.
"""

import asyncio
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import websockets

import continuous_pipeline
from intake_score import read_certificate

ALL = {".ai", ".app"}


def queue_db(directory, name="queue.sqlite3"):
    conn = sqlite3.connect(Path(directory) / name)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE domains (
            domain TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            last_processed_at TEXT,
            classification TEXT,
            last_error TEXT DEFAULT '',
            seen_count INTEGER NOT NULL DEFAULT 1,
            first_seen_run_id TEXT NOT NULL DEFAULT '',
            last_seen_run_id TEXT NOT NULL DEFAULT ''
        )
    """)
    continuous_pipeline.ensure_domain_registry_schema(conn)
    return conn


class CountingConnection:
    """Passes everything through, counting the round trips that cost us."""

    def __init__(self, conn):
        self._conn = conn
        self.commits = 0
        self.rollbacks = 0

    def execute(self, *args, **kwargs):
        return self._conn.execute(*args, **kwargs)

    def commit(self):
        self.commits += 1
        return self._conn.commit()

    def rollback(self):
        self.rollbacks += 1
        return self._conn.rollback()


def rows(conn):
    return {
        row["domain"]: (
            row["status"],
            row["seen_count"],
            row["intake_score"],
            row["intake_evidence"],
            row["first_seen_run_id"],
            row["last_seen_run_id"],
        )
        for row in conn.execute("SELECT * FROM domains").fetchall()
    }


class BatchedRegistrationTests(unittest.TestCase):
    """Same writes, same answers, one commit."""

    def test_a_batch_stores_exactly_what_the_serial_loop_stored(self):
        # The equivalence that has to hold before anything else matters: this
        # replaced the per-domain call on the live intake path, so a
        # difference here is a difference in what the pipeline queues.
        certificate = ["a.ai", "b.app", "www.b.app", "c.ai", "junk.example"]
        reading = read_certificate(certificate, ALL)
        scores = {entry.domain: entry for entry in reading.scored}
        domains = sorted(scores)
        self.assertEqual(domains, ["a.ai", "b.app", "c.ai"])

        with tempfile.TemporaryDirectory() as d:
            serial_conn = queue_db(d, "serial.sqlite3")
            serial_results = [
                continuous_pipeline.register_domain_event(
                    serial_conn,
                    domain,
                    ALL,
                    run_id="run-1",
                    intake_score=scores[domain].score,
                    intake_evidence=scores[domain].evidence,
                )
                for domain in domains
            ]

            batch_conn = queue_db(d, "batch.sqlite3")
            batch_results = continuous_pipeline.register_domain_events(
                batch_conn,
                domains,
                ALL,
                run_id="run-1",
                intake_scores=scores,
            )

        # first_seen/last_seen are wall-clock stamps and will differ; every
        # decision-bearing column must not.
        self.assertEqual(rows(serial_conn), rows(batch_conn))
        for serial, batch in zip(serial_results, batch_results):
            self.assertEqual(serial, batch)

    def test_a_repeat_certificate_batches_the_same_way(self):
        # Repeats take the other branch (UPDATE ... RETURNING), and they are
        # the majority of the feed, so they need the same guarantee.
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            first = read_certificate(["foo.ai"], ALL)
            continuous_pipeline.register_domain_events(
                conn, ["foo.ai"], ALL, run_id="run-1",
                intake_scores={e.domain: e for e in first.scored},
            )
            launched = read_certificate(
                ["foo.ai", "app.foo.ai", "api.foo.ai"], ALL
            )
            results = continuous_pipeline.register_domain_events(
                conn, ["foo.ai"], ALL, run_id="run-2",
                intake_scores={e.domain: e for e in launched.scored},
            )

            self.assertEqual(len(results), 1)
            self.assertTrue(results[0]["is_repeat_event"])
            self.assertFalse(results[0]["is_new_domain"])
            row = rows(conn)["foo.ai"]
            self.assertEqual(row[1], 2)
            # The richer certificate still wins, exactly as in the serial path.
            self.assertEqual(row[2], launched.scored[0].score)
            self.assertGreater(launched.scored[0].score, first.scored[0].score)

    def test_a_certificate_costs_one_commit_not_one_per_domain(self):
        # This is the saving. Every commit is a synchronous round trip, and
        # the listener awaited them in sequence before it could return to
        # recv() -- one of them was measured live at 3.639 seconds.
        certificate = ["a.ai", "b.ai", "c.ai", "d.app", "e.app"]
        reading = read_certificate(certificate, ALL)
        scores = {entry.domain: entry for entry in reading.scored}
        domains = sorted(scores)

        with tempfile.TemporaryDirectory() as d:
            serial = CountingConnection(queue_db(d, "serial.sqlite3"))
            for domain in domains:
                continuous_pipeline.register_domain_event(
                    serial, domain, ALL, intake_score=scores[domain].score,
                )
            batch = CountingConnection(queue_db(d, "batch.sqlite3"))
            continuous_pipeline.register_domain_events(
                batch, domains, ALL, intake_scores=scores,
            )

        self.assertEqual(serial.commits, len(domains))
        self.assertEqual(batch.commits, 1)

    def test_a_certificate_with_nothing_tracked_on_it_never_commits(self):
        # 99% of the feed lands here (196,078 of 197,761 domains observed in
        # one live run). It must not cost a round trip.
        with tempfile.TemporaryDirectory() as d:
            conn = CountingConnection(queue_db(d))
            results = continuous_pipeline.register_domain_events(
                conn, ["nope.io", "also-nope.xyz"], ALL,
            )
        self.assertEqual(conn.commits, 0)
        self.assertTrue(all(not r["accepted"] for r in results))

    def test_a_failure_mid_certificate_leaves_no_open_transaction(self):
        # The listener's caller treats any exception as a connection fault and
        # reconnects on the same connection. An abandoned transaction would
        # hold row locks on a connection the pipeline keeps using.
        with tempfile.TemporaryDirectory() as d:
            conn = CountingConnection(queue_db(d))
            with mock.patch.object(
                continuous_pipeline,
                "_register_event_statements",
                side_effect=[
                    {"accepted": True, "is_new_domain": True, "domain": "a.ai",
                     "extension": ".ai", "queued": True,
                     "is_repeat_event": False},
                    RuntimeError("connection dropped mid-certificate"),
                ],
            ):
                with self.assertRaises(RuntimeError):
                    continuous_pipeline.register_domain_events(
                        conn, ["a.ai", "b.ai"], ALL,
                    )
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(conn.commits, 0)


class _RecordingConnect:
    """Records the connect kwargs, then refuses, so the listener loops."""

    def __init__(self):
        self.calls: list[dict] = []

    def __call__(self, url, **kwargs):
        self.calls.append(kwargs)
        return self

    async def __aenter__(self):
        raise RuntimeError("not connecting in this test")

    async def __aexit__(self, *exc_info):
        return False


class SocketBufferTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_listener_asks_for_far_more_slack_than_the_default(self):
        # websockets defaults max_queue to 16 frames. At the domains-only
        # firehose's ~1,700 certificates a second that is about ten
        # milliseconds of slack, so a single status write or registration
        # round trip overran it, the transport stopped being read, and the
        # feed server started discarding certificates it could not deliver.
        # Nothing in this process could see that happen, which is why the
        # value is asserted here rather than inferred from a symptom.
        recorder = _RecordingConnect()
        stop = asyncio.Event()
        env = {
            "CERTSTREAM_URL": "ws://127.0.0.1:1/domains-only",
            "STREAM_RECONNECT_SECONDS": "0.05",
            "STREAM_RECONNECT_MAX_SECONDS": "0.05",
        }
        with mock.patch.dict("os.environ", env), \
             mock.patch.object(continuous_pipeline, "RUNTIME_MODE", "local"), \
             mock.patch.object(continuous_pipeline, "write_status", lambda **_: None), \
             mock.patch.object(continuous_pipeline, "read_status", lambda: {}), \
             mock.patch.object(
                 continuous_pipeline, "selected_extensions", lambda: {".ai"}
             ), \
             mock.patch.object(
                 continuous_pipeline.websockets, "connect", recorder
             ):
            task = asyncio.create_task(continuous_pipeline.listener(None, stop))
            await asyncio.sleep(0.2)
            stop.set()
            await asyncio.wait_for(task, timeout=5)

        self.assertTrue(recorder.calls, "listener never tried to connect")
        max_queue = recorder.calls[0].get("max_queue")
        self.assertIsNotNone(
            max_queue,
            "listener left max_queue at the websockets default of 16 frames",
        )
        self.assertGreaterEqual(max_queue, 1024)

    async def test_the_buffer_size_is_tunable_without_a_redeploy(self):
        # The right value depends on the feed rate and on how long this
        # process's other work blocks the loop, and both move.
        recorder = _RecordingConnect()
        stop = asyncio.Event()
        env = {
            "CERTSTREAM_URL": "ws://127.0.0.1:1/domains-only",
            "STREAM_RECONNECT_SECONDS": "0.05",
            "STREAM_RECONNECT_MAX_SECONDS": "0.05",
            "STREAM_MAX_QUEUE": "2048",
        }
        with mock.patch.dict("os.environ", env), \
             mock.patch.object(continuous_pipeline, "RUNTIME_MODE", "local"), \
             mock.patch.object(continuous_pipeline, "write_status", lambda **_: None), \
             mock.patch.object(continuous_pipeline, "read_status", lambda: {}), \
             mock.patch.object(
                 continuous_pipeline, "selected_extensions", lambda: {".ai"}
             ), \
             mock.patch.object(
                 continuous_pipeline.websockets, "connect", recorder
             ):
            task = asyncio.create_task(continuous_pipeline.listener(None, stop))
            await asyncio.sleep(0.2)
            stop.set()
            await asyncio.wait_for(task, timeout=5)

        self.assertEqual(recorder.calls[0]["max_queue"], 2048)


class HotPathStatusWriteTests(unittest.IsolatedAsyncioTestCase):
    """A new domain must not cost a status file rewrite of its own."""

    async def _run_feed(self, domain_count: int, flush_seconds: str):
        shutdown = asyncio.Event()
        writes: list[dict] = []
        sent = 0

        async def handler(websocket):
            nonlocal sent
            try:
                while not shutdown.is_set() and sent < domain_count:
                    await websocket.send(
                        json.dumps({"data": [f"new{sent}.ai"]})
                    )
                    sent += 1
                    await asyncio.sleep(0)
                await shutdown.wait()
            except websockets.exceptions.ConnectionClosed:
                pass

        def register(conn, domains, *args, **kwargs):
            return [
                {
                    "accepted": True,
                    "queued": True,
                    "is_new_domain": True,
                    "is_repeat_event": False,
                    "domain": domain,
                    "extension": ".ai",
                    "seen_count": 1,
                }
                for domain in domains
            ]

        server = await websockets.serve(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        stop = asyncio.Event()
        env = {
            "CERTSTREAM_URL": f"ws://127.0.0.1:{port}",
            "STREAM_IDLE_TIMEOUT_SECONDS": "99999",
            "REPEAT_STATUS_FLUSH_SECONDS": flush_seconds,
            "INTAKE_STATUS_FLUSH_SECONDS": "99999",
        }
        try:
            with mock.patch.dict("os.environ", env), \
                 mock.patch.object(continuous_pipeline, "RUNTIME_MODE", "local"), \
                 mock.patch.object(
                     continuous_pipeline,
                     "write_status",
                     lambda **updates: writes.append(dict(updates)),
                 ), \
                 mock.patch.object(continuous_pipeline, "read_status", lambda: {}), \
                 mock.patch.object(
                     continuous_pipeline, "selected_extensions", lambda: {".ai"}
                 ), \
                 mock.patch.object(
                     continuous_pipeline, "using_postgres", lambda *a, **k: False
                 ), \
                 mock.patch.object(
                     continuous_pipeline, "register_domain_events", register
                 ):
                task = asyncio.create_task(
                    continuous_pipeline.listener(None, stop)
                )
                while sent < domain_count:
                    await asyncio.sleep(0.02)
                await asyncio.sleep(0.1)
                stop.set()
                await asyncio.wait_for(task, timeout=5)
        finally:
            shutdown.set()
            server.close()
            await asyncio.wait_for(server.wait_closed(), timeout=5)
        return writes, sent

    async def test_new_domains_are_coalesced_into_far_fewer_writes(self):
        # write_status reads the whole status file, re-serialises it and
        # atomically rewrites it. The new-domain path did that twice per
        # domain, on the one code path that must return to recv() promptly.
        writes, sent = await self._run_feed(60, flush_seconds="0.5")
        publishes = [w for w in writes if "new_domains_received" in w]
        self.assertTrue(publishes, "new domains were never published at all")
        self.assertLess(
            len(publishes),
            sent / 4,
            f"{len(publishes)} status writes for {sent} new domains -- the "
            "hot path is still publishing per domain",
        )

    async def test_coalescing_does_not_lose_a_single_domain(self):
        # Throttling the publish is only acceptable because the counters
        # themselves stay exact; the last publish has to be the whole truth.
        writes, sent = await self._run_feed(40, flush_seconds="0.01")
        publishes = [w for w in writes if "new_domains_received" in w]
        self.assertEqual(publishes[-1]["new_domains_received"], sent)
        self.assertEqual(publishes[-1]["domains_received"], sent)
        self.assertEqual(
            publishes[-1]["received_by_extension"][".ai"], sent
        )
        self.assertEqual(publishes[-1]["last_domain_received"], f"new{sent - 1}.ai")

    async def test_a_new_domain_publish_carries_the_repeat_totals_with_it(self):
        # The forced flush this replaced existed to stop a new-domain publish
        # from carrying stale repeat totals. One write for both keeps that.
        writes, _ = await self._run_feed(20, flush_seconds="0.01")
        for write in writes:
            if "new_domains_received" in write:
                self.assertIn("repeat_events_ignored", write)
                self.assertIn("certificate_events_received", write)


class PerFrameWorkTests(unittest.IsolatedAsyncioTestCase):
    """Nothing that opens a connection may sit on the per-certificate path."""

    async def _flood(self, *, frames: int, runtime_mode: str):
        """Run the real listener against a server flooding it with frames."""
        shutdown = asyncio.Event()
        sent = 0
        control_reads = 0
        consumed: list[int] = []

        async def handler(websocket):
            nonlocal sent
            try:
                while not shutdown.is_set() and sent < frames:
                    await websocket.send(
                        json.dumps({"data": [f"host{sent}.example"]})
                    )
                    sent += 1
                    await asyncio.sleep(0)
                await shutdown.wait()
            except websockets.exceptions.ConnectionClosed:
                pass

        def read_control():
            nonlocal control_reads
            control_reads += 1
            return {"desired": {"pipeline": True}}

        def record(**updates):
            if "stream_frames_received" in updates:
                consumed.append(int(updates["stream_frames_received"]))

        server = await websockets.serve(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        stop = asyncio.Event()
        env = {
            "CERTSTREAM_URL": f"ws://127.0.0.1:{port}",
            "STREAM_IDLE_TIMEOUT_SECONDS": "99999",
            "INTAKE_STATUS_FLUSH_SECONDS": "1",
            "REPEAT_STATUS_FLUSH_SECONDS": "1",
        }
        started = 0.0
        elapsed = 0.0
        try:
            with mock.patch.dict("os.environ", env), \
                 mock.patch.object(
                     continuous_pipeline, "RUNTIME_MODE", runtime_mode
                 ), \
                 mock.patch.object(continuous_pipeline, "write_status", record), \
                 mock.patch.object(continuous_pipeline, "read_status", lambda: {}), \
                 mock.patch.object(
                     continuous_pipeline, "read_control", read_control
                 ), \
                 mock.patch.object(
                     continuous_pipeline, "selected_extensions", lambda: {".ai"}
                 ), \
                 mock.patch.object(
                     continuous_pipeline, "using_postgres", lambda *a, **k: False
                 ), \
                 mock.patch.object(
                     continuous_pipeline,
                     "register_domain_events",
                     lambda conn, domains, *a, **k: [],
                 ):
                started = asyncio.get_running_loop().time()
                task = asyncio.create_task(
                    continuous_pipeline.listener(None, stop)
                )
                while sent < frames:
                    await asyncio.sleep(0.01)
                elapsed = asyncio.get_running_loop().time() - started
                stop.set()
                await asyncio.wait_for(task, timeout=5)
        finally:
            shutdown.set()
            server.close()
            await asyncio.wait_for(server.wait_closed(), timeout=5)
        return sent, control_reads, elapsed

    async def test_the_pause_flag_is_not_re_read_for_every_certificate(self):
        # read_control() opens a fresh Postgres connection, runs one SELECT
        # and closes it. It was called once per loop iteration, and the loop
        # iterates once per certificate, so intake was capped at roughly one
        # certificate per connection setup -- measured live at 30.3 a second
        # while the feed server discarded ~580 a second it could not hand
        # over. The cap is invisible from inside the process: no error, no
        # disconnect, just a slow client.
        sent, control_reads, _ = await self._flood(
            frames=400, runtime_mode="cloud"
        )
        self.assertGreaterEqual(sent, 400)
        self.assertLess(
            control_reads,
            sent / 10,
            f"read_control() ran {control_reads} times for {sent} frames -- "
            "a connection setup is still sitting on the intake hot path",
        )

    async def test_a_local_run_never_reads_the_control_store_at_all(self):
        # Local mode has no cross-service control store; the short-circuit
        # that kept it out of the loop must survive the caching.
        _, control_reads, _ = await self._flood(
            frames=200, runtime_mode="local"
        )
        self.assertEqual(control_reads, 0)

    async def test_the_listener_keeps_up_with_a_flooding_feed(self):
        # The property, rather than one instance of it: whatever ends up on
        # this path, the listener has to consume the feed far faster than the
        # feed server's patience. The floor is deliberately far below what
        # parsing and a Public Suffix List lookup actually cost, so this fails
        # only when per-frame I/O has crept back in -- the live rate when it
        # last did was 30 frames a second.
        sent, _, elapsed = await self._flood(
            frames=1500, runtime_mode="cloud"
        )
        rate = sent / max(elapsed, 1e-6)
        self.assertGreater(
            rate,
            200,
            f"listener consumed only {rate:.0f} frames/second",
        )


if __name__ == "__main__":
    unittest.main()
