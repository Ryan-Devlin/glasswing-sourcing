"""Batch submit/collect lifecycle, against a fake client.

The money-losing failures here are silent ones: submitting a second batch
while one is outstanding (double spend the accounting never sees), or
clearing the state id before results are read (a paid batch stranded with no
way to fetch it). Both are asserted directly.
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import claude_batch as cb


class FakeResults:
    def __init__(self, entries):
        self._entries = entries

    def __aiter__(self):
        async def gen():
            for entry in self._entries:
                yield entry
        return gen()


class FakeBatches:
    def __init__(self, *, status="ended", entries=(), batch_id="batch_1"):
        self.status = status
        self.entries = list(entries)
        self.batch_id = batch_id
        self.created = []

    async def create(self, requests):
        self.created.append(requests)
        return SimpleNamespace(id=self.batch_id)

    async def retrieve(self, batch_id):
        return SimpleNamespace(id=batch_id, processing_status=self.status)

    def results(self, batch_id):
        return FakeResults(self.entries)


class FakeClient:
    def __init__(self, batches):
        self.messages = SimpleNamespace(batches=batches)


def ok(custom_id, text="verdict"):
    return SimpleNamespace(
        custom_id=custom_id,
        result=SimpleNamespace(type="succeeded", message=text),
    )


def failed(custom_id, kind="errored"):
    return SimpleNamespace(custom_id=custom_id, result=SimpleNamespace(type=kind))


class CustomIdTests(unittest.TestCase):
    def test_domains_are_sanitised_into_a_safe_id(self):
        self.assertEqual(cb.custom_id_for("foo.com.ai"), "foo_com_ai")
        self.assertEqual(cb.custom_id_for("  EXA.AI "), "exa_ai")

    def test_empty_input_still_yields_an_id(self):
        # An empty custom_id is rejected by the API and fails the whole batch,
        # not just the one row.
        self.assertEqual(cb.custom_id_for(""), "unknown")
        self.assertEqual(cb.custom_id_for(None), "unknown")

    def test_ids_are_length_bounded(self):
        self.assertLessEqual(len(cb.custom_id_for("a" * 500 + ".ai")), 60)


class StateTests(unittest.TestCase):
    def test_missing_file_reads_as_empty_not_an_error(self):
        with TemporaryDirectory() as d:
            self.assertFalse(cb.read_state(Path(d) / "nope.json").in_flight)

    def test_corrupt_file_reads_as_empty_rather_than_raising(self):
        # A parse error must not take diligence down; the cost of being wrong
        # is one duplicate batch, not lost corpus data.
        with TemporaryDirectory() as d:
            path = Path(d) / "s.json"
            path.write_text("{not json", encoding="utf-8")
            self.assertFalse(cb.read_state(path).in_flight)

    def test_round_trip_preserves_the_id_and_mapping(self):
        with TemporaryDirectory() as d:
            path = Path(d) / "s.json"
            cb.write_state(
                cb.BatchState("b1", "2026-08-18T00:00:00Z", {"a": "a.ai"}, {"a": {"domain": "a.ai"}}),
                path
            )
            state = cb.read_state(path)
            self.assertEqual(state.batch_id, "b1")
            self.assertEqual(state.domains, {"a": "a.ai"})
            self.assertTrue(state.in_flight)


class BatchLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """Submit/collect against a fake client.

    unittest's async case rather than pytest-asyncio: this project ships no
    async pytest plugin, and a test file should not be the reason a runtime
    dependency lands.
    """

    def setUp(self):
        self._dir = TemporaryDirectory()
        self.path = Path(self._dir.name) / "batch_state.json"

    def tearDown(self):
        self._dir.cleanup()

    async def test_submit_records_the_batch_id(self):
        batches = FakeBatches()
        batch_id = await cb.submit(
            FakeClient(batches), [("a_ai", {"model": "m"})], path=self.path
        )
        self.assertEqual(batch_id, "batch_1")
        self.assertEqual(cb.read_state(self.path).batch_id, "batch_1")
        self.assertEqual(batches.created[0][0]["custom_id"], "a_ai")

    async def test_submit_refuses_while_a_batch_is_outstanding(self):
        # A second submission would double the day's spend without the
        # caller's accounting ever seeing it, since a batch is only counted
        # once its results come back.
        batches = FakeBatches()
        client = FakeClient(batches)
        await cb.submit(client, [("a_ai", {})], path=self.path)
        second = await cb.submit(client, [("b_ai", {})], path=self.path)
        self.assertEqual(second, "")
        self.assertEqual(len(batches.created), 1)
        self.assertEqual(cb.read_state(self.path).batch_id, "batch_1")

    async def test_submitting_nothing_is_a_no_op(self):
        batches = FakeBatches()
        self.assertEqual(await cb.submit(FakeClient(batches), [], path=self.path), "")
        self.assertEqual(batches.created, [])

    async def test_collect_with_nothing_in_flight_is_idle(self):
        result = await cb.collect(FakeClient(FakeBatches()), path=self.path)
        self.assertEqual(result, ({}, [], "idle", {}))

    async def test_a_running_batch_keeps_its_state_for_the_next_run(self):
        # Clearing here would strand a batch that is already paid for.
        batches = FakeBatches(status="in_progress")
        client = FakeClient(batches)
        await cb.submit(client, [("a_ai", {})], path=self.path)
        _, _, status, _ = await cb.collect(client, path=self.path)
        self.assertEqual(status, "running")
        self.assertEqual(cb.read_state(self.path).batch_id, "batch_1")

    async def test_ended_batch_returns_results_and_clears_state(self):
        batches = FakeBatches(status="ended", entries=[ok("a_ai"), ok("b_ai")])
        client = FakeClient(batches)
        await cb.submit(client, [("a_ai", {}), ("b_ai", {})], path=self.path)
        responses, failures, status, _ = await cb.collect(client, path=self.path)
        self.assertEqual(status, "ended")
        self.assertEqual(set(responses), {"a_ai", "b_ai"})
        self.assertEqual(failures, [])
        self.assertFalse(cb.read_state(self.path).in_flight)

    async def test_per_row_failures_are_reported_not_dropped(self):
        # A dropped failure is a candidate that silently never gets a verdict
        # and never gets requeued either.
        batches = FakeBatches(
            status="ended",
            entries=[ok("a_ai"), failed("b_ai", "errored"), failed("c_ai", "expired")],
        )
        client = FakeClient(batches)
        await cb.submit(
            client, [("a_ai", {}), ("b_ai", {}), ("c_ai", {})], path=self.path
        )
        responses, failures, _, _ = await cb.collect(client, path=self.path)
        self.assertEqual(set(responses), {"a_ai"})
        self.assertEqual(dict(failures), {"b_ai": "errored", "c_ai": "expired"})

    async def test_a_new_batch_can_be_submitted_once_the_previous_ended(self):
        batches = FakeBatches(status="ended", entries=[ok("a_ai")])
        client = FakeClient(batches)
        await cb.submit(client, [("a_ai", {})], path=self.path)
        await cb.collect(client, path=self.path)
        batches.batch_id = "batch_2"
        self.assertEqual(
            await cb.submit(client, [("b_ai", {})], path=self.path), "batch_2"
        )


if __name__ == "__main__":
    unittest.main()


class RowRoundTripTests(unittest.IsolatedAsyncioTestCase):
    """A batched verdict must be persistable exactly like a synchronous one.

    The persistence path merges the whole candidate row into what it stores.
    A batch answers a run later, so if the row did not survive the gap the two
    tiers would write different columns for the same kind of verdict.
    """

    def setUp(self):
        self._dir = TemporaryDirectory()
        self.path = Path(self._dir.name) / "batch_state.json"

    def tearDown(self):
        self._dir.cleanup()

    async def test_rows_survive_submit_and_come_back_on_collect(self):
        row = {"domain": "a.ai", "page_title": "A", "homepage_excerpt": "x" * 100}
        batches = FakeBatches(status="ended", entries=[ok("a_ai")])
        client = FakeClient(batches)
        await cb.submit(client, [("a_ai", {})], rows={"a_ai": row}, path=self.path)
        _, _, status, rows = await cb.collect(client, path=self.path)
        self.assertEqual(status, "ended")
        self.assertEqual(rows["a_ai"], row)

    async def test_rows_are_still_available_while_the_batch_runs(self):
        row = {"domain": "a.ai"}
        client = FakeClient(FakeBatches(status="in_progress"))
        await cb.submit(client, [("a_ai", {})], rows={"a_ai": row}, path=self.path)
        self.assertEqual(cb.read_state(self.path).rows["a_ai"], row)
