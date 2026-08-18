"""Behaviour of the funnel stage arithmetic.

Pure counts in, percentages out — no database, so every rule about how a drop
is distinguished from a backlog is asserted directly.
"""

import unittest

from funnel import (
    Funnel, Stage, Throughput, biggest_losses, certstream_funnel, screening_funnel,
)


class DropVersusBacklogTests(unittest.TestCase):
    def test_backlog_is_excluded_from_the_drop_rate(self):
        # The distinction the whole view rests on. A stage that has ruled on
        # 10 of 1,000 rows and thrown out 9 is dropping 90% of what it judges,
        # not 0.9%. Putting the backlog in the denominator would report this
        # filter as harmless and send you tuning the wrong stage.
        stage = Stage(key="s", label="S", entered=1000, dropped=9, pending=990)
        self.assertEqual(stage.decided, 10)
        self.assertAlmostEqual(stage.drop_rate, 0.9)
        self.assertAlmostEqual(stage.pending_rate, 0.99)
        self.assertEqual(stage.survived, 1)

    def test_pending_rows_are_not_counted_as_survivors(self):
        stage = Stage(key="s", label="S", entered=100, dropped=0, pending=70)
        self.assertEqual(stage.survived, 30)

    def test_a_stage_with_nothing_decided_has_a_zero_drop_rate(self):
        stage = Stage(key="s", label="S", entered=500, dropped=0, pending=500)
        self.assertEqual(stage.decided, 0)
        self.assertEqual(stage.drop_rate, 0.0)

    def test_empty_stage_does_not_divide_by_zero(self):
        stage = Stage(key="s", label="S", entered=0)
        self.assertEqual(stage.drop_rate, 0.0)
        self.assertEqual(stage.pending_rate, 0.0)
        self.assertEqual(stage.survived, 0)

    def test_counts_never_go_negative_on_inconsistent_input(self):
        # The queue and classification stores are read in separate queries and
        # can disagree by a few rows mid-write. That must not render a
        # negative survivor count in the UI.
        stage = Stage(key="s", label="S", entered=10, dropped=99, pending=0)
        self.assertEqual(stage.survived, 0)


class ScreeningFunnelTests(unittest.TestCase):
    def setUp(self):
        self.funnel = screening_funnel(
            queued=1000, unscreened=700, screened=300,
            classified=200, ghost=50, insufficient=100, rejected=10,
            candidates=40, claude_reviewed=25, claude_kept=5,
        )
        self.stages = {s.key: s for s in self.funnel.stages}

    def test_the_chain_is_arithmetically_closed(self):
        # Each stage's entered must equal the previous stage's survived, or
        # the reported percentages describe a funnel that doesn't exist.
        for previous, nxt in zip(self.funnel.stages, self.funnel.stages[1:]):
            with self.subTest(stage=nxt.key):
                self.assertEqual(nxt.entered, previous.survived)

    def test_backlog_appears_at_the_queue_and_at_claude_only(self):
        # Those are the two places work waits rather than being rejected.
        with_pending = {s.key for s in self.funnel.stages if s.pending}
        self.assertEqual(with_pending, {"queued", "claude_reviewed"})

    def test_unscreened_queue_is_pending_not_dropped(self):
        queued = self.stages["queued"]
        self.assertEqual(queued.pending, 700)
        self.assertEqual(queued.dropped, 0)
        self.assertEqual(queued.survived, 300)

    def test_uncalled_candidates_are_pending_not_dropped(self):
        reviewed = self.stages["claude_reviewed"]
        self.assertEqual(reviewed.pending, 15)
        self.assertEqual(reviewed.dropped, 0)

    def test_failed_fetches_are_dropped_not_pending(self):
        # A domain that was fetched and produced no verdict is a real loss;
        # it already consumed the crawl and will not be retried by this stage.
        screened = self.stages["screened"]
        self.assertEqual(screened.dropped, 100)
        self.assertEqual(screened.pending, 0)

    def test_final_survivors_match_the_kept_count(self):
        self.assertEqual(self.funnel.survived, 5)
        self.assertAlmostEqual(self.funnel.to_dict()["overall_survival_rate"], 0.005)

    def test_share_of_start_is_cumulative_attrition(self):
        payload = self.funnel.to_dict()
        by_key = {s["key"]: s for s in payload["stages"]}
        self.assertAlmostEqual(by_key["queued"]["share_of_start"], 0.3)
        self.assertAlmostEqual(by_key["reachable"]["share_of_start"], 0.15)


class CertstreamFunnelTests(unittest.TestCase):
    """One run-scoped funnel: hostnames in, queued domains out.

    Replaces two separate views. Splitting the guards from the deduplication
    made the reader do arithmetic between two tables for no gain.
    """

    def setUp(self):
        self.funnel = certstream_funnel(
            1_000,
            {"extension_not_tracked": 700, "machine_generated": 20},
            new_domains=50,
            repeat_domains=30,
        )
        self.stages = {s.key: s for s in self.funnel.stages}

    def test_the_chain_is_closed_from_hostnames_to_queued_domains(self):
        for previous, nxt in zip(self.funnel.stages, self.funnel.stages[1:]):
            with self.subTest(stage=nxt.key):
                self.assertEqual(nxt.entered, previous.survived)
        self.assertEqual(self.funnel.survived, 50)

    def test_guards_run_in_declared_order_each_seeing_the_previous_survivors(self):
        from certstream_filter import DROP_REASONS

        guard_keys = [s.key for s in self.funnel.stages][: len(DROP_REASONS)]
        self.assertEqual(guard_keys, list(DROP_REASONS))

    def test_multi_san_collapse_is_a_drop_at_the_reduction_step(self):
        # 280 hostnames survive the guards but only 80 distinct domains come
        # out; the 200 collapsed entries belong here, not to dedup.
        self.assertEqual(self.stages["registrable"].dropped, 200)

    def test_repeat_observations_drop_at_the_queue_step(self):
        self.assertEqual(self.stages["new_to_queue"].dropped, 30)

    def test_a_run_with_no_traffic_yields_no_negative_counts(self):
        empty = certstream_funnel(0, {})
        for stage in empty.stages:
            with self.subTest(stage=stage.key):
                self.assertGreaterEqual(stage.survived, 0)
        self.assertEqual(empty.survived, 0)

    def test_the_note_says_it_is_one_run(self):
        # A cumulative counter cannot answer "is intake working right now".
        self.assertIn("run", self.funnel.to_dict()["note"].lower())


class BiggestLossesTests(unittest.TestCase):
    def test_ranked_by_absolute_count_not_by_rate(self):
        # A 100% drop rate on 12 rows is not where the funnel loses its
        # inventory; ranking by rate would surface it above a stage that
        # discards thousands.
        funnel = Funnel(name="t", stages=[
            Stage(key="big", label="Big", entered=10_000, dropped=5_000),
            Stage(key="total", label="Total", entered=12, dropped=12),
        ])
        self.assertEqual([l["key"] for l in biggest_losses(funnel)], ["big", "total"])

    def test_stages_that_drop_nothing_are_omitted(self):
        funnel = Funnel(name="t", stages=[
            Stage(key="a", label="A", entered=100, dropped=0, pending=100),
            Stage(key="b", label="B", entered=100, dropped=5),
        ])
        self.assertEqual([l["key"] for l in biggest_losses(funnel)], ["b"])


if __name__ == "__main__":
    unittest.main()


class ThroughputTests(unittest.TestCase):
    """Arrival against fetch rate — the capacity question.

    The number this replaces was never measured: a "373/min arriving vs
    ~97/min capacity" figure lived in a code comment and got quoted as fact.
    The local queue snapshot could not settle it either, having stopped
    receiving data weeks earlier, so it described a machine nobody was
    running. This has to be computed against the live database.
    """

    def test_overload_ratio_is_arrivals_per_fetch(self):
        t = Throughput(window_hours=24, arrived=30_000, fetched=7_500)
        self.assertEqual(t.overload_ratio, 4.0)
        self.assertFalse(t.keeping_up)

    def test_keeping_up_when_screening_matches_arrival(self):
        t = Throughput(window_hours=24, arrived=5_000, fetched=5_000)
        self.assertTrue(t.keeping_up)
        self.assertEqual(t.overload_ratio, 1.0)

    def test_screening_faster_than_arrival_still_reads_as_keeping_up(self):
        # Draining a backlog is healthy, not an anomaly.
        t = Throughput(window_hours=24, arrived=1_000, fetched=4_000)
        self.assertTrue(t.keeping_up)

    def test_a_stalled_worker_is_not_reported_as_keeping_up(self):
        # Zero fetches and a growing queue looks identical to keeping up if
        # you only watch the backlog delta. It is the opposite condition, and
        # dividing by zero would have reported it as an infinite ratio.
        t = Throughput(window_hours=24, arrived=5_000, fetched=0)
        self.assertEqual(t.overload_ratio, 0.0)
        self.assertFalse(t.keeping_up)
        self.assertTrue(t.to_dict()["stalled"])

    def test_an_idle_window_is_neither_stalled_nor_keeping_up(self):
        t = Throughput(window_hours=24, arrived=0, fetched=0)
        self.assertFalse(t.to_dict()["stalled"])
        self.assertFalse(t.keeping_up)

    def test_rates_are_per_minute_over_the_window(self):
        t = Throughput(window_hours=24, arrived=1_440, fetched=720)
        self.assertAlmostEqual(t.arrival_per_min, 1.0)
        self.assertAlmostEqual(t.fetch_per_min, 0.5)

    def test_a_zero_window_does_not_divide_by_zero(self):
        t = Throughput(window_hours=0, arrived=10, fetched=5)
        self.assertEqual(t.arrival_per_min, 0.0)
        self.assertEqual(t.fetch_per_min, 0.0)
