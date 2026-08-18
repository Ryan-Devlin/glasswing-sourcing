"""Behaviour of the funnel stage arithmetic.

Pure counts in, percentages out — no database, so every rule about how a drop
is distinguished from a backlog is asserted directly.
"""

import unittest

from funnel import (
    Funnel, Stage, Throughput, biggest_losses, run_funnel,
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


class RunFunnelTests(unittest.TestCase):
    """One pipeline, one unit, scoped to one CertStream run."""

    def setUp(self):
        self.funnel = run_funnel(
            received=1_000, unscreened=300, no_verdict=200,
            ghost=150, insufficient=250, rejected=40, candidates=55,
        )
        self.stages = {s.key: s for s in self.funnel.stages}

    def test_the_top_is_exactly_the_domains_received_this_run(self):
        # The requirement this view exists to satisfy: the number at the top
        # must equal what CertStream added to the queue during this run, with
        # no reconciliation step in between.
        self.assertEqual(self.funnel.stages[0].entered, 1_000)
        self.assertEqual(self.stages["received"].label,
                         "Domains received from CertStream")

    def test_the_chain_is_arithmetically_closed(self):
        for previous, nxt in zip(self.funnel.stages, self.funnel.stages[1:]):
            with self.subTest(stage=nxt.key):
                self.assertEqual(nxt.entered, previous.survived)

    def test_every_stage_counts_domains_so_nothing_changes_unit(self):
        # Mixing hostname-level guard counts into this table is what produced
        # a 4040% drop rate: a cumulative tally measured against a reset
        # population. No stage here counts anything but queue rows.
        for stage in self.funnel.stages:
            with self.subTest(stage=stage.key):
                self.assertLessEqual(stage.dropped, stage.entered)
                self.assertLessEqual(stage.drop_rate, 1.0)

    def test_unscreened_domains_are_pending_not_dropped(self):
        self.assertEqual(self.stages["received"].pending, 300)
        self.assertEqual(self.stages["received"].dropped, 0)

    def test_the_survivors_are_the_candidate_pool(self):
        self.assertEqual(self.funnel.survived, 55)

    def test_an_idle_run_produces_no_negative_counts(self):
        empty = run_funnel(received=0, unscreened=0, no_verdict=0, ghost=0,
                           insufficient=0, rejected=0, candidates=0)
        for stage in empty.stages:
            with self.subTest(stage=stage.key):
                self.assertGreaterEqual(stage.survived, 0)
                self.assertEqual(stage.drop_rate, 0.0)

    def test_inconsistent_inputs_never_render_negative(self):
        # The two counts are read in one query, but a mid-write read can still
        # disagree; the UI must not show a negative.
        weird = run_funnel(received=10, unscreened=99, no_verdict=99, ghost=99,
                           insufficient=99, rejected=99, candidates=99)
        for stage in weird.stages:
            with self.subTest(stage=stage.key):
                self.assertGreaterEqual(stage.survived, 0)
                self.assertGreaterEqual(stage.entered, 0)


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
