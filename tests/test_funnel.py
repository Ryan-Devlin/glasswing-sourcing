"""Behaviour of the funnel stage arithmetic.

Pure counts in, percentages out — no database, so every rule about how a drop
is distinguished from a backlog is asserted directly.
"""

import unittest

from funnel import (
    Funnel, Stage, Throughput, biggest_losses, build_funnel,
)


class StageTests(unittest.TestCase):
    """IN and OUT only — there is deliberately no backlog column.

    An earlier version reported "waiting" separately, which put a third
    number in every row that a reader had to add back before the arithmetic
    closed. A domain not yet reached leaves the funnel at that step like any
    other; the question is what shaved the population and by how much.
    """

    def test_a_stage_is_only_in_and_out(self):
        stage = Stage(key="s", label="S", entered=1000, dropped=900)
        self.assertEqual(stage.survived, 100)
        self.assertAlmostEqual(stage.drop_rate, 0.9)
        self.assertFalse(hasattr(stage, "pending"))

    def test_empty_stage_does_not_divide_by_zero(self):
        stage = Stage(key="s", label="S", entered=0)
        self.assertEqual(stage.drop_rate, 0.0)
        self.assertEqual(stage.survived, 0)

    def test_counts_never_go_negative_on_inconsistent_input(self):
        self.assertEqual(Stage(key="s", label="S", entered=10, dropped=99).survived, 0)

    def test_shave_of_top_is_measured_against_the_original_population(self):
        # The number that says which steps matter: a step removing 90% of a
        # population already down to a fraction of the top line is doing
        # almost nothing to the funnel overall.
        payload = Stage(key="s", label="S", entered=100, dropped=90).to_dict(
            top_line=100_000
        )
        self.assertAlmostEqual(payload["drop_rate"], 0.9)
        self.assertAlmostEqual(payload["shave_of_top"], 0.0009)


class BuildFunnelTests(unittest.TestCase):
    def setUp(self):
        self.funnel = build_funnel("t", 100_000, [
            ("tld", "TLD not tracked", 96_000, ""),
            ("machine", "Machine-generated", 900, ""),
            ("repeat", "Already seen", 2_000, ""),
            ("ghost", "Nothing served", 260, ""),
        ])

    def test_each_step_sees_only_the_previous_survivors(self):
        for previous, nxt in zip(self.funnel.stages, self.funnel.stages[1:]):
            with self.subTest(step=nxt.key):
                self.assertEqual(nxt.entered, previous.survived)

    def test_the_top_line_is_the_first_step_in(self):
        self.assertEqual(self.funnel.entered, 100_000)

    def test_the_chain_is_closed(self):
        dropped = sum(s.dropped for s in self.funnel.stages)
        self.assertEqual(self.funnel.survived, 100_000 - dropped)

    def test_a_step_cannot_drop_more_than_reached_it(self):
        # Guard tallies and queue counts come from different statements; an
        # over-count must clamp rather than render a rate above 100%.
        funnel = build_funnel("t", 10, [("a", "A", 999, "")])
        self.assertEqual(funnel.stages[0].dropped, 10)
        self.assertLessEqual(funnel.stages[0].drop_rate, 1.0)
        self.assertEqual(funnel.survived, 0)

    def test_an_arbitrary_number_of_steps_is_supported(self):
        # The screening script is a long ordered chain; the page renders
        # however many steps it is given rather than a hand-maintained set.
        many = build_funnel("t", 1_000, [
            (f"s{i}", f"Step {i}", 10, "") for i in range(25)
        ])
        self.assertEqual(len(many.stages), 25)
        self.assertEqual(many.survived, 750)

    def test_a_run_with_no_traffic_is_all_zero_not_negative(self):
        empty = build_funnel("t", 0, [("a", "A", 0, ""), ("b", "B", 0, "")])
        self.assertEqual(empty.survived, 0)
        for stage in empty.stages:
            with self.subTest(step=stage.key):
                self.assertEqual(stage.drop_rate, 0.0)


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
            Stage(key="a", label="A", entered=100, dropped=0),
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
