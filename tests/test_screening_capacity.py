"""Closed-loop screening concurrency.

Replaces a hardcoded worker count. The gap between arrival and screening
capacity could not be measured -- the local queue snapshot was stale and there
are no production credentials -- so the controller is written to need no such
measurement: it reacts to signals the worker already publishes each batch.
"""

import unittest

from screening_capacity import CapacityDecision, next_worker_count


def step(current, **kw):
    kw.setdefault("failure_rate", 0.28)
    kw.setdefault("rss_mb", 250.0)
    kw.setdefault("pending", 5_000)
    return next_worker_count(current, **kw)


class RampTests(unittest.TestCase):
    def test_a_backlog_with_healthy_fetches_ramps_up(self):
        first = step(40)
        self.assertGreater(first.workers, 40)
        self.assertTrue(first.changed)

    def test_ramping_stops_at_the_ceiling(self):
        workers = 40
        for _ in range(30):
            workers = step(workers, ceiling=80).workers
        self.assertEqual(workers, 80)

    def test_a_small_pool_still_moves_when_the_percentage_step_rounds_flat(self):
        # int(1 * 1.25) is 1: a purely multiplicative step would pin a small
        # pool in place forever.
        self.assertGreater(step(1, floor=1).workers, 1)


class BackoffTests(unittest.TestCase):
    def test_rising_failures_back_the_pool_off(self):
        decision = step(60, failure_rate=0.5)
        self.assertLess(decision.workers, 60)
        self.assertIn("fetch failures", decision.reason)

    def test_memory_outranks_a_healthy_failure_rate(self):
        # A recycle discards the whole in-flight wave, so it costs more than
        # the extra concurrency ever bought.
        decision = step(60, failure_rate=0.05, rss_mb=950, pending=99_999)
        self.assertLess(decision.workers, 60)
        self.assertIn("memory", decision.reason)

    def test_backoff_stops_at_the_floor(self):
        workers = 80
        for _ in range(40):
            workers = step(workers, failure_rate=0.9, floor=10).workers
        self.assertEqual(workers, 10)

    def test_no_backlog_releases_capacity(self):
        decision = step(40, pending=10)
        self.assertLess(decision.workers, 40)
        self.assertIn("keeps up", decision.reason)


class StabilityTests(unittest.TestCase):
    def test_the_dead_band_holds_rather_than_oscillating(self):
        # A single trip point makes a controller step up, cross the line, step
        # down, and thrash concurrency forever. Between the two thresholds it
        # must do nothing at all.
        decision = step(40, failure_rate=0.37)
        self.assertEqual(decision.workers, 40)
        self.assertFalse(decision.changed)
        self.assertIn("dead band", decision.reason)

    def test_a_steady_healthy_system_settles_instead_of_drifting(self):
        workers = 40
        for _ in range(25):
            workers = step(workers, failure_rate=0.37).workers
        self.assertEqual(workers, 40)

    def test_missing_measurements_hold_rather_than_ramp(self):
        # The first batch after a deploy has published nothing. Ramping on
        # absent evidence is how a controller runs away before it has seen a
        # single result.
        for kwargs in (
            {"failure_rate": None, "rss_mb": 250.0},
            {"failure_rate": 0.1, "rss_mb": None},
            {"failure_rate": None, "rss_mb": None},
        ):
            with self.subTest(**kwargs):
                decision = next_worker_count(40, pending=9_999, **kwargs)
                self.assertEqual(decision.workers, 40)
                self.assertFalse(decision.changed)
                self.assertIn("no measurement", decision.reason)


class BoundsTests(unittest.TestCase):
    def test_the_result_is_always_inside_the_configured_bounds(self):
        for current in (-5, 0, 1, 40, 500, 10_000):
            for failure in (0.0, 0.35, 1.0):
                with self.subTest(current=current, failure=failure):
                    workers = step(
                        current, failure_rate=failure, floor=10, ceiling=80
                    ).workers
                    self.assertGreaterEqual(workers, 10)
                    self.assertLessEqual(workers, 80)

    def test_a_reason_is_always_recorded(self):
        # The decision has to be auditable from the status file; a pool that
        # silently changed size would be indistinguishable from a bug.
        for failure in (0.0, 0.37, 0.9):
            self.assertTrue(step(40, failure_rate=failure).reason)

    def test_the_decision_serialises_for_the_status_file(self):
        payload = step(40).to_dict()
        self.assertIn("screening_workers", payload)
        self.assertIn("screening_capacity_reason", payload)


if __name__ == "__main__":
    unittest.main()
