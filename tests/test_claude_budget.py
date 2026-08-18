"""Routing rules for the daily diligence budget.

Pure in, pure out — the cap, the tiering, and the savings arithmetic are
asserted without a client, a clock, or a database.
"""

import unittest

from claude_budget import BATCH_DISCOUNT, Plan, describe_savings, plan_calls


def rows(*scores):
    return [{"domain": f"d{i}.ai", "priority_score": s} for i, s in enumerate(scores)]


class DailyCapTests(unittest.TestCase):
    def test_never_dispatches_more_than_the_daily_budget(self):
        plan = plan_calls(rows(*([95] * 500)), calls_used_today=0, daily_budget=100)
        self.assertEqual(plan.total_calls, 100)
        self.assertEqual(len(plan.deferred), 400)

    def test_budget_already_spent_dispatches_nothing(self):
        plan = plan_calls(rows(99, 98, 97), calls_used_today=100, daily_budget=100)
        self.assertEqual(plan.total_calls, 0)
        self.assertEqual(len(plan.deferred), 3)
        self.assertIn("already spent", plan.note)

    def test_partial_budget_is_respected(self):
        plan = plan_calls(rows(*([50] * 40)), calls_used_today=95, daily_budget=100)
        self.assertEqual(plan.total_calls, 5)

    def test_overspend_recorded_upstream_does_not_go_negative(self):
        plan = plan_calls(rows(50), calls_used_today=999, daily_budget=100)
        self.assertEqual(plan.total_calls, 0)
        self.assertEqual(plan.budget_remaining, 0)


class TieringTests(unittest.TestCase):
    def test_only_exceptional_candidates_get_the_expensive_tier(self):
        # The synchronous tier costs double a batched one. It is reserved for
        # companies a human would want to see today, not filled to a quota.
        plan = plan_calls(
            rows(95, 92, 70, 60, 50),
            calls_used_today=0, sync_reserve=4, exceptional_score=90,
        )
        self.assertEqual([r["priority_score"] for r in plan.sync], [95, 92])
        self.assertEqual(len(plan.batched), 3)

    def test_unused_reserve_goes_to_the_batch_tier_not_to_waste(self):
        # If nothing clears the bar, the day's diligence must not shrink.
        plan = plan_calls(
            rows(*([50] * 100)),
            calls_used_today=0, daily_budget=100, sync_reserve=10,
            exceptional_score=90,
        )
        self.assertEqual(len(plan.sync), 0)
        self.assertEqual(len(plan.batched), 100)
        self.assertIn("half-price batch", plan.note)

    def test_budget_goes_to_the_best_candidates_regardless_of_input_order(self):
        plan = plan_calls(
            rows(10, 99, 20, 98, 30),
            calls_used_today=0, daily_budget=3, sync_reserve=1,
            exceptional_score=90,
        )
        self.assertEqual([r["priority_score"] for r in plan.sync], [99])
        self.assertEqual([r["priority_score"] for r in plan.batched], [98, 30])
        self.assertEqual([r["priority_score"] for r in plan.deferred], [20, 10])

    def test_reserve_cannot_exceed_the_remaining_budget(self):
        plan = plan_calls(
            rows(*([95] * 50)),
            calls_used_today=98, daily_budget=100, sync_reserve=10,
        )
        self.assertEqual(plan.total_calls, 2)
        self.assertEqual(len(plan.sync), 2)

    def test_a_zero_reserve_batches_everything(self):
        plan = plan_calls(rows(*([99] * 20)), calls_used_today=0, sync_reserve=0)
        self.assertEqual(len(plan.sync), 0)
        self.assertEqual(len(plan.batched), 20)

    def test_no_candidate_is_routed_to_two_tiers(self):
        plan = plan_calls(rows(*([95] * 30)), calls_used_today=0, daily_budget=20)
        ids = [id(r) for r in plan.sync + plan.batched + plan.deferred]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(ids), 30)

    def test_unscorable_rows_sort_last_rather_than_raising(self):
        plan = plan_calls(
            [{"domain": "bad.ai", "priority_score": "n/a"},
             {"domain": "good.ai", "priority_score": 95}],
            calls_used_today=0, daily_budget=1, sync_reserve=1,
        )
        self.assertEqual(plan.sync[0]["domain"], "good.ai")


class SavingsTests(unittest.TestCase):
    def test_cost_multiple_reflects_the_actual_split(self):
        plan = plan_calls(
            rows(*([95] * 10 + [50] * 90)),
            calls_used_today=0, daily_budget=100, sync_reserve=10,
        )
        expected = (10 + 90 * BATCH_DISCOUNT) / 100
        self.assertAlmostEqual(plan.cost_multiple, expected)
        self.assertAlmostEqual(plan.cost_multiple, 0.55)

    def test_all_batched_hits_the_discount_floor(self):
        plan = plan_calls(rows(*([50] * 10)), calls_used_today=0, sync_reserve=0)
        self.assertAlmostEqual(plan.cost_multiple, BATCH_DISCOUNT)

    def test_all_sync_shows_no_saving(self):
        plan = plan_calls(
            rows(*([99] * 5)), calls_used_today=0, daily_budget=5, sync_reserve=5,
        )
        self.assertAlmostEqual(plan.cost_multiple, 1.0)

    def test_empty_plan_does_not_divide_by_zero(self):
        self.assertEqual(Plan().cost_multiple, 0.0)
        self.assertIn("No diligence calls", describe_savings(Plan()))

    def test_savings_line_is_human_readable(self):
        plan = plan_calls(
            rows(*([95] * 10 + [50] * 90)),
            calls_used_today=0, daily_budget=100, sync_reserve=10,
        )
        line = describe_savings(plan)
        self.assertIn("45% saved", line)
        self.assertIn("10 immediate, 90 batched", line)


if __name__ == "__main__":
    unittest.main()
