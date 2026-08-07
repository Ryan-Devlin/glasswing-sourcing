import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import filter as screening_filter
import common
from glasswing_portfolio import (
    GLASSWING_PORTFOLIO_DOMAINS,
    GLASSWING_PORTFOLIO_EXCLUSIONS,
)


class BasketRoutingTests(unittest.TestCase):
    def apply_basket(self, basket: str, initial_bucket: str) -> pd.Series:
        with tempfile.TemporaryDirectory() as directory:
            yes_path = Path(directory) / "yes.csv"
            no_path = Path(directory) / "no.csv"
            target = yes_path if basket == "yes" else no_path
            pd.DataFrame([{
                "domain_url": "https://labeled.ai",
                "rating": 0 if basket == "no" else 90,
            }]).to_csv(target, index=False)
            with (
                patch.object(screening_filter, "YES_BASKET_DATA", yes_path),
                patch.object(screening_filter, "NO_BASKET_DATA", no_path),
            ):
                result = screening_filter.apply_persistent_overrides(
                    pd.DataFrame([{
                        "domain": "labeled.ai",
                        "routing_bucket": initial_bucket,
                    }])
                )
        return result.iloc[0]

    def test_unverified_yes_basket_does_not_overwrite_objective_routing(self):
        row = self.apply_basket("yes", "reject")
        self.assertEqual(row["routing_bucket"], "reject")
        self.assertEqual(row["training_basket"], "yes")
        self.assertEqual(row["human_rating"], 90)

    def test_human_yes_stays_in_original_classification(self):
        row = self.apply_basket("yes", "review")
        self.assertEqual(row["routing_bucket"], "review")
        self.assertEqual(row["training_basket"], "yes")
        self.assertEqual(row["human_rating"], 90)

    def test_human_no_stays_in_original_classification(self):
        row = self.apply_basket("no", "priority")
        self.assertEqual(row["routing_bucket"], "priority")
        self.assertEqual(row["training_basket"], "no")
        self.assertEqual(row["human_rating"], 0)

    def test_unverified_no_basket_does_not_create_a_rejection(self):
        row = self.apply_basket("no", "priority")
        self.assertEqual(row["routing_bucket"], "priority")
        self.assertEqual(row["training_basket"], "no")

    def test_portfolio_seed_adds_missing_and_preserves_manual_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path = Path(directory) / "yes.csv"
            no_path = Path(directory) / "no.csv"
            pd.DataFrame([{
                "domain_url": "https://existing.ai", "rating": 73,
            }]).to_csv(yes_path, index=False)
            pd.DataFrame([{
                "domain_url": "https://manual-no.ai", "rating": 8,
            }]).to_csv(no_path, index=False)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                added = common.seed_yes_basket(
                    ["existing.ai", "manual-no.ai", "new-positive.ai"],
                    rating=100,
                )
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

        self.assertEqual(added, 1)
        self.assertEqual(dict(zip(yes.domain_url, yes.rating)), {
            "https://existing.ai": 73,
            "https://new-positive.ai": 100,
        })
        self.assertEqual(dict(zip(no.domain_url, no.rating)), {
            "https://manual-no.ai": 8,
        })

    def test_official_portfolio_seed_is_reviewed_and_unique(self):
        self.assertEqual(len(GLASSWING_PORTFOLIO_DOMAINS), 62)
        self.assertEqual(len(set(GLASSWING_PORTFOLIO_DOMAINS)), 62)
        self.assertEqual(len(GLASSWING_PORTFOLIO_EXCLUSIONS), 0)
        self.assertTrue({
            "atlantic-quantum.com",
            "csm.ai",
            "nthparty.com",
            "papercrane.io",
            "plannuh.com",
            "terbiumlabs.com",
            "zylotech.com",
        }.issubset(GLASSWING_PORTFOLIO_DOMAINS))


if __name__ == "__main__":
    unittest.main()
