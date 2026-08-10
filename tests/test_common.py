import tempfile
import unittest
import warnings
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import common
from common import upsert_csv


class CsvPersistenceTests(unittest.TestCase):
    def test_upsert_blanks_missing_values_without_future_warning(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.csv"
            rows = pd.DataFrame({"domain": ["example.ai"], "score": [None]})

            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always", FutureWarning)
                saved = upsert_csv(path, rows)

            self.assertEqual(saved.loc[0, "score"], "")
            self.assertFalse(
                any(isinstance(item.message, FutureWarning) for item in captured)
            )


class InvestmentBasketTests(unittest.TestCase):
    def basket_paths(self, directory: str):
        return (
            Path(directory) / "yes_basket.csv",
            Path(directory) / "no_basket.csv",
        )

    def test_basket_stores_only_url_and_rating(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entry("https://Strong.ai/about", "yes", 88)
                saved = common.read_basket_data(yes_path)

            self.assertEqual(list(saved.columns), ["domain_url", "rating"])
            self.assertEqual(saved.loc[0, "domain_url"], "https://strong.ai")
            self.assertEqual(saved.loc[0, "rating"], 88)

    def test_cloud_basket_merges_labels_written_by_another_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            pd.DataFrame([{
                "domain_url": "https://local.ai", "rating": 70,
            }]).to_csv(yes_path, index=False)
            shared = [{
                "domain": "remote.ai",
                "label": "yes",
                "rating": 89,
                "source": "claude",
                "reason": "claude_keep",
                "labeled_at": "2026-08-10T00:00:00Z",
            }]
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
                patch.object(common, "shared_training_label_rows", return_value=shared),
            ):
                saved = common.read_basket_data(yes_path)

            self.assertEqual(
                set(saved.domain_url),
                {"https://local.ai", "https://remote.ai"},
            )

    def test_human_edit_records_separate_gold_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entry(
                    "reviewed.ai", "yes", 86,
                    source="human", reason="strong_technical_moat",
                )
                metadata = common.read_label_metadata()
            self.assertEqual(metadata.loc[0, "source"], "human")
            self.assertEqual(metadata.loc[0, "reason"], "strong_technical_moat")

    def test_yes_and_no_baskets_are_mutually_exclusive(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entry("flip.ai", "yes", 75)
                common.save_basket_entry("flip.ai", "no", 0)
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

            self.assertTrue(yes.empty)
            self.assertEqual(no.loc[0, "domain_url"], "https://flip.ai")
            self.assertEqual(no.loc[0, "rating"], 0)

    def test_automated_label_does_not_overwrite_human_decision(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entry(
                    "protected.ai", "yes", 92, source="human"
                )
                saved = common.save_automated_basket_entries(
                    [("protected.ai", 10), ("new-reject.ai", 20)],
                    "no",
                    source="claude",
                    reason="claude_reject",
                )
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

            self.assertEqual(saved, 1)
            self.assertEqual(yes.domain_url.tolist(), ["https://protected.ai"])
            self.assertEqual(no.domain_url.tolist(), ["https://new-reject.ai"])

    def test_automated_label_can_update_an_earlier_automated_label(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_automated_basket_entries(
                    [("changed.ai", 40)], "no", source="claude"
                )
                common.save_automated_basket_entries(
                    [("changed.ai", 88)], "yes", source="claude"
                )
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

            self.assertEqual(yes.domain_url.tolist(), ["https://changed.ai"])
            self.assertEqual(yes.rating.tolist(), [88])
            self.assertTrue(no.empty)

    def test_basket_rating_must_be_zero_to_one_hundred(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entry("zero.ai", "no", 0)
                with self.assertRaises(ValueError):
                    common.save_basket_entry("invalid.ai", "yes", 101)

    def test_hard_denied_seed_adds_no_without_overwriting_manual_data(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            pd.DataFrame([{
                "domain_url": "https://manual-yes.ai", "rating": 91,
            }]).to_csv(yes_path, index=False)
            pd.DataFrame([{
                "domain_url": "https://existing-no.ai", "rating": 17,
            }]).to_csv(no_path, index=False)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                added = common.seed_no_basket([
                    "manual-yes.ai",
                    "existing-no.ai",
                    "new-denial.ai",
                    "new-denial.ai",
                ])
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

            self.assertEqual(added, 1)
            self.assertEqual(yes.loc[0, "rating"], 91)
            self.assertEqual(
                dict(zip(no.domain_url, no.rating)),
                {
                    "https://existing-no.ai": 17,
                    "https://new-denial.ai": 0,
                },
            )

    def test_scored_basket_transfers_each_objective_rating(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                added = common.seed_scored_basket(
                    [("review.ai", 63), ("candidate.ai", 84)],
                    "yes",
                )
                saved = common.read_basket_data(yes_path)

            self.assertEqual(added, 2)
            self.assertEqual(
                dict(zip(saved.domain_url, saved.rating)),
                {"https://review.ai": 63, "https://candidate.ai": 84},
            )

    def test_authoritative_hard_denial_moves_yes_label_to_no(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entry("changed.ai", "yes", 72)
                added = common.seed_scored_basket(
                    [("changed.ai", 8)],
                    "no",
                    reason="visible_hard_denial",
                    move_existing=True,
                )
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

            self.assertEqual(added, 1)
            self.assertTrue(yes.empty)
            self.assertEqual(no.loc[0, "domain_url"], "https://changed.ai")
            self.assertEqual(no.loc[0, "rating"], 8)

    def test_bulk_hard_denial_moves_many_rows_in_one_basket_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            yes_path, no_path = self.basket_paths(directory)
            with (
                patch.object(common, "YES_BASKET_DATA", yes_path),
                patch.object(common, "NO_BASKET_DATA", no_path),
            ):
                common.save_basket_entries(
                    [("one.ai", 71), ("two.ai", 64)],
                    "yes",
                )
                saved = common.save_basket_entries(
                    [("one.ai", 12), ("two.ai", 9)],
                    "no",
                    reason="bulk_move_all_to_hard_denied",
                )
                yes = common.read_basket_data(yes_path)
                no = common.read_basket_data(no_path)

            self.assertEqual(saved, 2)
            self.assertTrue(yes.empty)
            self.assertEqual(
                dict(zip(no.domain_url, no.rating)),
                {"https://one.ai": 12, "https://two.ai": 9},
            )


if __name__ == "__main__":
    unittest.main()
