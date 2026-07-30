import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import common
import training_truth
from classification_store import connect_store


class TrainingTruthTests(unittest.TestCase):
    def test_rebuild_keeps_humans_and_portfolio_but_drops_automatic_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            yes = root / "yes.csv"
            no = root / "no.csv"
            metadata = root / "training_label_metadata.csv"
            overrides = root / "overrides.csv"
            ground_truth = root / "ground_truth.csv"
            database = root / "classifications.sqlite3"

            pd.DataFrame([
                {"domain_url": "https://human-yes.ai", "rating": 81},
                {"domain_url": "https://portfolio.ai", "rating": 100},
                {"domain_url": "https://python-yes.ai", "rating": 70},
            ]).to_csv(yes, index=False)
            pd.DataFrame([
                {"domain_url": "https://human-no.ai", "rating": 12},
                {"domain_url": "https://python-no.ai", "rating": 0},
            ]).to_csv(no, index=False)
            pd.DataFrame([
                {"domain": "human-yes.ai", "label": "yes", "rating": 81,
                 "source": "human", "reason": "reviewed", "labeled_at": "1"},
                {"domain": "portfolio.ai", "label": "yes", "rating": 100,
                 "source": "glasswing_portfolio", "reason": "verified",
                 "labeled_at": "1"},
                {"domain": "python-yes.ai", "label": "yes", "rating": 70,
                 "source": "crawler_classification", "reason": "automatic",
                 "labeled_at": "1"},
                {"domain": "python-no.ai", "label": "no", "rating": 0,
                 "source": "weak_rule", "reason": "automatic",
                 "labeled_at": "1"},
            ]).to_csv(metadata, index=False)
            pd.DataFrame([
                {
                    "domain": "human-no.ai",
                    "override_target": "Hard Denied",
                    "override_note": "manual review",
                    "updated_at": "2",
                }
            ]).to_csv(overrides, index=False)
            with connect_store(database) as conn:
                conn.execute(
                    """
                    INSERT INTO manual_decisions(
                        domain,target_label,target_bucket,objective_score,
                        note,source,decided_at
                    ) VALUES (
                        'python-no.ai','Hard Denied','reject',0,
                        'automatic','automated_job','1'
                    )
                    """
                )
                conn.commit()

            with (
                patch.object(training_truth, "YES_BASKET_DATA", yes),
                patch.object(training_truth, "NO_BASKET_DATA", no),
                patch.object(training_truth, "OVERRIDES_CSV", overrides),
                patch.object(training_truth, "GROUND_TRUTH_CSV", ground_truth),
                patch.object(common, "YES_BASKET_DATA", yes),
                patch.object(common, "NO_BASKET_DATA", no),
            ):
                result = training_truth.rebuild_human_ground_truth(
                    classification_db=database
                )

            self.assertEqual(result["ground_truth"], 3)
            rebuilt_yes = pd.read_csv(yes)
            rebuilt_no = pd.read_csv(no)
            self.assertEqual(
                set(rebuilt_yes["domain_url"]),
                {"https://human-yes.ai", "https://portfolio.ai"},
            )
            self.assertEqual(
                rebuilt_no["domain_url"].tolist(),
                ["https://human-no.ai"],
            )
            with sqlite3.connect(database) as conn:
                decisions = conn.execute(
                    "SELECT domain,source FROM manual_decisions"
                ).fetchall()
            self.assertEqual(
                decisions,
                [
                    ("human-no.ai", "human"),
                    ("human-yes.ai", "human"),
                ],
            )


if __name__ == "__main__":
    unittest.main()
