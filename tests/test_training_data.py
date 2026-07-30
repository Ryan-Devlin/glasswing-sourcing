import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import training_data


class TrainingDataTests(unittest.TestCase):
    def test_compact_export_excludes_missing_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            filtered = root / "filtered"
            manual = root / "manual"
            llm = root / "llm"
            filtered.mkdir()
            manual.mkdir()
            llm.mkdir()
            pd.DataFrame([
                {"domain": "good.ai", "observed_at": "2", "page_title": "Enterprise security", "homepage_excerpt": "Protect company infrastructure", "fetch_error": ""},
                {"domain": "missing.ai", "observed_at": "2", "page_title": "", "homepage_excerpt": "", "fetch_error": "TimeoutError: "},
            ]).to_csv(filtered / "all_screened.csv", index=False)
            pd.DataFrame([
                {"domain_url": "https://good.ai", "rating": 82},
                {"domain_url": "https://missing.ai", "rating": 0},
            ]).to_csv(manual / "yes_basket.csv", index=False)
            pd.DataFrame(columns=["domain_url", "rating"]).to_csv(manual / "no_basket.csv", index=False)

            with (
                patch.object(training_data, "FILTERED", filtered),
                patch.object(training_data, "LLM", llm),
                patch.object(training_data, "YES_BASKET_DATA", manual / "yes_basket.csv"),
                patch.object(training_data, "NO_BASKET_DATA", manual / "no_basket.csv"),
            ):
                ready, excluded, manifest = training_data.build_training_dataset()

            self.assertEqual(ready["domain"].tolist(), ["good.ai"])
            self.assertEqual(excluded["domain"].tolist(), ["missing.ai"])
            self.assertLessEqual(len(ready.iloc[0]["input_text"]), 1800)
            self.assertEqual(manifest["model_ready"], 1)

    def test_domain_split_is_deterministic(self):
        self.assertEqual(training_data._split("same.ai"), training_data._split("same.ai"))

    def test_instruction_export_caps_negatives_at_two_per_positive(self):
        frame = pd.DataFrame([
            {"domain": f"yes-{i}.ai", "split": "train", "label": "yes"}
            for i in range(2)
        ] + [
            {"domain": f"no-{i}.ai", "split": "train", "label": "no"}
            for i in range(20)
        ])
        balanced = training_data.balanced_instruction_frame(frame)
        self.assertEqual(int((balanced["label"] == "yes").sum()), 2)
        self.assertEqual(int((balanced["label"] == "no").sum()), 4)


if __name__ == "__main__":
    unittest.main()
