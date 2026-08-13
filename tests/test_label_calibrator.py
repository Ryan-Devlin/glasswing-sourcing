import unittest

from label_calibrator import (
    PreferenceCalibrator,
    text_features,
    unusable_training_text,
)


class LabelCalibratorTests(unittest.TestCase):
    def test_nan_placeholder_training_rows_are_rejected(self):
        self.assertTrue(unusable_training_text(
            "Company: nan\nWebsite evidence: nan\nLocation: nan"
        ))
        self.assertFalse(unusable_training_text(
            "Company: Acme\nWebsite evidence: industrial robotics platform"
        ))

    def test_text_features_do_not_learn_nan(self):
        self.assertNotIn("nan", text_features("nan nan industrial robotics"))

    def test_model_uses_training_split_only(self):
        rows = [
            {
                "split": "train",
                "label": "yes",
                "trust_tier": "gold",
                "sample_weight": "1",
                "input_text": "industrial robotics autonomy platform",
            },
            {
                "split": "train",
                "label": "no",
                "trust_tier": "gold",
                "sample_weight": "1",
                "input_text": "marketing consulting agency services",
            },
            {
                "split": "test",
                "label": "no",
                "trust_tier": "gold",
                "sample_weight": "1",
                "input_text": "industrial robotics autonomy platform",
            },
        ]
        model = PreferenceCalibrator(rows)
        self.assertEqual(model.document_counts, {"yes": 1, "no": 1})
        self.assertGreater(
            model.predict("industrial robotics autonomy platform").probability_yes,
            model.predict("marketing consulting agency services").probability_yes,
        )

    def test_validation_selects_threshold_without_using_test_rows(self):
        def row(split, label, text):
            return {
                "split": split, "label": label, "trust_tier": "gold",
                "sample_weight": "1", "input_text": text,
            }
        rows = []
        for index in range(55):
            rows.append(row("train", "yes", f"robotics autonomy control {index}"))
            rows.append(row("train", "no", f"marketing agency services {index}"))
        rows.extend([
            row("validation", "yes", "robotics autonomy platform"),
            row("validation", "no", "marketing agency services"),
            # Contradictory test labels must not influence model readiness.
            row("test", "no", "robotics autonomy platform"),
        ])
        model = PreferenceCalibrator(rows)
        assert model.validation_metrics["auc"] == 1.0
        assert model.validation_metrics["positive_recall"] == 1.0
        assert model.training_ready


if __name__ == "__main__":
    unittest.main()
