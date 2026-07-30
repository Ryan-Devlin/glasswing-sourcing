import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import claude_classifier


class ClaudeThresholdTests(unittest.TestCase):
    def test_payload_is_lean_but_keeps_core_evidence(self):
        payload = claude_classifier.company_payload({
            "domain": "example.ai",
            "homepage_excerpt": "x" * 5000,
            "meta_description": "y" * 1000,
            "rule_evidence": "z" * 1000,
        })
        self.assertLess(len(payload), 1900)
        self.assertIn("example.ai", payload)

    def prepare(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "all_screened.csv"
            pd.DataFrame(rows).to_csv(path, index=False)
            with (
                patch.object(
                    claude_classifier,
                    "load_currently_diligenced_domains",
                    return_value=set(),
                ),
                patch.object(claude_classifier, "FILTERED", Path(directory)),
            ):
                selected, stats = claude_classifier.prepare_input(
                    path,
                    reclassify=False,
                    limit=0,
                    minimum_score=51,
                    percentage_cap=1.0,
                    max_calls=10,
                    exceptional_score=80,
                )
        return selected, stats

    def test_every_uncapped_company_above_fifty_is_eligible(self):
        selected, stats = self.prepare([
            {
                "domain": "fifty.ai",
                "diligence_rank_score": 50,
                "rule_score": 50,
                "qualified_for_llm": "false",
                "location_status": "UNKNOWN",
            },
            {
                "domain": "fifty-one.ai",
                "diligence_rank_score": 51,
                "rule_score": 51,
                "qualified_for_llm": "false",
                "location_status": "UNKNOWN",
            },
            {
                "domain": "ninety.ai",
                "diligence_rank_score": 90,
                "rule_score": 90,
                "qualified_for_llm": "true",
                "location_status": "US",
            },
        ])
        self.assertEqual(set(selected.domain), {"fifty-one.ai", "ninety.ai"})
        self.assertEqual(stats["eligible_above_score"], 2)

    def test_legacy_uncapped_disqualifier_does_not_consume_claude_call(self):
        selected, _ = self.prepare([
            {
                "domain": "chinese.ai",
                "diligence_rank_score": 79,
                "rule_score": 79,
                "location_status": "UNKNOWN",
                "predominantly_non_english": "true",
            },
            {
                "domain": "eligible.ai",
                "diligence_rank_score": 62,
                "rule_score": 62,
                "location_status": "UNKNOWN",
                "predominantly_non_english": "false",
            },
        ])
        self.assertEqual(selected.domain.tolist(), ["eligible.ai"])


if __name__ == "__main__":
    unittest.main()
