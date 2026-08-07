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

    def test_live_rescore_rescues_a_domain_with_a_stale_low_score(self):
        # Confirmed live: a real company (page_title "Abrient -- Sovereign
        # AI for Life & Annuity") visibly scored 84 on the dashboard's own
        # live rescore of the same stored text, but was "Not reviewed" --
        # diligence_rank_score (a different, frozen formula from whenever
        # it was first screened) was below minimum_score. The live rescore
        # must rescue it: never miss Claude review just because scoring
        # calibration has since improved.
        selected, _ = self.prepare([
            {
                "domain": "abrient.ai",
                "diligence_rank_score": 40,
                "rule_score": 40,
                "location_status": "US",
                "page_title": "Abrient — Sovereign AI for Life & Annuity",
                "meta_description": (
                    "Abrient is agent infrastructure for life and "
                    "annuity carriers. Our inference infrastructure and "
                    "model serving platform gives insurance operations "
                    "teams AI observability into underwriting automation "
                    "workflows."
                ),
                "homepage_excerpt": (
                    "Customers deploy our production API to reduce "
                    "manual processing time and integrate with core "
                    "policy systems."
                ),
            },
            {
                "domain": "genuinely-low.ai",
                "diligence_rank_score": 40,
                "rule_score": 40,
                "location_status": "US",
                "page_title": "genuinely-low.ai",
                "meta_description": "",
                "homepage_excerpt": "",
            },
        ])
        self.assertIn("abrient.ai", selected.domain.tolist())
        self.assertNotIn("genuinely-low.ai", selected.domain.tolist())

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
