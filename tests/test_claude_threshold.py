import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import claude_classifier


class ClaudeThresholdTests(unittest.TestCase):
    def test_empty_response_is_retryable_failure_not_reject(self):
        with self.assertRaises(claude_classifier.DiligenceResponseError):
            claude_classifier.validate_response_text("")

    def test_maybe_response_is_retryable_failure_not_bucket_output(self):
        response = """
        <decision>MAYBE</decision><score>50</score>
        <summary>Unclear company</summary><reason>Needs more evidence</reason>
        """
        with self.assertRaises(claude_classifier.DiligenceResponseError):
            claude_classifier.validate_response_text(response)

    def test_complete_binary_response_is_valid(self):
        response = """
        <decision>REJECT</decision><score>20</score>
        <summary>Consumer photo editor</summary><reason>Not enterprise software</reason>
        """
        claude_classifier.validate_response_text(response)

    def test_decisive_results_append_to_corresponding_baskets(self):
        approved = pd.DataFrame([
            {"domain": "investable.ai", "llm_fit_score": 91},
        ])
        denied = pd.DataFrame([
            {"domain": "hard-denied.ai", "llm_fit_score": 12},
        ])

        with patch.object(
            claude_classifier, "save_automated_basket_entries"
        ) as save:
            save.side_effect = [1, 1]
            counts = claude_classifier.append_claude_buckets(approved, denied)

        self.assertEqual(counts, (1, 1))
        self.assertEqual(save.call_args_list[0].args, (
            [("investable.ai", 91)], "yes",
        ))
        self.assertEqual(save.call_args_list[1].args, (
            [("hard-denied.ai", 12)], "no",
        ))
        self.assertEqual(save.call_args_list[0].kwargs["source"], "claude")
        self.assertEqual(save.call_args_list[1].kwargs["source"], "claude")

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

    def test_zone_universe_cache_content_never_reaches_claude(self):
        # ~170M .com/.app candidate domains from the zone crawl -- user
        # direction was explicit: that dataset never needs Claude, Python
        # scoring alone is enough given the volume. content_source is set
        # to "zone_universe_cache" only when a domain's page content was
        # actually served from zone_universe's own crawl cache.
        selected, stats = self.prepare([
            {
                "domain": "huge-zone-crawl.com",
                "diligence_rank_score": 95,
                "rule_score": 95,
                "location_status": "US",
                "content_source": "zone_universe_cache",
            },
            {
                "domain": "certstream-find.ai",
                "diligence_rank_score": 80,
                "rule_score": 80,
                "location_status": "US",
                "content_source": "network",
            },
        ])
        self.assertEqual(selected.domain.tolist(), ["certstream-find.ai"])
        self.assertEqual(stats["eligible_above_score"], 1)


if __name__ == "__main__":
    unittest.main()
