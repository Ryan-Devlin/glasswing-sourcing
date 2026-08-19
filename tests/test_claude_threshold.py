import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import claude_classifier
from diligence_logic import (
    apply_hard_guards,
    dimension_score,
    parse_assessment,
    parse_green_flags,
    normalize_green_flags,
)


def test_green_flags_require_allowed_type_and_evidence():
    flags = parse_green_flags(
        "ex_big_tech::Ex-Meta founders::Two founders previously led Meta infrastructure teams | "
        "made_up::Famous::Unsupported | repeat_founder::Repeat founder::"
    )
    assert flags == [{
        "type": "ex_big_tech",
        "label": "Ex-Meta",
        "evidence": "Two founders previously led Meta infrastructure teams",
    }]


def test_green_flags_normalize_legacy_types_labels_and_evidence():
    flags = normalize_green_flags([
        {
            "type": "big_tech", "label": "Ex-Google founders",
            "evidence": "Both founders previously built infrastructure products at Google.",
            "source": "https://example.ai/team",
        },
        {
            "type": "ex_big_tech", "label": "Ex-Google team",
            "evidence": "Duplicate signal with enough evidence words to otherwise pass.",
        },
        {"type": "patents", "label": "Patents", "evidence": "unverified"},
        {"type": "made_up", "label": "Famous", "evidence": "A long but invalid claim that must be removed."},
    ])
    assert flags == [{
        "type": "ex_big_tech",
        "label": "Ex-Google",
        "evidence": "Both founders previously built infrastructure products at Google.",
        "source": "https://example.ai/team",
    }]


class ClaudeThresholdTests(unittest.TestCase):
    def test_unverified_facts_do_not_turn_a_keep_into_reject(self):
        assessment = {
            "decision": "KEEP",
            "thesis_fit": 5, "enterprise_value": 4, "product_depth": 4,
            "technical_moat": 4, "stage_fit": 3,
            "us_based": "unclear",
            "funding_millions_usd": -1,
            "latest_round": "unknown",
            "institutional_rounds": -1,
            "ownership_status": "unclear",
            "company_maturity": "unknown",
            "category": "unclear",
            "product_summary": "",
            "diligence_sources": "",
            "concerns": "",
        }
        result = apply_hard_guards(assessment)
        self.assertEqual(result["decision"], "KEEP")
        self.assertEqual(result["concerns"], "Location not verified")

    def test_confirmed_non_us_still_forces_reject(self):
        assessment = {
            "decision": "KEEP", "fit_score": 90, "us_based": "no",
            "thesis_fit": 5, "enterprise_value": 5, "product_depth": 5,
            "technical_moat": 5, "stage_fit": 5,
            "funding_millions_usd": -1, "latest_round": "unknown",
            "institutional_rounds": -1, "ownership_status": "unclear",
            "company_maturity": "unknown", "category": "vertical_ai",
            "product_summary": "Product", "diligence_sources": "", "concerns": "",
        }
        self.assertEqual(apply_hard_guards(assessment)["decision"], "REJECT")

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
        <decision>REJECT</decision>
        <thesis_fit>1</thesis_fit><enterprise_value>1</enterprise_value>
        <product_depth>1</product_depth><technical_moat>0</technical_moat>
        <stage_fit>2</stage_fit><evidence_confidence>4</evidence_confidence>
        <summary>Consumer photo editor</summary><reason>Not enterprise software</reason>
        <portfolio_overlap>no</portfolio_overlap><portfolio_company>none</portfolio_company>
        <portfolio_overlap_evidence>none</portfolio_overlap_evidence>
        <positive_evidence>Working consumer product</positive_evidence>
        <negative_evidence>No enterprise buyer</negative_evidence><unknowns>none</unknowns>
        """
        claude_classifier.validate_response_text(response)

    def test_confirmed_portfolio_technology_overlap_forces_reject(self):
        assessment = {
            "decision": "KEEP", "us_based": "yes",
            "thesis_fit": 5, "enterprise_value": 5, "product_depth": 5,
            "technical_moat": 5, "stage_fit": 5,
            "funding_millions_usd": 1, "latest_round": "seed",
            "institutional_rounds": 1, "ownership_status": "independent",
            "company_maturity": "early_stage", "concerns": "",
            "reasoning": "Strong candidate", "portfolio_overlap": "yes",
            "portfolio_company": "neyon.ai",
            "portfolio_overlap_evidence": "Both provide the same core autonomous security remediation product.",
        }
        result = apply_hard_guards(assessment)
        self.assertEqual(result["decision"], "REJECT")
        self.assertLessEqual(result["fit_score"], 35)
        self.assertIn("neyon.ai", result["reasoning"])
        self.assertIn("Direct technology overlap", result["hard_disqualifiers"][-1])

    def test_unverified_portfolio_overlap_does_not_force_reject(self):
        assessment = {
            "decision": "KEEP", "us_based": "yes",
            "thesis_fit": 5, "enterprise_value": 5, "product_depth": 5,
            "technical_moat": 5, "stage_fit": 5,
            "funding_millions_usd": 1, "latest_round": "seed",
            "institutional_rounds": 1, "ownership_status": "independent",
            "company_maturity": "early_stage", "concerns": "",
            "reasoning": "Strong candidate", "portfolio_overlap": "unclear",
            "portfolio_company": "none", "portfolio_overlap_evidence": "none",
        }
        self.assertEqual(apply_hard_guards(assessment)["decision"], "KEEP")

    def test_rubric_response_has_sufficient_token_headroom(self):
        # The evidence-first rubric requires fifteen tags; too small a ceiling
        # truncates the answer before the closing evidence tags and the whole
        # call is discarded as malformed. Asserted on the value the request
        # actually carries rather than on source text, so it keeps holding
        # when the request body moves.
        params = claude_classifier.request_params({"domain": "a.ai"}, "m")
        self.assertGreaterEqual(params["max_tokens"], 1000)
        self.assertEqual(params["system"], claude_classifier.SYSTEM_PROMPT)
        source = Path(claude_classifier.__file__).read_text(encoding="utf-8")
        self.assertIn('"failure_details"', source)

    def test_both_tiers_send_an_identical_request_body(self):
        # The synchronous and batched tiers must differ only in transport. If
        # they built different bodies, a batched verdict would be answering a
        # subtly different question than an immediate one for the same row.
        source = Path(claude_classifier.__file__).read_text(encoding="utf-8")
        classify_body = source.split("async def classify(", 1)[1].split(
            "\ndef load_currently_diligenced_domains", 1
        )[0]
        self.assertIn("request_params(row, model)", classify_body)
        run_body = source.split("async def run(", 1)[1]
        self.assertIn("request_params(row, options.model)", run_body)

    def test_weighted_score_is_deterministic_and_confidence_is_separate(self):
        assessment = {
            "thesis_fit": 5, "enterprise_value": 4, "product_depth": 4,
            "technical_moat": 3, "stage_fit": 4, "evidence_confidence": 1,
        }
        self.assertEqual(dimension_score(assessment), 81)
        assessment["evidence_confidence"] = 5
        self.assertEqual(dimension_score(assessment), 81)

    def test_parser_preserves_evidence_and_computes_decision(self):
        response = """
        <decision>REJECT</decision><thesis_fit>5</thesis_fit>
        <enterprise_value>4</enterprise_value><product_depth>4</product_depth>
        <technical_moat>4</technical_moat><stage_fit>3</stage_fit>
        <evidence_confidence>2</evidence_confidence><category>vertical_ai</category>
        <us_based>unclear</us_based><funding_millions_usd>-1</funding_millions_usd>
        <latest_round>unknown</latest_round><institutional_rounds>-1</institutional_rounds>
        <company_maturity>unknown</company_maturity><ownership_status>unclear</ownership_status>
        <ownership_evidence>none</ownership_evidence><summary>Enterprise AI workflow</summary>
        <reason>Clear buyer and deep workflow</reason><concern>Funding unknown</concern>
        <funding_evidence>none</funding_evidence><location_evidence>none</location_evidence>
        <positive_evidence>Product and buyer are explicit</positive_evidence>
        <negative_evidence>none</negative_evidence><unknowns>Location and funding</unknowns>
        """
        result = parse_assessment(response, ["https://example.ai"])
        self.assertEqual(result["decision"], "KEEP")
        self.assertEqual(result["model_recommendation"], "REJECT")
        self.assertEqual(result["fit_score"], 82)
        self.assertEqual(result["unknowns"], "Location and funding")

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

    def test_payload_carries_the_stored_page_text_it_already_paid_for(self):
        # This used to cap the excerpt at 800 characters and buy a web search
        # to make up the difference -- backwards on both counts. The crawler
        # stores 12,000 characters per candidate, and passing them costs a
        # fraction of the $0.01/call search they replace.
        payload = claude_classifier.company_payload({
            "domain": "example.ai",
            "homepage_excerpt": "x" * 20000,
            "meta_description": "y" * 1000,
            "rule_evidence": "z" * 1000,
        })
        self.assertGreater(payload.count("x"), 10000)
        self.assertIn("example.ai", payload)

    def test_payload_stays_bounded(self):
        # Evidence-rich is not unbounded: a pathological excerpt must still be
        # truncated, or one row could blow the context window and the budget.
        payload = claude_classifier.company_payload({
            "domain": "example.ai",
            "homepage_excerpt": "x" * 500000,
            "meta_description": "y" * 50000,
            "rule_evidence": "z" * 50000,
        })
        self.assertLess(len(payload), 14000)

    def test_payload_does_not_ask_for_a_search_it_cannot_run(self):
        # The instruction and the tool were removed together. Leaving the ask
        # in would tell the model to search with no tool to search with, which
        # invites invented citations instead of an honest unknown.
        payload = claude_classifier.company_payload({"domain": "example.ai"})
        instruction = payload.split("{", 1)[0].lower()
        self.assertNotIn("search", instruction)

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

    def test_zone_universe_cache_content_is_excluded_automatically(self):
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

    def test_manual_send_overrides_zone_source_and_score_gates(self):
        selected, stats = self.prepare([{
            "domain": "praxl.app",
            "diligence_rank_score": 0,
            "rule_score": 0,
            "location_status": "UNKNOWN",
            "content_source": "zone_universe_cache",
            "qualified_for_llm": "true",
        }])
        self.assertEqual(selected.domain.tolist(), ["praxl.app"])
        self.assertEqual(stats["eligible_above_score"], 1)


if __name__ == "__main__":
    unittest.main()


def _ranked(counts):
    """Score-ordered candidate frame: {pillar: n} with cyber scoring highest."""
    rows = []
    score = 95
    for pillar, n in counts.items():
        for _ in range(n):
            rows.append({
                "domain": f"{pillar}{score}.ai",
                "_queue_score": score,
                "rule_score": score,
                "rule_thesis_pillar": pillar,
            })
            score -= 1
    frame = pd.DataFrame(rows).sort_values("_queue_score", ascending=False)
    return frame.reset_index(drop=True)


def test_one_pillar_cannot_monopolise_the_paid_queue():
    """Cyber took 40.8% of live Claude KEEPs while vertical AI -- with the
    largest candidate pool of any pillar -- took 3.8% and quantum 0.8%. Cyber
    copy is keyword-dense, so it accumulates rule score and crowds the others
    out of a global top-N selection rather than being more investable."""
    frame = _ranked({
        "intelligent_enterprise_defense": 20,
        "physical_ai": 5,
        "vertical_ai": 8,
        "next_gen_compute": 3,
    })
    assert frame.head(10)["rule_thesis_pillar"].nunique() == 1, "precondition"

    selected = claude_classifier._spread_across_pillars(frame, 10)
    assert len(selected) == 10
    assert selected["rule_thesis_pillar"].nunique() >= 4
    # No pillar may take more than half of a batch here.
    assert selected["rule_thesis_pillar"].value_counts().max() <= 5


def test_strongest_candidate_overall_is_still_reviewed_first():
    frame = _ranked({"intelligent_enterprise_defense": 5, "vertical_ai": 5})
    selected = claude_classifier._spread_across_pillars(frame, 4)
    assert selected.iloc[0]["_queue_score"] == frame["_queue_score"].max()


def test_quota_is_respected_exactly_and_never_exceeded():
    frame = _ranked({"a": 6, "b": 6, "c": 6})
    for quota in (1, 2, 5, 9, 18):
        assert len(claude_classifier._spread_across_pillars(frame, quota)) == quota


def test_quota_larger_than_the_pool_returns_everything_once():
    frame = _ranked({"a": 2, "b": 3})
    selected = claude_classifier._spread_across_pillars(frame, 50)
    assert len(selected) == 5
    assert selected["domain"].is_unique


def test_zero_or_empty_is_safe():
    frame = _ranked({"a": 3})
    assert claude_classifier._spread_across_pillars(frame, 0).empty
    assert claude_classifier._spread_across_pillars(pd.DataFrame(), 5).empty


def test_single_pillar_keeps_pure_score_order():
    frame = _ranked({"intelligent_enterprise_defense": 6})
    selected = claude_classifier._spread_across_pillars(frame, 4)
    assert list(selected["domain"]) == list(frame.head(4)["domain"])


def test_missing_pillar_column_falls_back_to_score_order():
    frame = _ranked({"a": 4, "b": 4}).drop(columns=["rule_thesis_pillar"])
    selected = claude_classifier._spread_across_pillars(frame, 3)
    assert list(selected["domain"]) == list(frame.head(3)["domain"])


def test_diversification_can_be_switched_off():
    frame = _ranked({"intelligent_enterprise_defense": 20, "vertical_ai": 8})
    with patch.object(claude_classifier, "DIVERSIFY_CLAUDE_PILLARS", False):
        selected = claude_classifier._spread_across_pillars(frame, 10)
    assert selected["rule_thesis_pillar"].nunique() == 1


class NoWebSearchTests(unittest.TestCase):
    """The diligence call must not carry a web-search tool.

    Measured: the search cost $0.01/call -- more than the Haiku tokens for the
    entire call -- to rediscover a homepage already stored on the row. These
    assertions keep it from creeping back in with the tool re-added but the
    evidence budget left small, which would be the worst of both.
    """

    def setUp(self):
        self.source = Path("claude_classifier.py").read_text(encoding="utf-8")
        self.classify = self.source.split("async def classify(", 1)[1].split(
            "\ndef load_currently_diligenced_domains", 1
        )[0]

    def test_the_diligence_call_declares_no_tools(self):
        self.assertNotIn("web_search", self.classify)
        self.assertNotIn("tools=", self.classify)

    def test_the_system_prompt_says_there_is_no_search(self):
        # Without this the model is told to use a search it does not have.
        # Whitespace-normalised: the prompt hard-wraps, so "no web search"
        # spans a newline in the source.
        prompt = " ".join(claude_classifier.SYSTEM_PROMPT.lower().split())
        self.assertIn("no web search", prompt)
        self.assertNotIn("one exact company+domain web search", prompt)

    def test_absence_of_coverage_is_not_stamped_on_every_assessment(self):
        # With no search, no call can produce a URL, so an unconditional
        # concern would assert a search result nobody looked for -- and land
        # on 100% of rows, which is not a signal. Assert on the assignment
        # rather than the phrase: the phrase also appears in the comment
        # explaining why the assignment was removed.
        self.assertNotIn('assessment["concerns"]', self.classify)

    def test_the_evidence_budget_is_large_and_tunable(self):
        self.assertGreaterEqual(claude_classifier.HOMEPAGE_EXCERPT_CHARS, 12000)
        self.assertIn("CLAUDE_HOMEPAGE_EXCERPT_CHARS", self.source)


class DailyCapTests(unittest.TestCase):
    """The daily limit is a ceiling, not a default.

    It was sitting at 151 in the live dashboard while the code claimed a
    100/day budget: the worker took min(5000, config), so the constant only
    applied when the operator had never touched the field.
    """

    def setUp(self):
        self.source = Path("continuous_pipeline.py").read_text(encoding="utf-8")

    def test_config_cannot_raise_the_cap(self):
        self.assertIn("min(\n            CLAUDE_DAILY_LIMIT,", self.source)
        self.assertNotIn("min(5000, int(config.get", self.source)

    def test_the_cap_is_one_hundred(self):
        import continuous_pipeline

        self.assertEqual(continuous_pipeline.CLAUDE_DAILY_LIMIT, 100)

    def test_the_api_offers_no_value_above_the_cap(self):
        # A field the worker would clamp is a field that lies to the operator.
        api = Path("web_api.py").read_text(encoding="utf-8")
        self.assertIn("claude_daily_limit: int = Field(100, ge=1, le=100)", api)
