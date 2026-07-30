import json
import unittest
from pathlib import Path

from filter import (
    agency_assessment,
    explicit_non_us_identity_assessment,
    high_conviction_fit_assessment,
    hosting_provider_assessment,
    investment_profile_label,
    media_newsletter_assessment,
    placeholder_assessment,
    route_company,
    score_text,
    venture_quality_assessment,
)


class RuleCalibrationTests(unittest.TestCase):
    def test_short_keywords_do_not_match_inside_unrelated_words(self):
        _, _, _, counts = score_text(
            "Rapid capital formation capabilities for a neighborhood bakery."
        )
        self.assertEqual(counts["product"], 0)
        self.assertEqual(counts["business_value"], 0)

    def test_consulting_identity_overrides_product_marketing_words(self):
        rejected, evidence = agency_assessment(
            "Northwave is an AI consulting firm. We provide custom software "
            "and platform implementation services for clients."
        )
        self.assertTrue(rejected)
        self.assertIn("consulting firm", evidence)

    def test_engineering_services_company_is_not_a_venture_product(self):
        rejected, evidence = agency_assessment(
            "Our Services help semiconductor companies find specialized "
            "engineers. We offer verification services and help clients scale "
            "their design teams by providing the necessary resources from "
            "our pool of highly skilled engineers."
        )
        self.assertTrue(rejected)
        self.assertIn("engineering-services provider", evidence)

    def test_course_and_event_identity_are_non_product_content(self):
        course, _ = media_newsletter_assessment(
            "Online AI program for students. Explore the curriculum, "
            "earn certification, and enroll today."
        )
        event, _ = media_newsletter_assessment(
            "International AI Conference. View speakers and agenda, "
            "reserve your seat, and buy tickets."
        )
        self.assertTrue(course)
        self.assertTrue(event)

    def test_directory_identity_is_rejected_without_penalizing_product_docs(self):
        directory, _ = media_newsletter_assessment(
            "AI Tools Directory for discovering, reviewing, and comparing "
            "providers with transparent rankings and reviews."
        )
        product, _ = media_newsletter_assessment(
            "Developer platform with a model evaluation API, documentation, "
            "deployment workflows, and enterprise integrations."
        )
        self.assertTrue(directory)
        self.assertFalse(product)

    def test_positive_fit_requires_a_phrase_bundle(self):
        matched, evidence = high_conviction_fit_assessment(
            "Evaluation infrastructure for application-layer robotics with "
            "continuous evals and reproducible evaluation."
        )
        generic, _ = high_conviction_fit_assessment(
            "AI-powered enterprise platform with an API and dashboard."
        )
        self.assertTrue(matched)
        self.assertGreaterEqual(len(evidence), 2)
        self.assertFalse(generic)

    def test_domain_sales_pitch_is_hard_rejection_evidence(self):
        text = (
            "3DScanner.AI & 3DScans.AI Domain Pair For Sale. "
            "Concept Interface Preview. This is not a screenshot of existing "
            "software. Interested in this Domain? Contact Brokerage Team. "
            "We provide secure escrow and transfer services."
        )
        detected, reason, evidence = placeholder_assessment(
            text,
            word_count=len(text.split()),
        )
        self.assertTrue(detected)
        self.assertEqual(reason, "parked_or_for_sale")
        self.assertIn("domain_for_sale", evidence)

    def test_domain_specific_software_is_not_a_sales_page(self):
        text = (
            "Domain-specific enterprise AI software for manufacturing teams. "
            "Our platform deploys computer vision models into production."
        )
        detected, reason, _ = placeholder_assessment(
            text,
            word_count=len(text.split()),
        )
        self.assertFalse(detected)
        self.assertEqual(reason, "")

    def test_web_hosting_company_is_outside_venture_pipeline(self):
        detected, evidence = hosting_provider_assessment(
            "NameHero is a web hosting company offering web hosting, "
            "WordPress hosting, reseller hosting, and domain management."
        )
        self.assertTrue(detected)
        self.assertIn("web hosting", evidence)

    def test_normal_part_of_and_time_series_are_not_funding_events(self):
        label, _, _ = investment_profile_label(
            "Safety is a fundamental part of developing autonomous systems. "
            "The platform analyzes time series data.",
            maturity_risk=0,
        )
        self.assertEqual(label, "Investable Candidate")

    def test_explicit_non_us_company_identity(self):
        detected, evidence = explicit_non_us_identity_assessment(
            "WHJ is the Saudi AI powerhouse for enterprise operations."
        )
        self.assertTrue(detected)
        self.assertTrue(evidence)

    def test_no_fit_without_disqualifier_is_insufficient_not_denied(self):
        bucket, reason = route_company(
            is_parked=False,
            is_coming=False,
            is_agency=False,
            is_consumer_junk=False,
            is_placeholder=False,
            page_error="",
            page_status=200,
            location_status="UNKNOWN",
            maturity_risk=0,
            qualified_for_llm=False,
            score=12,
            potential_glasswing_fit=False,
            substantive_no_fit_evidence=True,
        )
        self.assertEqual(bucket, "insufficient")
        self.assertEqual(reason, "substantive_site_no_glasswing_fit_signals")

    def test_strong_multidimensional_candidate_is_promoted(self):
        bucket, reason = route_company(
            is_parked=False,
            is_coming=False,
            is_agency=False,
            is_consumer_junk=False,
            page_error="",
            page_status=200,
            location_status="UNKNOWN",
            maturity_risk=0,
            qualified_for_llm=False,
            score=58,
            potential_glasswing_fit=True,
            high_conviction_fit=False,
            strong_multidimensional_fit=True,
        )
        self.assertEqual(bucket, "priority")
        self.assertEqual(reason, "venture_quality_investable")

    def test_venture_quality_requires_wedge_and_moat_for_investable(self):
        text = (
            "Purpose-built cybersecurity platform for security teams. "
            "Our protocol-aware runtime continuously detects prompt injection "
            "and automates remediation in production. Proprietary attack "
            "corpus and closed-loop learning reduce investigation time. "
            "Request a demo and read our API documentation."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        profile = venture_quality_assessment(
            text,
            pillar=pillar,
            counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        self.assertTrue(profile["wedge_present"])
        self.assertTrue(profile["moat_present"])
        self.assertTrue(profile["investable_ready"])

    def test_generic_ai_slop_does_not_enter_review(self):
        text = (
            "Unlock the power of AI with our all-in-one AI platform. "
            "Our cutting-edge AI solutions revolutionize your business and "
            "seamlessly integrate AI for everyone."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        profile = venture_quality_assessment(
            text,
            pillar=pillar,
            counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        self.assertTrue(profile["slop_risk"])
        self.assertFalse(profile["reviewable"])
        self.assertFalse(profile["investable_ready"])

    def test_live_config_has_no_stale_quantum_filter(self):
        config_path = Path(__file__).resolve().parents[1] / "data" / "run_config.json"
        if not config_path.exists():
            # data/ is gitignored (crawl data/local run state, not source), so
            # a fresh checkout with no local run yet has no config to check.
            # This test's value is catching a stale filter in *your* actual
            # saved config, not asserting anything about a clean checkout.
            self.skipTest("no local run_config.json yet (fresh checkout)")
        config = json.loads(config_path.read_text())
        self.assertEqual(config["include_keywords"], [])
        self.assertEqual(config["exclude_keywords"], [])
        self.assertLessEqual(config["minimum_score"], 55)
        self.assertLessEqual(config["minimum_signal_dimensions"], 2)


if __name__ == "__main__":
    unittest.main()
