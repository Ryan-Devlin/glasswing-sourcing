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

    def test_curated_research_paper_list_is_non_product_content(self):
        # Reported case (bestpapers.ai): a curated list of academic papers
        # with citation counts and abstracts scored 81 and reached
        # Investable -- none of the directory/comparison terms above are
        # about research literature, so it slipped through undetected.
        papers, evidence = media_newsletter_assessment(
            "The best AI papers of 2025. DeepSeek-R1: Incentivizing "
            "Reasoning Capability in LLMs via Reinforcement Learning. "
            "176 citations. Abstract: we introduce our first-generation "
            "reasoning models."
        )
        self.assertTrue(papers)
        self.assertIn("curated research paper list", evidence)

    def test_ai_newsletter_with_buzzwords_is_caught_despite_product_language(self):
        # An AI newsletter reporting on the industry naturally uses the same
        # buzzwords ("proprietary", "platform", "automation") a real product
        # page would -- this used to slip through the old product_hits<=1
        # gate. Confirmed reported bug: these were scoring up to 100.
        newsletter, evidence = media_newsletter_assessment(
            "AI Weekly Report is a daily newsletter covering the latest news "
            "in artificial intelligence. Subscribe to get curated news on "
            "proprietary foundation model platforms, enterprise automation "
            "trends, and workflow research. Read the latest stories and stay "
            "on top of AI with our editorial team's daily brief."
        )
        self.assertTrue(newsletter)
        self.assertIn("daily newsletter", evidence)

    def test_webinar_landing_page_is_non_product_content(self):
        webinar, _ = media_newsletter_assessment(
            "Join our free webinar on scaling AI infrastructure. Register "
            "now to save your seat, view the agenda, and meet our speakers."
        )
        self.assertTrue(webinar)

    def test_webinar_invite_without_register_rsvp_language_is_still_caught(self):
        # Reported case: this invite copy scored 98 because it shares no
        # words with the "register/RSVP/agenda/speakers" confirming list --
        # only "Join <company> for a ... webinar" identifies it as an event.
        webinar, evidence = media_newsletter_assessment(
            "Join Flux for a live webinar on engineering intelligence. Learn "
            "how to measure AI development, improve visibility, reduce risk, "
            "and prove engineering ROI."
        )
        self.assertTrue(webinar)
        self.assertIn("join ... for a webinar", evidence)

    def test_personal_portfolio_site_is_non_product_content(self):
        personal, _ = media_newsletter_assessment(
            "Welcome to my personal website. This is my portfolio "
            "showcasing projects I've built as a freelance developer. "
            "Hire me for your next project."
        )
        self.assertTrue(personal)

    def test_founder_site_with_company_language_is_not_flagged_as_personal(self):
        # A solo founder's legitimate startup site can read personally, but
        # mentioning a team/customers/pricing means it isn't a bare
        # portfolio -- don't let the personal-site check reject real
        # early-stage companies.
        founder_site, _ = media_newsletter_assessment(
            "Welcome to my personal website. I'm building the future of "
            "enterprise data infrastructure. Our platform serves customers "
            "across our team, with pricing plans for every stage."
        )
        self.assertFalse(founder_site)

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

    def test_generic_governance_terms_do_not_fake_high_conviction(self):
        matched, _ = high_conviction_fit_assessment(
            "Enterprise observability, guardrails, and governance for AI."
        )
        self.assertFalse(matched)

    def test_vertical_visual_ai_bundle_is_high_conviction(self):
        matched, evidence = high_conviction_fit_assessment(
            "Visual AI agents provide continuous vision for production line "
            "monitoring and expert-guided root cause analysis."
        )
        self.assertTrue(matched)
        self.assertGreaterEqual(len(evidence), 3)

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

    def test_trusted_high_conviction_fit_is_not_ignored_by_router(self):
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
            score=18,
            potential_glasswing_fit=False,
            high_conviction_fit=True,
            substantive_no_fit_evidence=True,
        )
        self.assertEqual(bucket, "priority")
        self.assertEqual(reason, "trusted_high_conviction_product_fit")

    def test_high_conviction_never_overrides_explicit_disqualifier(self):
        bucket, reason = route_company(
            is_parked=False,
            is_coming=False,
            is_agency=True,
            is_consumer_junk=False,
            page_error="",
            page_status=200,
            location_status="UNKNOWN",
            maturity_risk=0,
            qualified_for_llm=False,
            score=90,
            potential_glasswing_fit=True,
            high_conviction_fit=True,
        )
        self.assertEqual(bucket, "reject")
        self.assertEqual(reason, "agency_or_consultancy")

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

    def test_frontier_technical_moat_can_substitute_for_wedge_copy(self):
        text = (
            "Agent security protocol for enterprise security teams. "
            "Our deployed platform uses MPC co-signing and programmable "
            "guardrails to enforce policies for autonomous agents on every "
            "wallet and chain. Open protocol, API documentation, and demo."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        profile = venture_quality_assessment(
            text,
            pillar=pillar,
            counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        self.assertTrue(profile["moat_present"])
        self.assertTrue(profile["frontier_technical_wedge"])
        self.assertTrue(profile["investable_ready"])

    def test_generic_enterprise_positioning_does_not_consume_review(self):
        text = (
            "Enterprise AI operations platform for financial services. "
            "Our software provides data insights and workflow automation "
            "for advisors, asset managers, and operations teams. Book a demo."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        profile = venture_quality_assessment(
            text,
            pillar=pillar,
            counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        self.assertFalse(profile["moat_present"])
        self.assertFalse(profile["reviewable"])

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
