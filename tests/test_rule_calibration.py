import json
import unittest

import filter as screening_filter
from pathlib import Path

from filter import (
    agency_assessment,
    b2c_assessment,
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

    def test_boutique_security_advisory_is_a_consultancy_not_a_product(self):
        # Confirmed live: this exact copy scored as a top cybersecurity
        # candidate -- a small team billed per project ("the engagement")
        # advising clients is a consultancy regardless of industry.
        rejected, evidence = agency_assessment(
            "A small, senior team of offensive professionals who crossed "
            "into AI security. We secure AI systems, advise leaders on AI "
            "strategy, and build the capability that outlasts the "
            "engagement."
        )
        self.assertTrue(rejected)
        self.assertTrue(
            any(
                term in evidence
                for term in ("advise leaders", "outlasts the engagement", "crossed into ai security")
            )
        )

    def test_bespoke_ai_implementation_for_regulated_industries_is_a_service(self):
        # Confirmed live: this exact copy scored as a top candidate --
        # bespoke, per-client "building AI systems for X" is a services
        # model, not a repeatable product, even with enterprise-sounding
        # language.
        rejected, evidence = agency_assessment(
            "A precision approach to AI implementation. Building AI "
            "systems for regulated industries with enterprise-grade "
            "controls."
        )
        self.assertTrue(rejected)
        self.assertIn("bespoke-ai-implementation services", evidence)

    def test_handoff_delivery_model_is_a_service_signal(self):
        rejected, evidence = agency_assessment(
            "Production AI agents, workflow automation, and system "
            "integrations. Built for your business, handed off to your "
            "team."
        )
        self.assertTrue(rejected)
        self.assertIn("handed off to your team", evidence)

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

    def test_bare_job_title_bio_is_a_personal_site_not_a_company(self):
        # Confirmed live: real examples with no explicit "my portfolio" /
        # "hire me" self-identification at all -- just a bare job title as
        # the entire visible content -- were landing in Needs Review as if
        # they were real companies to screen.
        engineer_bio, evidence = media_newsletter_assessment(
            "Full stack engineer. Builder of systems that run."
        )
        self.assertTrue(engineer_bio)
        self.assertIn("bare job title bio, no company framing", evidence)

        consultant_bio, _ = media_newsletter_assessment(
            "Frontend engineer building scalable platforms. Currently at "
            "Dream11, serving 150M+ users."
        )
        self.assertTrue(consultant_bio)

    def test_habit_tracker_consumer_app_is_caught(self):
        # Confirmed live: real consumer apps (habit tracking, personal
        # money transfer, a reading app, a cannabis guide, a message-tone
        # checker) all scored 0 on b2c_assessment -- none of their real
        # vocabulary was in B2C_USE_CASE at all.
        is_junk, score, evidence = b2c_assessment(
            "Upmode - Track habits. Earn XP. Upmode is a free habit "
            "tracker with XP and levels. Track your daily habits, earn "
            "XP, and level up. Make discipline measurable."
        )
        self.assertTrue(is_junk)
        self.assertIn("habit tracker", evidence)

    def test_hobby_open_source_project_is_not_a_company(self):
        # Confirmed live: "A modern open-source CRM" (an OSS client, no
        # pricing/customers/team anywhere) reached Needs Review with
        # nothing an investor could actually evaluate.
        oss_project, evidence = media_newsletter_assessment(
            "Twenty. A modern open-source CRM."
        )
        self.assertTrue(oss_project)
        self.assertIn("open-source", evidence)

    def test_open_core_company_with_pricing_is_not_flagged(self):
        # An open-core company is a real, investable business even though
        # it also describes itself as open-source -- company_signals
        # (pricing/enterprise/customers) must keep this one out.
        open_core, _ = media_newsletter_assessment(
            "Our open-source platform is trusted by enterprise customers "
            "worldwide. Our team offers pricing plans for every stage, "
            "with case studies from our largest customers."
        )
        self.assertFalse(open_core)

    def test_careers_page_job_title_is_not_a_personal_site(self):
        # The job-title-bio check must not fire on a real company's careers
        # page just because it mentions an engineering role -- it's much
        # longer and carries real company framing (pricing/team/customers).
        careers_page, _ = media_newsletter_assessment(
            "We're hiring a Senior Software Engineer to join our team. "
            "Our platform serves thousands of customers with enterprise "
            "pricing plans, and this role will help scale our product "
            "across every stage of our growing company. Apply today to "
            "join our team and help build the future of our platform."
        )
        self.assertFalse(careers_page)

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

    def test_short_registrar_parking_text_is_still_caught(self):
        # Confirmed live: word_count<=2 used to return early (not parked)
        # before the parked-phrase check ever ran -- real registrar parking
        # pages are often only a handful of words, so this exact case fell
        # through to Needs Review instead of Hard Denied.
        detected, reason, _ = placeholder_assessment(
            "porkbun.com parked domain",
            word_count=3,
        )
        self.assertTrue(detected)
        self.assertEqual(reason, "parked_or_for_sale")

    def test_gandi_parked_by_owner_phrasing_is_caught(self):
        # Confirmed live: this exact Gandi.net parking template ("is
        # currently parked by the owner") didn't match the old, narrower
        # "this domain is parked" phrase.
        text = (
            "This domain name has been registered with Gandi.net. It is "
            "currently parked by the owner."
        )
        detected, reason, _ = placeholder_assessment(
            text,
            word_count=len(text.split()),
        )
        self.assertTrue(detected)
        self.assertEqual(reason, "parked_or_for_sale")

    def test_generic_domain_aggregator_template_is_caught(self):
        # Confirmed live: real examples like "asteriskrd.app is your first
        # and best source for information about asteriskrd" and "Find the
        # best information and most relevant links on all topics related
        # to..." are ad-supported SEO-filler parking templates, not real
        # content, but didn't match any existing parked/placeholder phrase.
        text = (
            "asteriskrd.app is your first and best source for information "
            "about asteriskrd. Here you will also find topics relating to "
            "issues of general interest. We hope you find what you are "
            "looking for!"
        )
        detected, reason, _ = placeholder_assessment(
            text,
            word_count=len(text.split()),
        )
        self.assertTrue(detected)
        self.assertEqual(reason, "parked_or_for_sale")

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

    def test_no_fit_without_disqualifier_is_discarded_not_denied(self):
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
        self.assertEqual(bucket, "screened_out")
        self.assertEqual(reason, "substantive_site_no_glasswing_fit_signals")

    def test_strong_multidimensional_candidate_reaches_review(self):
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
        self.assertEqual(bucket, "review")
        self.assertEqual(reason, "venture_quality_investable")

    def test_trusted_high_conviction_fit_still_reaches_review(self):
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
        self.assertEqual(bucket, "review")
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

    def test_strong_school_mention_slightly_lifts_founder_score(self):
        text = (
            "Founded by a team with deep enterprise software experience. "
            "Our founders previously built and sold two companies."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        baseline = venture_quality_assessment(
            text, pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        with_school = venture_quality_assessment(
            text + " One of our founders is a Stanford graduate.",
            pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        # Slight, not decisive: a real lift, but nowhere near the ~18-20
        # points a real founder_visibility/credibility signal is worth.
        delta = with_school["founder_score"] - baseline["founder_score"]
        self.assertGreater(delta, 0)
        self.assertLessEqual(delta, 10)

    def test_diploma_mill_mention_slightly_lowers_founder_score(self):
        text = (
            "Founded by a team with deep enterprise software experience. "
            "Our founders previously built and sold two companies. "
            "One of our founders is a Stanford graduate."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        with_top_school = venture_quality_assessment(
            text, pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        with_bad_school = venture_quality_assessment(
            text.replace("Stanford", "University of Phoenix"),
            pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        self.assertLess(
            with_bad_school["founder_score"],
            with_top_school["founder_score"],
        )

    def test_strong_work_experience_slightly_lifts_founder_score(self):
        text = (
            "Founded by a team with deep enterprise software experience. "
            "Our founders previously built and sold two companies."
        )
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        baseline = venture_quality_assessment(
            text, pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        with_faang = venture_quality_assessment(
            text + " Our team previously worked at Google and Stripe.",
            pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
        delta = with_faang["founder_score"] - baseline["founder_score"]
        self.assertGreater(delta, 0)
        self.assertLessEqual(delta, 10)

    def test_school_signal_alone_never_decides_investable_ready(self):
        # "Slightly" means never decisive on its own -- a thin page with
        # nothing but a school name and no real product/wedge/moat
        # evidence must not become investable_ready just from pedigree.
        text = "Founded by a Harvard and MIT team. We are building something."
        _, pillar, _, counts = score_text(text)
        from filter import glasswing_dimensions

        profile = venture_quality_assessment(
            text, pillar=pillar, counts=counts,
            dimensions=glasswing_dimensions(text),
        )
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


class InaccessiblePageRoutingTests(unittest.TestCase):
    """Error pages must never reach Needs Review.

    Confirmed live 2026-08-11: getapplica.io (a Cloudflare "525 SSL handshake
    failed" interstitial) and wounder.tech (an "nginx 404 Not Found" page)
    both sat in Needs Review scoring 5 and 1. Both render real text, so the
    blank-page ghost check never caught them, and "review" is protected from
    purging -- so they stayed there permanently.
    """

    def _route(self, *, page_error="", page_status=200, score=5):
        return route_company(
            is_parked=False,
            is_coming=False,
            is_agency=False,
            is_consumer_junk=False,
            is_placeholder=False,
            page_error=page_error,
            page_status=page_status,
            location_status="UNKNOWN",
            maturity_risk=0,
            qualified_for_llm=False,
            score=score,
            potential_glasswing_fit=True,
        )

    def test_http_404_page_is_ghosted_not_sent_to_needs_review(self):
        bucket, reason = self._route(page_status=404, score=1)
        self.assertEqual(bucket, "ghost")
        self.assertEqual(reason, "inaccessible_http_404")

    def test_cloudflare_525_error_page_is_ghosted(self):
        bucket, reason = self._route(page_status=525, score=5)
        self.assertEqual(bucket, "ghost")
        self.assertEqual(reason, "inaccessible_http_525")

    def test_every_4xx_and_5xx_status_is_ghosted(self):
        for status in (400, 403, 404, 410, 500, 502, 503, 525):
            with self.subTest(status=status):
                bucket, _ = self._route(page_status=status)
                self.assertEqual(bucket, "ghost")

    def test_fetch_error_is_ghosted(self):
        bucket, reason = self._route(page_error="TimeoutError: fetch timed out")
        self.assertEqual(bucket, "ghost")
        self.assertEqual(reason, "inaccessible_site")

    def test_a_reachable_page_is_still_eligible_for_needs_review(self):
        # The fix must not sweep real, reachable companies into ghost.
        bucket, _ = self._route(page_status=200, score=55)
        self.assertNotEqual(bucket, "ghost")


class BlockedSiteRetryTests(unittest.TestCase):
    """"We were blocked" must never mean "not a company".

    Confirmed live: of ~5,400 domains discarded in 90 minutes, ~450 were HTTP
    errors and 403 was the single largest code -- a Cloudflare/WAF challenge
    aimed at our own scraper. Certificates are issued before a site goes live,
    so these skew toward the freshest startups in the feed. Tombstoning them as
    automated exclusions stopped them ever being crawled again.
    """

    def test_blocked_and_server_error_statuses_are_retryable(self):
        for status in (401, 402, 403, 407, 408, 409, 425, 429,
                       500, 502, 503, 520, 521, 522, 523, 526, 530, 599):
            with self.subTest(status=status):
                self.assertTrue(
                    screening_filter.is_retryable_inaccessible_reason(
                        f"inaccessible_http_{status}"
                    ),
                    f"{status} should be retried, not permanently excluded",
                )

    def test_genuinely_absent_pages_are_not_retryable(self):
        # 404/410 mean the server answered clearly: nothing is there.
        for status in (404, 410, 400, 451):
            with self.subTest(status=status):
                self.assertFalse(
                    screening_filter.is_retryable_inaccessible_reason(
                        f"inaccessible_http_{status}"
                    )
                )

    def test_non_http_reasons_are_never_treated_as_retryable(self):
        for reason in (
            "",
            None,
            "parked_or_for_sale",
            "agency_or_consultancy",
            "ghost_or_inaccessible_site",
            "inaccessible_site",
            "inaccessible_http_",
            "inaccessible_http_abc",
        ):
            with self.subTest(reason=reason):
                self.assertFalse(
                    screening_filter.is_retryable_inaccessible_reason(reason)
                )

    def test_worker_requeues_blocked_domains_instead_of_closing_them_out(self):
        source = Path("continuous_pipeline.py").read_text(encoding="utf-8")
        self.assertIn('screening_stats.get("retry_domains", [])', source)
        # A retried domain must not also be counted as successfully screened.
        self.assertIn("and domain not in retry_domains", source)


class SubstantiveSiteFloorTests(unittest.TestCase):
    """A real site scoring well must not be deleted for lacking a keyword.

    This rule sat ahead of every score-based review path, so a company scoring
    in the 40s was discarded before it could reach them. Measured over 3 hours
    of live traffic: 86.5% of discards scored 0-9, so the rule is correct in
    bulk -- but the top of the discard pile was xfactor.io at 50,
    trialetics.io 48, nexorum.io 47 and detaillabs.ai 45, all purged.
    """

    def _route(self, score):
        return route_company(
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
            score=score,
            potential_glasswing_fit=False,
            substantive_no_fit_evidence=True,
        )

    def test_low_signal_substantive_sites_are_still_discarded(self):
        # The 86.5% majority must keep being discarded; this is not a reversal.
        for score in (0, 5, 12, 25, 34):
            with self.subTest(score=score):
                bucket, reason = self._route(score)
                self.assertEqual(bucket, "screened_out")
                self.assertEqual(
                    reason, "substantive_site_no_glasswing_fit_signals"
                )

    def test_higher_scoring_substantive_sites_reach_review(self):
        for score in (35, 44, 50, 72):
            with self.subTest(score=score):
                bucket, _ = self._route(score)
                self.assertNotEqual(
                    bucket, "screened_out",
                    f"score {score} was discarded before any review path",
                )
                self.assertNotEqual(bucket, "ghost")

    def test_floor_is_tunable_and_bounded(self):
        source = Path("filter.py").read_text(encoding="utf-8")
        block = source.split("SUBSTANTIVE_REVIEW_FLOOR = ", 1)[1][:200]
        self.assertIn("GLASSWING_SUBSTANTIVE_REVIEW_FLOOR", block)
        # Clamped, so a bad value cannot silently disable or invert the rule.
        self.assertIn("min(100", block)
        self.assertIn("max(", block)
        self.assertEqual(screening_filter.SUBSTANTIVE_REVIEW_FLOOR, 35)

    def test_retained_substantive_site_is_not_a_hard_denial(self):
        # It must land in the human queue, not be presented as a denial.
        _, reason = self._route(50)
        self.assertNotIn(reason, screening_filter.HARD_DENIAL_REASONS)
