import unittest

from filter import (
    commodity_wrapper_assessment,
    investment_eligibility_adjusted_score,
    score_text,
)


class GlasswingFitScoringTests(unittest.TestCase):
    def score(self, text: str) -> int:
        return score_text(text)[0]

    def test_glasswing_fit_outranks_generic_ai_and_noise(self):
        strong = {
            "aegisec": (
                "AI security platform for modern enterprises protecting "
                "against prompt injection, data leakage, and adversarial attack."
            ),
            "movedot": (
                "AI agents for physical engineering turn telemetry, simulations, "
                "models, and documents into answers for engineering teams."
            ),
            "formal": (
                "Protocol-aware data security proxy securing databases, APIs, "
                "and infrastructure with dynamic masking, real-time policies, "
                "and zero-trust access."
            ),
            "ozana": (
                "Real-time medical interpretation and ambient clinical "
                "documentation for healthcare and emergency services on one "
                "secure platform."
            ),
            "pharos": (
                "Security and compliance infrastructure for AI agents with a "
                "live attack corpus and audit-ready evidence."
            ),
        }
        noise = {
            "custom_ai_shop": (
                "AI development company building custom AI solutions and "
                "custom AI agents. Get a free consultation."
            ),
            "job_board": "Job board for agentic AI jobs and careers.",
            "coupons": "Verified certification coupon and discount codes.",
            "cold_calling": (
                "No-code AI cold calling and appointment scheduling platform."
            ),
            "portfolio": "Personal portfolio of an aspiring software engineer.",
        }

        strong_scores = {name: self.score(text) for name, text in strong.items()}
        noise_scores = {name: self.score(text) for name, text in noise.items()}

        self.assertGreaterEqual(min(strong_scores.values()), 60, strong_scores)
        self.assertLessEqual(max(noise_scores.values()), 42, noise_scores)
        self.assertGreater(
            min(strong_scores.values()),
            max(noise_scores.values()),
        )

    def test_specific_security_gets_correct_pillar(self):
        score, pillar, _, _ = score_text(
            "Enterprise security platform preventing prompt injection, data "
            "leakage, and adversarial attack with zero-trust access."
        )
        self.assertGreaterEqual(score, 60)
        self.assertEqual(pillar, "intelligent_enterprise_defense")

    def test_video_security_company_gets_defense_not_physical_ai_pillar(self):
        # "physical security"/"video security" used to appear in both
        # physical_ai's and intelligent_enterprise_defense's term lists --
        # a camera-monitoring security vendor always won a tied vote toward
        # physical_ai (defined first in THESIS), even though a surveillance
        # SaaS is a security-defense product, not a robotics/autonomous-
        # systems one. This was a real, visible skew: 3 of the top 4
        # "Investable" companies by priority score were tagged Physical AI.
        _, pillar, _, _ = score_text(
            "Cloud video security platform for physical security teams. "
            "AI-powered CCTV monitoring, alerts, and threat detection for "
            "enterprise security operations."
        )
        self.assertEqual(pillar, "intelligent_enterprise_defense")

    def test_usable_html_gets_low_nonzero_coverage_score(self):
        score = self.score(
            "A neighborhood bakery publishing its menu, opening hours, "
            "location, and online ordering information for local customers."
        )
        self.assertGreater(score, 0)
        self.assertLess(score, 25)

    def test_empty_html_is_the_only_zero_score(self):
        self.assertEqual(self.score(""), 0)

    def test_generic_ai_employee_wrapper_is_not_fit(self):
        is_wrapper, evidence = commodity_wrapper_assessment(
            "An AI employee that automates any task. Build your own agent "
            "and deploy a sales agent in minutes."
        )
        self.assertTrue(is_wrapper)
        self.assertIn("ai employee", evidence)

    def test_online_casino_is_an_explicit_hard_exclusion(self):
        is_wrapper, evidence = commodity_wrapper_assessment(
            "Bangladesh's best online casino and sports betting platform "
            "with live games, bonuses, and fast payouts."
        )
        self.assertTrue(is_wrapper)
        self.assertIn("online casino", evidence)

    def test_non_english_caps_strong_thesis_fit_at_five(self):
        adjusted, reasons = investment_eligibility_adjusted_score(
            79,
            predominantly_non_english=True,
        )
        self.assertEqual(adjusted, 5)
        self.assertIn("predominantly_non_english:max_5", reasons)

    def test_confirmed_non_us_caps_score_at_ten(self):
        adjusted, reasons = investment_eligibility_adjusted_score(
            93,
            location_status="NON_US",
        )
        self.assertEqual(adjusted, 10)
        self.assertIn("confirmed_non_us:max_10", reasons)

    def test_multiple_disqualifiers_use_strictest_cap(self):
        adjusted, _ = investment_eligibility_adjusted_score(
            84,
            agency=True,
            consumer=True,
            mature_or_funded=True,
        )
        self.assertEqual(adjusted, 15)


if __name__ == "__main__":
    unittest.main()
