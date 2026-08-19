"""Non-US self-identification, which decides a hard denial.

The rule already existed and almost never fired: it was hardcoded to six
countries and its patterns required whitespace, so "the Saudi-built AI
platform" was missed purely because of the hyphen. Measured against the
candidate corpus, 18 companies that plainly say they are elsewhere -- "made in
Italy", "Canada's leading", "Bangladesh's top online casino", "for Australian
lawyers" -- had all reached the human review queue.
"""

import unittest
from pathlib import Path

import filter as screening_filter

assess = screening_filter.explicit_non_us_identity_assessment


class OriginTests(unittest.TestCase):
    def test_hyphenated_demonyms_are_caught(self):
        # The exact miss: whitespace-only patterns could not see the hyphen.
        hit, evidence = assess("Nakhla is the Saudi-built AI platform.")
        self.assertTrue(hit)
        self.assertTrue(any(e.startswith("origin:") for e in evidence))

    def test_origin_phrasings(self):
        for text in (
            "Made in Germany. The leading workflow platform.",
            "A British AI company building developer tools.",
            "Based in Hong Kong, we build enterprise automation.",
            "Canada's leading SR&ED consultants.",
            "African-built enterprise AI systems.",
        ):
            with self.subTest(text=text):
                self.assertTrue(assess(text)[0], text)

    def test_origin_is_labelled_separately_from_market(self):
        # They are different claims -- where a company is, versus who it
        # sells to -- and a reviewer needs to see which one fired.
        _, evidence = assess("Made in Germany. The leading workflow platform.")
        self.assertTrue(any(e.startswith("origin:") for e in evidence))


class MarketTests(unittest.TestCase):
    def test_a_market_stated_entirely_elsewhere_is_caught(self):
        for text in (
            "AI-first real estate intelligence for Indian brokerages.",
            "The premier digital business directory for Eswatini.",
            "AI platform for complex Australian legal work.",
            "Aurora Trust helps European SMEs navigate AI regulation.",
        ):
            with self.subTest(text=text):
                hit, evidence = assess(text)
                self.assertTrue(hit, text)
                self.assertTrue(any(e.startswith("market:") for e in evidence))


class FalsePositiveTests(unittest.TestCase):
    def test_a_us_company_selling_abroad_is_not_flagged(self):
        # Selling into a market is not being from it. This is the failure the
        # original narrow patterns were protecting against, and generalising
        # them must not give it up.
        for text in (
            "Boston-based AI company. We serve customers across Europe and Asia.",
            "AI agents for enterprise sales teams. Built in San Francisco.",
        ):
            with self.subTest(text=text):
                self.assertFalse(assess(text)[0], text)

    def test_copy_with_no_geography_is_not_flagged(self):
        for text in (
            "Open source algorithmic trading platform for Python developers.",
            "A private, native assistant built for Apple silicon Macs.",
            "",
            "   ",
        ):
            with self.subTest(text=text):
                self.assertFalse(assess(text)[0], text)

    def test_us_location_evidence_still_wins(self):
        # The caller only promotes to NON_US when location assessment has not
        # already found US evidence; that guard has to keep working.
        text = "Headquartered in Austin, Texas. Serving European enterprises."
        location, _ = screening_filter.company_location_assessment(text)
        self.assertNotEqual(location, "NON_US")


class WiringTests(unittest.TestCase):
    def test_non_us_is_a_hard_denial_reason(self):
        # route_company turns a NON_US location into a hard denial; this
        # asserts the reason is registered as one rather than quietly routed
        # to review, which is where these were all ending up.
        self.assertIn("confirmed_non_us", screening_filter.HARD_DENIAL_REASONS)

    def test_route_company_rejects_a_non_us_location(self):
        source = Path(screening_filter.__file__).read_text(encoding="utf-8")
        body = source.split("def route_company(", 1)[1]
        self.assertIn('return "reject", "confirmed_non_us"', body)

    def test_patterns_are_compiled_once_not_per_call(self):
        # Looping ~250 terms across four shapes meant roughly a thousand
        # regex searches per domain screened, on the hot path.
        self.assertTrue(screening_filter._ORIGIN_PATTERNS)
        self.assertTrue(screening_filter._MARKET_PATTERNS)
        self.assertLess(
            len(screening_filter._ORIGIN_PATTERNS)
            + len(screening_filter._MARKET_PATTERNS),
            12,
        )


if __name__ == "__main__":
    unittest.main()
