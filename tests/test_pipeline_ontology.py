"""The ontology document must describe the pipeline that exists.

A hand-written map of a filter goes stale the first time someone adds a guard,
and a stale map is worse than none: it is read as authoritative while the code
quietly removes domains it never mentions. These tests fail the build instead.
"""

import re
import unittest
from pathlib import Path

import certstream_filter
import funnel

DOC = Path("PIPELINE_ONTOLOGY.md")


def doc_text() -> str:
    return DOC.read_text(encoding="utf-8")


REMOVING_BUCKETS = ("reject", "ghost", "screened_out")


def route_company_outcomes() -> list[tuple[str, str]]:
    source = Path("filter.py").read_text(encoding="utf-8")
    body = source.split("def route_company(", 1)[1]
    body = body[: body.index("\ndef ")]
    return [
        (m.group(1), m.group(2).replace("{page_status}", "<status>"))
        for m in re.finditer(r'return\s+"(\w+)",\s*(?:f?")([^"]*)"', body)
    ]


def removing_reasons() -> list[str]:
    return [r for b, r in route_company_outcomes() if b in REMOVING_BUCKETS]


def routing_reasons() -> list[str]:
    return [r for b, r in route_company_outcomes() if b not in REMOVING_BUCKETS]


class CoverageTests(unittest.TestCase):
    def test_every_intake_guard_is_documented(self):
        text = doc_text()
        for guard in certstream_filter.DROP_REASONS:
            with self.subTest(guard=guard):
                self.assertIn(guard, text)

    def test_every_screening_removal_is_documented(self):
        # A new removal appearing without a line here means the document has
        # started lying about what shrinks the population.
        text = doc_text()
        for reason in removing_reasons():
            with self.subTest(reason=reason):
                self.assertIn(reason, text)

    def test_outcomes_that_route_onward_are_not_numbered_as_removals(self):
        # They belong in the document -- omitting them is what made it look
        # incomplete -- but never in a numbered row, which would overstate
        # what the filter discards. Seven of the twenty-one route onward.
        numbered = "\n".join(
            line for line in doc_text().splitlines()
            if re.match(r"^\| \d+ \|", line)
        )
        for reason in routing_reasons():
            with self.subTest(reason=reason):
                self.assertNotIn(reason, numbered)

    def test_routing_outcomes_are_still_explained_somewhere(self):
        text = doc_text()
        for reason in routing_reasons():
            with self.subTest(reason=reason):
                self.assertIn(reason, text)

    def test_every_removal_point_is_numbered(self):
        # The count is the point: a reader should be able to say how many
        # places a domain can die.
        numbered = re.findall(r"^\| \d+ \|", doc_text(), flags=re.M)
        expected = (
            3                       # reduction
            + len(certstream_filter.DROP_REASONS) - 3   # intake guards
            + 1                     # shared certificate
            + 1                     # already queued
            + 1                     # no verdict
            + len(removing_reasons())
            + 2                     # diligence selection: score floor, already done
            + 1                     # Claude REJECT
            + 1                     # human no
        )
        self.assertEqual(len(numbered), expected)

    def test_the_screening_order_table_matches_route_company(self):
        # funnel.SCREENING_ORDER drives what the page renders; if it drifts
        # from the code, guards silently fall to the bottom of the funnel.
        ordered = [key for key, _ in funnel.SCREENING_ORDER]
        actual = removing_reasons()
        placed = [key for key in ordered if key in actual]
        self.assertEqual(placed, [key for key in actual if key in ordered])


class DemotionTests(unittest.TestCase):
    """Layers that touch every domain without shrinking the population.

    apply_run_preferences reads like a filter -- include keywords, exclude
    keywords, industries, required US evidence -- but its effect is
    routing_bucket = "review". Leaving it out entirely made the document look
    incomplete; listing it as a removal would have overstated what the filter
    discards.
    """

    def test_run_preferences_are_described_as_demotion_not_removal(self):
        text = doc_text()
        self.assertIn("apply_run_preferences", text)
        self.assertIn("demotion", text.lower())

    def test_every_run_preference_reason_is_named(self):
        source = Path("filter.py").read_text(encoding="utf-8")
        index = source.index("include_keywords = keyword_list(")
        start = source.rfind("\ndef ", 0, index)
        body = source[start:source.index("\ndef ", index)]
        reasons = re.findall(r'reasons\.append\(\s*\n?\s*"([^"]+)"', body)
        self.assertTrue(reasons)
        for reason in reasons:
            with self.subTest(reason=reason):
                self.assertIn(reason.rstrip("_:"), doc_text())


class ClaimTests(unittest.TestCase):
    def test_the_unit_boundary_is_stated(self):
        # The single most important fact in the document: everything before L1
        # counts hostnames and everything after counts domains. Mixing them is
        # what made the funnel page irreconcilable.
        text = doc_text()
        self.assertIn("unit changes exactly once", text)

    def test_the_authoritative_verdict_source_is_named(self):
        # A funnel built on the queue's classification column counted rows
        # with no verdict as having survived every step.
        self.assertIn("public.classifications.routing_reason", doc_text())


if __name__ == "__main__":
    unittest.main()
