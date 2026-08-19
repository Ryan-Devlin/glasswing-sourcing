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


def route_company_reasons() -> list[str]:
    source = Path("filter.py").read_text(encoding="utf-8")
    body = source.split("def route_company(", 1)[1]
    body = body[: body.index("\ndef ")]
    return [
        m.group(2).replace("{page_status}", "<status>")
        for m in re.finditer(r'return\s+"(\w+)",\s*(?:f?")([^"]*)"', body)
    ]


class CoverageTests(unittest.TestCase):
    def test_every_intake_guard_is_documented(self):
        text = doc_text()
        for guard in certstream_filter.DROP_REASONS:
            with self.subTest(guard=guard):
                self.assertIn(guard, text)

    def test_every_screening_outcome_is_documented(self):
        # 21 outcomes today. A new one appearing without a line here means the
        # document has started lying about what removes domains.
        text = doc_text()
        for reason in route_company_reasons():
            with self.subTest(reason=reason):
                self.assertIn(reason, text)

    def test_the_screening_order_table_matches_route_company(self):
        # funnel.SCREENING_ORDER drives what the page renders; if it drifts
        # from the code, guards silently fall to the bottom of the funnel.
        ordered = [key for key, _ in funnel.SCREENING_ORDER]
        actual = route_company_reasons()
        placed = [key for key in ordered if key in actual]
        self.assertEqual(placed, [key for key in actual if key in ordered])


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
