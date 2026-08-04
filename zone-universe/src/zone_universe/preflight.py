from __future__ import annotations

import re
from dataclasses import dataclass

from ai_search.extract import (
    COMING_SOON_SIGNALS,
    LOW_VALUE_HOMEPAGE_SIGNALS,
    PLACEHOLDER_STOP_SIGNALS,
    ExtractedPage,
    score_internal_link,
)

from .domain_quality import crawl_priority


FRONTIER_SIGNALS = (
    "artificial intelligence",
    "machine learning",
    "generative ai",
    "gen ai",
    "agentic ai",
    "ai agent",
    "ai agents",
    "large language model",
    "large language models",
    "llm",
    "computer vision",
    "cybersecurity",
    "cyber security",
    "threat detection",
    "quantum computing",
    "quantum",
    "robotics",
    "autonomous systems",
    "physical ai",
    "gpu infrastructure",
    "ai infrastructure",
    "data infrastructure",
    "cloud infrastructure",
    "developer infrastructure",
    "semiconductor",
    "foundation model",
    "neural network",
    "digital twin",
    "industrial automation",
    "drug discovery",
    "computational biology",
    "defense technology",
)

PRODUCT_SIGNALS = (
    "software",
    "platform",
    "product",
    "api",
    "infrastructure",
    "automation",
    "analytics",
    "developer tools",
    "workflow",
    "security",
    "technology",
    "data platform",
    "operating system",
    "enterprise",
    "cloud",
)

COMPANY_SIGNALS = (
    "our product",
    "our platform",
    "our technology",
    "we build",
    "we help",
    "customers",
    "companies",
    "businesses",
    "enterprises",
    "teams",
    "industries",
    "founded",
    "company",
)


@dataclass(frozen=True)
class PreflightDecision:
    promising: bool
    score: int
    reason: str
    evidence: tuple[str, ...] = ()


def _contains(text: str, phrases: tuple[str, ...]) -> tuple[str, ...]:
    """Return boundary-safe phrase hits from a small, cheap lexicon."""
    lower = (text or "").lower()
    return tuple(
        phrase
        for phrase in phrases
        if re.search(
            rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])",
            lower,
        )
    )


def assess_homepage(domain: str, page: ExtractedPage) -> PreflightDecision:
    """Decide whether a site deserves full crawling and HTML retention.

    This intentionally favors recall over precision. It is not the investment
    classifier; it is a cheap spend gate that requires some evidence of a real
    technology product or a thesis-relevant company before deeper requests,
    storage, indexing, RDAP, and classification are allowed.
    """
    metadata = " ".join(
        value
        for value in (page.title, page.description, page.headings)
        if value
    )
    evidence_text = f"{metadata} {page.body[:2500]}".strip()
    lower = evidence_text.lower()
    domain_rank = crawl_priority(domain)
    explicit_prelaunch = any(
        signal in lower for signal in COMING_SOON_SIGNALS
    )

    if page.is_parked:
        return PreflightDecision(False, 0, "parked_or_for_sale")
    placeholder_hits = tuple(
        signal for signal in PLACEHOLDER_STOP_SIGNALS if signal in lower
    )
    if placeholder_hits:
        return PreflightDecision(
            False,
            0,
            "hosting_placeholder",
            placeholder_hits,
        )
    if (
        not evidence_text
        or (page.word_count < 3 and not explicit_prelaunch)
    ):
        return PreflightDecision(False, 0, "no_usable_homepage_copy")

    frontier = _contains(evidence_text, FRONTIER_SIGNALS)
    products = _contains(evidence_text, PRODUCT_SIGNALS)
    companies = _contains(evidence_text, COMPANY_SIGNALS)
    low_value = tuple(
        signal for signal in LOW_VALUE_HOMEPAGE_SIGNALS if signal in lower
    )
    link_families = {
        family
        for link in page.internal_links[:80]
        for _, family in (score_internal_link(link, ""),)
        if family
    }
    strong_links = link_families.intersection({"technical", "product"})

    score = min(
        100,
        len(frontier) * 34
        + min(len(products), 3) * 12
        + min(len(companies), 2) * 9
        + (18 if strong_links else 0)
        + (8 if len(metadata.split()) >= 6 else 0)
        + (8 if domain_rank == 0 else 4 if domain_rank == 1 else 0),
    )
    evidence = tuple(dict.fromkeys(
        [*frontier, *products, *companies, *sorted(strong_links)]
    ))[:12]

    if low_value and not frontier and not strong_links:
        return PreflightDecision(
            False,
            score,
            "obvious_low_value_site",
            tuple(low_value),
        )

    if explicit_prelaunch:
        if (
            frontier
            or domain_rank == 0
            or (domain_rank == 1 and (products or companies))
        ):
            return PreflightDecision(
                True,
                max(score, 50),
                "promising_prelaunch",
                evidence,
            )
        return PreflightDecision(
            False,
            score,
            "generic_prelaunch_without_tech_signal",
            evidence,
        )

    if frontier and (
        products
        or companies
        or strong_links
        or page.word_count >= 12
    ):
        return PreflightDecision(
            True,
            max(score, 60),
            "frontier_or_ai_product_signal",
            evidence,
        )
    if (
        len(products) >= 2
        and (companies or strong_links)
        and page.word_count >= 15
    ):
        return PreflightDecision(
            True,
            max(score, 50),
            "credible_technology_company",
            evidence,
        )
    if strong_links and products and page.word_count >= 12:
        return PreflightDecision(
            True,
            max(score, 45),
            "product_or_technical_evidence_links",
            evidence,
        )
    if (
        domain_rank == 0
        and products
        and page.word_count >= 10
    ):
        return PreflightDecision(
            True,
            max(score, 45),
            "strong_domain_and_product_signal",
            evidence,
        )
    return PreflightDecision(
        False,
        score,
        "no_clear_technology_company_signal",
        evidence,
    )
