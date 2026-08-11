"""Pure diligence parsing and hard-guard logic with no API dependencies."""

from __future__ import annotations

import re
from typing import Any

from common import clean

DILIGENCE_VERSION = "2026-08-glasswing-rubric-v7"
KEEP_THRESHOLD = 70
CORE_MINIMUM = 3
SCORE_WEIGHTS = {
    "thesis_fit": 25,
    "enterprise_value": 20,
    "product_depth": 20,
    "technical_moat": 20,
    "stage_fit": 15,
}
OFFICIAL_CATEGORIES = {
    "vertical_ai",
    "physical_ai",
    "ai_adaptive_infrastructure",
    "intelligent_enterprise_defense",
    "collaborative_intelligence",
    "next_gen_compute",
    "other",
    "unclear",
}
FIELDS = [
    "decision",
    "thesis_fit",
    "enterprise_value",
    "product_depth",
    "technical_moat",
    "stage_fit",
    "evidence_confidence",
    "category",
    "us_based",
    "funding_millions_usd",
    "latest_round",
    "institutional_rounds",
    "company_maturity",
    "ownership_status",
    "ownership_evidence",
    "portfolio_overlap",
    "portfolio_company",
    "portfolio_overlap_evidence",
    "summary",
    "reason",
    "concern",
    "funding_evidence",
    "location_evidence",
    "positive_evidence",
    "green_flags",
    "negative_evidence",
    "unknowns",
]


def tag(text: str, name: str, default: str = "") -> str:
    match = re.search(
        rf"<{re.escape(name)}>\s*(.*?)\s*</{re.escape(name)}>",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return clean(match.group(1)) if match else default


def to_float(value: Any, default: float = -1.0) -> float:
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = -1) -> int:
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return default


def clamp_words(value: str, limit: int) -> str:
    return " ".join(clean(value).split()[:limit])


GREEN_FLAG_LABELS = {
    "ivy_league_spinout": "Ivy League spinout",
    "top_university_spinout": "University spinout",
    "ex_big_tech": "Ex-big tech team",
    "repeat_founder": "Repeat founder",
    "prior_exit": "Prior founder exit",
    "top_accelerator": "Top accelerator",
    "enterprise_traction": "Enterprise traction",
    "government_validation": "Government validation",
    "strategic_backing": "Strategic backing",
    "deep_tech_ip": "Defensible IP",
}


def parse_green_flags(value: str) -> list[dict[str, str]]:
    """Parse bounded, evidence-backed positive signals from model output."""
    flags: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in clean(value).split("|"):
        parts = [clean(part) for part in item.split("::", 2)]
        if len(parts) != 3:
            continue
        kind, label, evidence = parts
        kind = kind.lower()
        if kind not in GREEN_FLAG_LABELS or kind in seen or not evidence:
            continue
        seen.add(kind)
        flags.append({
            "type": kind,
            "label": clamp_words(label or GREEN_FLAG_LABELS[kind], 5),
            "evidence": clamp_words(evidence, 18),
        })
        if len(flags) == 4:
            break
    return flags


def recursively_collect_urls(value: Any, urls: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"url", "source"} and isinstance(child, str):
                if child.startswith(("http://", "https://")):
                    urls.add(child)
            recursively_collect_urls(child, urls)
    elif isinstance(value, list):
        for child in value:
            recursively_collect_urls(child, urls)


def dimension_score(assessment: dict) -> int:
    """Deterministic 0-100 Glasswing score from five bounded dimensions."""
    total = 0.0
    for name, weight in SCORE_WEIGHTS.items():
        total += max(0, min(5, to_int(assessment.get(name), 0))) * weight / 5
    return max(0, min(100, round(total)))


def substantive_decision(assessment: dict) -> str:
    core = (
        to_int(assessment.get("thesis_fit"), 0),
        to_int(assessment.get("enterprise_value"), 0),
        to_int(assessment.get("product_depth"), 0),
    )
    return (
        "KEEP"
        if dimension_score(assessment) >= KEEP_THRESHOLD and min(core) >= CORE_MINIMUM
        else "REJECT"
    )


def apply_hard_guards(
    assessment: dict,
    max_total_funding_m: float = 8.0,
    max_institutional_rounds: int = 2,
) -> dict:
    """Deterministic rules override model judgment."""
    reasons: list[str] = []

    if assessment["us_based"] == "no":
        reasons.append("Confirmed non-U.S.")

    funding = float(assessment["funding_millions_usd"])
    if funding > max_total_funding_m:
        reasons.append(f"Funding exceeds ${max_total_funding_m:g}M")

    if assessment["latest_round"] == "series_a_or_later":
        reasons.append("Series A or later")

    rounds = int(assessment["institutional_rounds"])
    if rounds > max_institutional_rounds:
        reasons.append("Too many prior rounds")

    ownership = assessment.get("ownership_status", "unclear")
    if ownership in {"acquired", "subsidiary", "merged", "public"}:
        reasons.append("Acquired or parent-owned")

    if assessment["company_maturity"] in {"established", "large_or_public"}:
        reasons.append("Too mature")

    if assessment.get("portfolio_overlap") == "yes":
        company = clean(assessment.get("portfolio_company")) or "Glasswing portfolio company"
        reasons.append(f"Direct technology overlap with {company}")

    assessment["fit_score"] = dimension_score(assessment)
    assessment["decision"] = substantive_decision(assessment)
    assessment["hard_disqualifiers"] = reasons
    if reasons:
        assessment["decision"] = "REJECT"
        assessment["fit_score"] = min(int(assessment["fit_score"]), 35)
        assessment["concerns"] = clamp_words(reasons[0], 15)
        if assessment.get("portfolio_overlap") == "yes":
            evidence = clean(assessment.get("portfolio_overlap_evidence"))
            assessment["reasoning"] = clamp_words(
                f"Direct technology overlap with {assessment.get('portfolio_company') or 'a Glasswing portfolio company'}. {evidence}",
                30,
            )
    elif assessment["decision"] == "KEEP":
        # Missing evidence is not negative evidence. Preserve a substantive
        # KEEP unless a confirmed hard disqualifier above exists, and surface
        # the most important uncertainty for human follow-up.
        if assessment["us_based"] == "unclear":
            assessment["concerns"] = assessment.get("concerns") or "Location not verified"
        elif ownership == "unclear":
            assessment["concerns"] = assessment.get("concerns") or "Ownership not verified"
        elif funding < 0 or assessment["latest_round"] == "unknown":
            assessment["concerns"] = assessment.get("concerns") or "Funding not verified"

    return assessment


def parse_assessment(
    text: str,
    urls: list[str],
    max_total_funding_m: float = 8.0,
    max_institutional_rounds: int = 2,
) -> dict:
    raw = {name: tag(text, name) for name in FIELDS}

    model_recommendation = raw["decision"].upper()
    if model_recommendation not in {"KEEP", "REJECT"}:
        model_recommendation = "REJECT"

    category = raw["category"].lower()
    if category not in OFFICIAL_CATEGORIES:
        category = "unclear"

    us_based = raw["us_based"].lower()
    if us_based not in {"yes", "no", "unclear"}:
        us_based = "unclear"

    latest_round = raw["latest_round"].lower()
    if latest_round not in {
        "none", "pre_seed", "seed", "series_a_or_later", "unknown"
    }:
        latest_round = "unknown"

    maturity = raw["company_maturity"].lower()
    if maturity not in {
        "very_early", "early_stage", "established",
        "large_or_public", "unknown"
    }:
        maturity = "unknown"

    ownership = raw["ownership_status"].lower()
    if ownership not in {
        "independent", "acquired", "subsidiary", "merged", "public", "unclear"
    }:
        ownership = "unclear"

    portfolio_overlap = raw["portfolio_overlap"].lower()
    if portfolio_overlap not in {"yes", "no", "unclear"}:
        portfolio_overlap = "unclear"

    assessment = {
        "decision": model_recommendation,
        "model_recommendation": model_recommendation,
        **{name: max(0, min(5, to_int(raw[name], 0))) for name in SCORE_WEIGHTS},
        "evidence_confidence": max(0, min(5, to_int(raw["evidence_confidence"], 0))),
        "category": category,
        "us_based": us_based,
        "funding_millions_usd": to_float(raw["funding_millions_usd"], -1),
        "latest_round": latest_round,
        "institutional_rounds": to_int(raw["institutional_rounds"], -1),
        "company_maturity": maturity,
        "ownership_status": ownership,
        "ownership_evidence": clamp_words(raw["ownership_evidence"], 20),
        "portfolio_overlap": portfolio_overlap,
        "portfolio_company": clamp_words(raw["portfolio_company"], 8),
        "portfolio_overlap_evidence": clamp_words(raw["portfolio_overlap_evidence"], 30),
        "product_summary": clamp_words(raw["summary"], 16),
        "reasoning": clamp_words(raw["reason"], 30),
        "concerns": clamp_words(raw["concern"], 18),
        "funding_evidence": clamp_words(raw["funding_evidence"], 18),
        "location_evidence": clamp_words(raw["location_evidence"], 18),
        "positive_evidence": clamp_words(raw["positive_evidence"], 35),
        "green_flags": parse_green_flags(raw["green_flags"]),
        "negative_evidence": clamp_words(raw["negative_evidence"], 25),
        "unknowns": clamp_words(raw["unknowns"], 25),
        "diligence_sources": " | ".join(urls[:5]),
        "diligence_version": DILIGENCE_VERSION,
    }
    return apply_hard_guards(
        assessment,
        max_total_funding_m=max_total_funding_m,
        max_institutional_rounds=max_institutional_rounds,
    )
