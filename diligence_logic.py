"""Pure diligence parsing and hard-guard logic with no API dependencies."""

from __future__ import annotations

import re
from typing import Any

from common import clean

DILIGENCE_VERSION = "2026-08-evidence-not-absence-v5"
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
    "score",
    "category",
    "us_based",
    "funding_millions_usd",
    "latest_round",
    "institutional_rounds",
    "company_maturity",
    "ownership_status",
    "ownership_evidence",
    "summary",
    "reason",
    "concern",
    "funding_evidence",
    "location_evidence",
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

    if reasons:
        assessment["decision"] = "REJECT"
        assessment["fit_score"] = min(int(assessment["fit_score"]), 35)
        assessment["concerns"] = clamp_words(reasons[0], 5)
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

    if assessment["decision"] == "KEEP":
        if assessment["fit_score"] < 75:
            assessment["decision"] = "REJECT"

    return assessment


def parse_assessment(
    text: str,
    urls: list[str],
    max_total_funding_m: float = 8.0,
    max_institutional_rounds: int = 2,
) -> dict:
    raw = {name: tag(text, name) for name in FIELDS}

    decision = raw["decision"].upper()
    if decision not in {"KEEP", "REJECT"}:
        decision = "REJECT"

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

    assessment = {
        "decision": decision,
        "fit_score": max(0, min(to_int(raw["score"], 0), 95)),
        "category": category,
        "us_based": us_based,
        "funding_millions_usd": to_float(raw["funding_millions_usd"], -1),
        "latest_round": latest_round,
        "institutional_rounds": to_int(raw["institutional_rounds"], -1),
        "company_maturity": maturity,
        "ownership_status": ownership,
        "ownership_evidence": clamp_words(raw["ownership_evidence"], 20),
        "product_summary": clamp_words(raw["summary"], 5),
        "reasoning": clamp_words(raw["reason"], 5),
        "concerns": clamp_words(raw["concern"], 5),
        "funding_evidence": clamp_words(raw["funding_evidence"], 12),
        "location_evidence": clamp_words(raw["location_evidence"], 12),
        "diligence_sources": " | ".join(urls[:5]),
        "diligence_version": DILIGENCE_VERSION,
    }
    return apply_hard_guards(
        assessment,
        max_total_funding_m=max_total_funding_m,
        max_institutional_rounds=max_institutional_rounds,
    )
