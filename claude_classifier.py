#!/usr/bin/env python3
"""External diligence + Glasswing classification using one Claaalude web search."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
from anthropic import AsyncAnthropic
from dotenv import load_dotenv

from common import (
    FILTERED, LLM, atomic_write_json, clean, get_model,
    save_automated_basket_entries, stable_domain, upsert_csv,
)
from filter import score_text
from classification_store import (
    backfill_claude_training_labels,
    purge_invalid_claude_diligence,
    upsert_claude_diligence,
)
from diligence_logic import (
    DILIGENCE_VERSION,
    apply_hard_guards,
    clamp_words,
    parse_assessment,
    recursively_collect_urls,
    to_float,
    to_int,
)

from glasswing_portfolio import GLASSWING_PORTFOLIO_DOMAINS
load_dotenv(Path(__file__).resolve().parent / ".env")

MAX_TOTAL_FUNDING_M = float(os.getenv("MAX_TOTAL_FUNDING_USD_M", "8"))
MAX_INSTITUTIONAL_ROUNDS = int(os.getenv("MAX_INSTITUTIONAL_ROUNDS", "2"))


SYSTEM_PROMPT = """You are Glasswing Ventures' evidence-first early-stage diligence analyst.
Target: pre-seed/seed enterprise B2B technology in vertical AI, physical AI,
AI-adaptive infrastructure, intelligent enterprise defense, collaborative
intelligence, and next-generation compute.

Use the supplied first-party website evidence and one exact company+domain web
search. Separate confirmed facts from unknowns. Missing verification is NEVER
negative evidence. Confirmed non-U.S., consumer, agency, generic wrapper,
funding >$8M, Series A+, >2 institutional rounds, acquired/subsidiary/public,
or established/large status is a hard disqualifier.

Score each dimension independently from 0-5:
- thesis_fit: direct fit to a Glasswing category and market thesis
- enterprise_value: clear enterprise/government buyer, painful workflow, ROI
- product_depth: specific product, deployment, workflow, and user evidence
- technical_moat: proprietary technical depth, data/workflow advantage, defensibility
- stage_fit: credible early-stage profile; unknown stage is neutral, not zero
- evidence_confidence: quality/coverage of evidence only; this does not measure fit

A 5 requires unusually strong concrete evidence; 3 is credible; 0 means evidence
of no fit, not merely missing information. Recommend KEEP only for substantive
high-conviction fit. There is no MAYBE. Return concise evidence, not marketing copy.

Return ONLY these tags, with no prose before or after:
<decision>KEEP|REJECT</decision>
<thesis_fit>integer 0-5</thesis_fit>
<enterprise_value>integer 0-5</enterprise_value>
<product_depth>integer 0-5</product_depth>
<technical_moat>integer 0-5</technical_moat>
<stage_fit>integer 0-5</stage_fit>
<evidence_confidence>integer 0-5</evidence_confidence>
<category>official category</category>
<us_based>yes|no|unclear</us_based>
<funding_millions_usd>number or -1</funding_millions_usd>
<latest_round>none|pre_seed|seed|series_a_or_later|unknown</latest_round>
<institutional_rounds>integer or -1</institutional_rounds>
<company_maturity>very_early|early_stage|established|large_or_public|unknown</company_maturity>
<ownership_status>independent|acquired|subsidiary|merged|public|unclear</ownership_status>
<ownership_evidence>twenty words maximum</ownership_evidence>
<summary>sixteen words maximum</summary>
<reason>thirty words maximum</reason>
<concern>eighteen words maximum</concern>
<funding_evidence>eighteen words maximum</funding_evidence>
<location_evidence>eighteen words maximum</location_evidence>
<positive_evidence>thirty-five words maximum</positive_evidence>
<negative_evidence>twenty-five words maximum; "none" if none</negative_evidence>
<unknowns>twenty-five words maximum; "none" if none</unknowns>
"""

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
    "summary",
    "reason",
    "concern",
    "funding_evidence",
    "location_evidence",
    "positive_evidence",
    "negative_evidence",
    "unknowns",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(FILTERED / "candidates_master.csv"),
    )
    parser.add_argument(
        "--model",
        default=os.getenv("ANTHROPIC_DILIGENCE_MODEL", get_model()),
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--minimum-score",
        type=int,
        default=int(os.getenv("CLAUDE_MIN_PYTHON_SCORE", "51")),
    )
    parser.add_argument(
        "--percentage-cap",
        type=float,
        default=float(os.getenv("CLAUDE_PERCENTAGE_CAP", "1.0")),
    )
    parser.add_argument(
        "--max-calls",
        type=int,
        default=int(os.getenv("CLAUDE_MAX_CALLS", "20")),
    )
    parser.add_argument(
        "--exceptional-score",
        type=int,
        default=int(os.getenv("CLAUDE_EXCEPTIONAL_SCORE", "88")),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reclassify", action="store_true")
    parser.add_argument("--attempt-report", default="")
    return parser.parse_args()


def company_payload(row: dict) -> str:
    data = {
        "domain": clean(row.get("domain", row.get("registrable", ""))),
        "website": clean(row.get("final_url", "")),
        "company_name_hint": clean(row.get("page_title", ""))[:90],
        "website_description": clean(row.get("meta_description", ""))[:280],
        "website_excerpt": clean(row.get("homepage_excerpt", ""))[:800],
        "website_thesis_category": clean(row.get("rule_thesis_pillar", "")),
        "website_thesis_evidence": clean(row.get("rule_evidence", ""))[:220],
        "website_location_evidence": clean(row.get("location_evidence", ""))[:120],
        "website_maturity_evidence": clean(row.get("maturity_evidence", ""))[:120],
    }
    return (
        "Use one ownership-focused web search for the exact company name and domain. "
        "Check acquired, acquisition, merged, subsidiary, parent company, public status, "
        "funding, stage, and headquarters. Return only tags.\n"
        + json.dumps(data, ensure_ascii=False)
    )




def response_text_and_urls(response: Any) -> tuple[str, list[str]]:
    """Extract assistant text and cited/search URLs from Anthropic response."""
    texts: list[str] = []
    urls: set[str] = set()

    for block in getattr(response, "content", []):
        block_type = getattr(block, "type", "")

        if block_type == "text":
            text = getattr(block, "text", "")
            if text:
                texts.append(text)

        try:
            dumped = block.model_dump()
        except Exception:
            dumped = {}

        recursively_collect_urls(dumped, urls)

    return "\n".join(texts), sorted(urls)

def dry_assessment(row: dict) -> dict:
    return apply_hard_guards({
        "decision": "REJECT",
        "model_recommendation": "REJECT",
        "thesis_fit": 2,
        "enterprise_value": 2,
        "product_depth": 2,
        "technical_moat": 1,
        "stage_fit": 2,
        "evidence_confidence": 1,
        "category": clean(row.get("rule_thesis_pillar", "unclear")),
        "us_based": "yes" if row.get("location_status") == "US" else "unclear",
        "funding_millions_usd": -1.0,
        "latest_round": "unknown",
        "institutional_rounds": -1,
        "company_maturity": "unknown",
        "ownership_status": "unclear",
        "ownership_evidence": "",
        "product_summary": "Dry run",
        "reasoning": "Website gate passed",
        "concerns": "Funding not verified",
        "funding_evidence": "",
        "location_evidence": clean(row.get("location_evidence", "")),
        "diligence_sources": "",
        "positive_evidence": "Website gate passed",
        "negative_evidence": "none",
        "unknowns": "External diligence not run",
        "diligence_version": DILIGENCE_VERSION,
    }, max_total_funding_m=MAX_TOTAL_FUNDING_M,
       max_institutional_rounds=MAX_INSTITUTIONAL_ROUNDS)



class DiligenceResponseError(ValueError):
    """The API returned no usable decisive diligence result."""


def validate_response_text(text: str) -> None:
    """Reject empty/malformed responses as retryable call failures."""
    required = (
        "decision", "thesis_fit", "enterprise_value", "product_depth",
        "technical_moat", "stage_fit", "evidence_confidence", "summary",
        "reason", "positive_evidence", "negative_evidence", "unknowns",
    )
    missing = [
        name
        for name in required
        if not re.search(
            rf"<{name}>\s*[^<]+?\s*</{name}>",
            text or "",
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]
    decision_match = re.search(
        r"<decision>\s*([^<]+?)\s*</decision>",
        text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    decision = decision_match.group(1).strip().upper() if decision_match else ""
    if missing or decision not in {"KEEP", "REJECT"}:
        detail = f"missing tags: {','.join(missing)}" if missing else "invalid decision"
        raise DiligenceResponseError(detail)

async def classify(
    client: AsyncAnthropic,
    sem: asyncio.Semaphore,
    row: dict,
    model: str,
) -> dict:
    tools = [{
        "type": "web_search_20250305",
        "name": "web_search",
        "max_uses": 1,
        "allowed_callers": ["direct"],
        "user_location": {
            "type": "approximate",
            "country": "US",
            "timezone": "America/New_York",
        },
    }]

    async with sem:
        response = await client.messages.create(
            model=model,
            max_tokens=500,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": company_payload(row),
            }],
            tools=tools,
        )

    text, urls = response_text_and_urls(response)
    validate_response_text(text)
    assessment = parse_assessment(
        text,
        urls,
        max_total_funding_m=MAX_TOTAL_FUNDING_M,
        max_institutional_rounds=MAX_INSTITUTIONAL_ROUNDS,
    )

    # Very early companies may have no external coverage. Missing coverage is
    # a follow-up concern, not proof that the company is a bad investment.
    if not urls:
        assessment["concerns"] = (
            clean(assessment.get("concerns")) or "No external coverage"
        )

    return assessment


def load_currently_diligenced_domains() -> set[str]:
    domains: set[str] = set()

    for path in (
        LLM / "llm_approved_master.csv",
        LLM / "llm_denied_master.csv",
    ):
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            frame = pd.read_csv(path, dtype=str).fillna("")
        except pd.errors.EmptyDataError:
            continue

        if "domain" not in frame.columns:
            continue

        if "llm_diligence_version" in frame.columns:
            frame = frame[
                frame["llm_diligence_version"] == DILIGENCE_VERSION
            ]

        domains.update(stable_domain(value) for value in frame["domain"])

    return {domain for domain in domains if domain}


def prepare_input(
    path: Path,
    reclassify: bool,
    limit: int,
    minimum_score: int,
    percentage_cap: float,
    max_calls: int,
    exceptional_score: int,
) -> tuple[pd.DataFrame, dict[str, int | float]]:
    try:
        frame = pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), {
            "total_screened": 0,
            "eligible_above_score": 0,
            "quota": 0,
            "exceptional_score": exceptional_score,
        }

    if frame.empty:
        return frame, {
            "total_screened": 0,
            "eligible_above_score": 0,
            "quota": 0,
            "exceptional_score": exceptional_score,
        }

    total_rows_in_input = len(frame)

    if "domain" not in frame.columns:
        source = (
            frame["registrable"]
            if "registrable" in frame.columns
            else pd.Series("", index=frame.index)
        )
        frame["domain"] = source.map(stable_domain)
    else:
        frame["domain"] = frame["domain"].map(stable_domain)

    if "location_status" in frame.columns:
        frame = frame[
            frame["location_status"].isin(["US", "UNKNOWN"])
        ].copy()

    # Score is the sole positive selection threshold. Eligibility caps are
    # enforced again here so legacy uncapped rows do not consume paid calls.
    for boolean_column in (
        "predominantly_non_english",
        "placeholder_detected",
        "media_newsletter",
        "commodity_wrapper",
    ):
        if boolean_column in frame.columns:
            disqualified = frame[boolean_column].astype(str).str.lower().isin(
                {"true", "1", "yes"}
            )
            frame = frame[~disqualified].copy()
    if "routing_reason" in frame.columns:
        capped_reason = frame["routing_reason"].astype(str).str.contains(
            "agency_or_consultancy|consumer_b2c_product|mature_or_funded",
            case=False,
            regex=True,
        )
        frame = frame[~capped_reason].copy()
    if "content_source" in frame.columns:
        # Zone Universe's .com/.app zone crawl is orders of magnitude
        # larger than every other source combined (~170M candidate
        # domains) -- explicit user direction: that dataset never goes to
        # Claude, Python rule-based scoring is enough given the volume.
        # content_source reflects where the page content was actually
        # fetched from (zone_universe's own cache), which only ever
        # matches for .com/.app domains zone_universe has crawled -- a
        # CertStream .ai domain can never collide with that cache.
        frame = frame[
            frame["content_source"] != "zone_universe_cache"
        ].copy()

    score_column = (
        "diligence_rank_score"
        if "diligence_rank_score" in frame.columns
        else "rule_score"
    )
    stored_score = pd.to_numeric(
        frame.get(score_column, 0),
        errors="coerce",
    ).fillna(0)

    # Confirmed live: the dashboard shows a live rescore of the stored
    # text (score_text, recomputed at display time so it reflects the
    # current scoring logic without needing to re-persist every row) --
    # a real company visibly scoring well above minimum_score on that
    # live rescore was still "Not reviewed" because this gate only ever
    # looked at diligence_rank_score, the frozen value from whenever it
    # was first screened, which is a different formula and can be lower.
    # Taking the higher of the two means a domain never misses Claude
    # review just because scoring calibration has since improved. The
    # disqualification filters above (location/non-English/placeholder/
    # media/commodity/agency/consumer/mature) already ran before this and
    # are untouched -- this only affects which score clears the bar.
    website_copy = (
        frame.get("page_title", pd.Series("", index=frame.index)).astype(str)
        + " "
        + frame.get("meta_description", pd.Series("", index=frame.index)).astype(str)
        + " "
        + frame.get("homepage_excerpt", pd.Series("", index=frame.index)).astype(str)
    ).str.strip()
    live_rescore = website_copy.map(
        lambda text: score_text(text)[0] if text else 0
    )
    frame["_queue_score"] = pd.concat(
        [stored_score, live_rescore], axis=1
    ).max(axis=1)

    frame = frame[frame["_queue_score"] >= minimum_score].copy()

    if not reclassify:
        completed = load_currently_diligenced_domains()
        frame = frame[~frame["domain"].isin(completed)].copy()

    frame = frame.sort_values(
        ["_queue_score", "rule_score"],
        ascending=[False, False],
    )

    # Confirmed live: this used to re-read a separate hardcoded
    # FILTERED/all_screened.csv here regardless of what `path` (this same
    # function's actual input) pointed at. That file is deliberately never
    # refreshed in cloud/Postgres mode (bulk_compatibility_exports_enabled()
    # in filter.py keeps huge CSV mirrors off worker volumes there) -- so
    # total_screened silently read 0 there, zeroing percentage_quota and
    # therefore the real quota (below) every single time in production,
    # with no error anywhere. total_rows_in_input reflects the real size of
    # whatever candidates file this call was actually given.
    total_screened = total_rows_in_input

    percentage_quota = int(total_screened * max(percentage_cap, 0))
    quota = (
        min(max_calls, percentage_quota)
        if max_calls >= 0
        else percentage_quota
    )
    quota = max(0, quota)

    # Normal runs should never miss an exceptional top candidate merely because
    # the percentage quota rounded down to zero. Quick tests pass max_calls=0,
    # which intentionally disables all paid diligence.
    if (
        max_calls != 0
        and not frame.empty
        and float(frame.iloc[0]["_queue_score"]) >= exceptional_score
    ):
        quota = max(1, quota)

    if limit:
        quota = min(quota, limit)

    if max_calls >= 0:
        quota = min(quota, max_calls)

    selected = frame.head(quota).copy()

    return selected, {
        "total_screened": total_screened,
        "eligible_above_score": len(frame),
        "quota": quota,
        "minimum_score": minimum_score,
        "percentage_cap": percentage_cap,
        "max_calls": max_calls,
        "exceptional_score": exceptional_score,
    }


def reconcile_masters(approved: pd.DataFrame, denied: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    approved_master = upsert_csv(
        LLM / "llm_approved_master.csv", approved, key="domain"
    )
    denied_master = upsert_csv(
        LLM / "llm_denied_master.csv", denied, key="domain"
    )

    if not approved.empty and not denied_master.empty:
        denied_master = denied_master[
            ~denied_master["domain"].isin(approved["domain"])
        ].copy()

    if not denied.empty and not approved_master.empty:
        approved_master = approved_master[
            ~approved_master["domain"].isin(denied["domain"])
        ].copy()

    for frame in (approved_master, denied_master):
        if not frame.empty and "llm_fit_score" in frame.columns:
            frame["llm_fit_score"] = pd.to_numeric(
                frame["llm_fit_score"], errors="coerce"
            ).fillna(0)

    if not approved_master.empty and "llm_fit_score" in approved_master.columns:
        approved_master = approved_master.sort_values(
            "llm_fit_score", ascending=False
        )
    if not denied_master.empty and "llm_fit_score" in denied_master.columns:
        denied_master = denied_master.sort_values(
            "llm_fit_score", ascending=False
        )

    approved_master.to_csv(LLM / "llm_approved_master.csv", index=False)
    denied_master.to_csv(LLM / "llm_denied_master.csv", index=False)
    approved_master.to_csv(LLM / "ranked_master.csv", index=False)
    denied_master.to_csv(LLM / "rejected_master.csv", index=False)

    return approved_master, denied_master


def append_claude_buckets(approved: pd.DataFrame, denied: pd.DataFrame) -> tuple[int, int]:
    """Persist every decisive Claude result in its corresponding basket."""
    protected_portfolio = set(GLASSWING_PORTFOLIO_DOMAINS)

    def entries(frame: pd.DataFrame) -> list[tuple[str, int]]:
        result = []
        for _, row in frame.iterrows():
            domain = stable_domain(row.get("domain", ""))
            if domain and domain not in protected_portfolio:
                result.append((domain, int(row["llm_fit_score"])))
        return result

    yes_count = save_automated_basket_entries(
        entries(approved),
        "yes",
        source="claude",
        reason="claude_keep",
    )
    no_count = save_automated_basket_entries(
        entries(denied),
        "no",
        source="claude",
        reason="claude_reject",
    )
    return yes_count, no_count


async def run() -> None:
    options = parse_args()
    path = Path(options.csv_path).expanduser()
    if not path.exists():
        raise SystemExit(f"Input CSV not found: {path}")

    # Older versions persisted API failures and empty responses as REJECT.
    # Remove only those invalid automated rows before queue selection so the
    # affected domains (for example ability.ai) become eligible for retry.
    repaired = purge_invalid_claude_diligence()
    if repaired:
        print(f"Queued {repaired} invalid historical Claude result(s) for retry.")

    frame, queue_stats = prepare_input(
        path,
        options.reclassify,
        options.limit,
        options.minimum_score,
        options.percentage_cap,
        options.max_calls,
        options.exceptional_score,
    )
    rows = frame.to_dict("records")
    attempted_domains: list[str] = []

    print(
        "Diligence queue | "
        f"screened={queue_stats['total_screened']} | "
        f"above_score={queue_stats['eligible_above_score']} | "
        f"minimum_score={queue_stats['minimum_score']} | "
        f"quota={queue_stats['quota']} | "
        f"max_calls={queue_stats['max_calls']} | "
        f"exceptional_score={queue_stats['exceptional_score']}"
    )

    if not rows:
        print("No companies selected for paid external diligence.")
        return

    if options.dry_run:
        assessments = [dry_assessment(row) for row in rows]
    else:
        if not os.getenv("ANTHROPIC_API_KEY"):
            raise SystemExit("ANTHROPIC_API_KEY missing from .env.")

        client = AsyncAnthropic()
        sem = asyncio.Semaphore(options.workers)
        attempted_domains = [
            stable_domain(row.get("domain", "")) for row in rows
        ]
        raw_results = await asyncio.gather(*[
            classify(client, sem, row, options.model)
            for row in rows
        ], return_exceptions=True)

        successful_rows = []
        assessments = []
        failures: list[tuple[str, Exception]] = []
        for row, result in zip(rows, raw_results):
            if isinstance(result, Exception):
                failures.append((stable_domain(row.get("domain", "")), result))
            else:
                successful_rows.append(row)
                assessments.append(result)

        for domain, exc in failures:
            print(
                "Diligence retry queued | "
                f"domain={domain or 'unknown'} | "
                f"error={type(exc).__name__}: {str(exc)[:180]}",
            )
        rows = successful_rows

        if options.attempt_report:
            atomic_write_json(Path(options.attempt_report), {
                "attempted_domains": [d for d in attempted_domains if d],
                "successful_domains": [
                    stable_domain(row.get("domain", "")) for row in rows
                ],
                "failed_domains": [domain for domain, _ in failures if domain],
            })

    if not rows:
        print("No valid Claude decisions persisted; failed calls remain retryable.")
        return

    merged: list[dict] = []
    for row, assessment in zip(rows, assessments):
        merged.append({
            **row,
            **{
                f"llm_{key}": value
                for key, value in assessment.items()
            },
            "llm_company_name": clean(row.get("page_title", ""))[:80]
            or clean(row.get("domain", "")),
            "llm_model": "DRY_RUN" if options.dry_run else options.model,
        })

    output = pd.DataFrame(merged)
    output["llm_fit_score"] = pd.to_numeric(
        output["llm_fit_score"], errors="coerce"
    ).fillna(0)

    # No MAYBE: confirmed hard guards have already converted factual
    # disqualifiers to REJECT. Unverified facts do not manufacture a denial.
    approved = output[
        output["llm_decision"].eq("KEEP")
    ].copy()

    denied = output[~output.index.isin(approved.index)].copy()
    denied.loc[denied["llm_decision"].ne("REJECT"), "llm_decision"] = "REJECT"

    final_results = pd.concat(
        [approved, denied], ignore_index=True, sort=False
    )
    upsert_claude_diligence(final_results.to_dict(orient="records"))
    # Also repairs labels from calls completed before cross-service baskets
    # became database-backed (including the existing weekend backlog).
    backfill_claude_training_labels()
    approved_master, denied_master = reconcile_masters(approved, denied)

    # Every result is decisive and is appended to the matching silver-label
    # basket. Protected human/portfolio truth is never overwritten.
    append_claude_buckets(approved, denied)

    print(
        f"Web diligence searches: {len(rows)} | "
        f"New approved: {len(approved)} | "
        f"New denied: {len(denied)} | "
        f"Approved master: {len(approved_master)} | "
        f"Denied master: {len(denied_master)}"
    )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
