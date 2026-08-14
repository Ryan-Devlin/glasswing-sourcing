#!/usr/bin/env python3
"""Build compact, leakage-resistant datasets for small local models."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from classification_store import CLASSIFICATION_DB, connect_store
from db_backend import using_postgres

from common import (
    FILTERED,
    LLM,
    NO_BASKET_DATA,
    YES_BASKET_DATA,
    clean,
    label_metadata_path,
    read_basket_data,
    read_label_metadata,
    stable_domain,
)
from glasswing_portfolio import GLASSWING_PORTFOLIO_DOMAINS

TRAINING = FILTERED.parent / "training"
MODEL_READY_CSV = TRAINING / "glasswing_model_ready.csv"
WEAK_LABELS_CSV = TRAINING / "glasswing_weak_labels.csv"
MODEL_READY_JSONL = TRAINING / "glasswing_instruction_tuning.jsonl"
EXCLUSIONS_CSV = TRAINING / "glasswing_training_exclusions.csv"
MANIFEST_JSON = TRAINING / "manifest.json"
DEFAULT_FILTERED_ROOT = FILTERED.resolve()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _short(value: object, limit: int) -> str:
    return clean(value)[:limit]


def _split(domain: str) -> str:
    """Stable domain split prevents the same company leaking across sets."""
    bucket = int(hashlib.sha256(domain.encode("utf-8")).hexdigest()[:8], 16) % 100
    if bucket < 80:
        return "train"
    if bucket < 90:
        return "validation"
    return "test"


def _latest_evidence() -> pd.DataFrame:
    # Tests, maintenance jobs, and future cloud workers can point FILTERED at an
    # isolated dataset.  Only consult the default canonical database when this
    # module is using the default data root; otherwise we could mix two stores.
    use_canonical_store = FILTERED.resolve() == DEFAULT_FILTERED_ROOT
    if use_canonical_store and (using_postgres() or Path(CLASSIFICATION_DB).exists()):
        records: list[dict[str, object]] = []
        with connect_store(CLASSIFICATION_DB, read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT domain,company_name,final_url,description,excerpt,
                       routing_bucket,routing_reason,fit_score,
                       confidence_score,priority_score,record_json,updated_at
                FROM classifications
                """
            ).fetchall()
        for row in rows:
            plain = dict(row)
            try:
                record = json.loads(plain.pop("record_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                record = {}
            if not isinstance(record, dict):
                record = {}
            record.update({
                "domain": plain["domain"],
                "page_title": (
                    record.get("page_title") or plain["company_name"]
                ),
                "final_url": (
                    record.get("final_url") or plain["final_url"]
                ),
                "meta_description": (
                    record.get("meta_description") or plain["description"]
                ),
                "homepage_excerpt": (
                    record.get("homepage_excerpt") or plain["excerpt"]
                ),
                "routing_bucket": plain["routing_bucket"],
                "routing_reason": plain["routing_reason"],
                "diligence_rank_score": plain["fit_score"],
                "confidence_score": plain["confidence_score"],
                "priority_score": plain["priority_score"],
                "observed_at": plain["updated_at"],
            })
            records.append(record)
        if records:
            return pd.DataFrame(records)
    screened = _read_csv(FILTERED / "all_screened.csv")
    if screened.empty:
        return pd.DataFrame(columns=["domain"])
    if "domain" not in screened:
        screened["domain"] = screened.get("registrable", "")
    screened["domain"] = screened["domain"].map(stable_domain)
    screened = screened[screened["domain"].ne("")].copy()
    usable = (
        screened.get("homepage_excerpt", pd.Series("", index=screened.index))
        .astype(str).str.strip().ne("")
        & ~screened.get("fetch_error", pd.Series("", index=screened.index))
        .astype(str).str.contains("TimeoutError", na=False)
    )
    screened["_usable"] = usable
    screened = screened.sort_values(
        ["domain", "_usable", "observed_at"],
        ascending=[True, True, True],
    )
    return screened.drop_duplicates("domain", keep="last")


def _claude_provenance() -> dict[str, str]:
    decisions: dict[str, str] = {}
    for path in (LLM / "llm_approved_master.csv", LLM / "llm_denied_master.csv"):
        frame = _read_csv(path)
        for _, row in frame.iterrows():
            domain = stable_domain(row.get("domain", ""))
            decision = clean(row.get("llm_decision", "")).upper()
            if domain and decision in {"KEEP", "REJECT"}:
                decisions[domain] = decision
    return decisions


def backfill_label_metadata() -> int:
    """Conservatively assign provenance to basket rows lacking a ledger row."""
    existing = read_label_metadata()
    known = set(existing["domain"]) if not existing.empty else set()
    portfolio = {stable_domain(value) for value in GLASSWING_PORTFOLIO_DOMAINS}
    claude = _claude_provenance()
    additions = []
    now = datetime.now(timezone.utc).isoformat()
    for label, path in (("yes", YES_BASKET_DATA), ("no", NO_BASKET_DATA)):
        for _, row in read_basket_data(path).iterrows():
            domain = stable_domain(row["domain_url"])
            if not domain or domain in known:
                continue
            expected = "KEEP" if label == "yes" else "REJECT"
            if domain in portfolio and label == "yes":
                source, reason = "glasswing_portfolio", "verified_portfolio_company"
            elif claude.get(domain) == expected:
                source, reason = "claude", "matching_decisive_claude_result"
            else:
                source, reason = "weak_rule", "legacy_automatic_or_unverified_label"
            additions.append({
                "domain": domain,
                "label": label,
                "rating": int(row["rating"]),
                "source": source,
                "reason": reason,
                "labeled_at": now,
            })
            known.add(domain)
    if additions:
        combined = pd.concat([existing, pd.DataFrame(additions)], ignore_index=True)
        path = label_metadata_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(path, index=False)
    return len(additions)


def _input_text(row: pd.Series) -> str:
    parts = [
        f"Company: {_short(row.get('page_title', ''), 100)}",
        f"Website description: {_short(row.get('meta_description', ''), 300)}",
        f"Website evidence: {_short(row.get('homepage_excerpt', ''), 800)}",
        f"Glasswing category evidence: {_short(row.get('rule_evidence', ''), 220)}",
        f"Location: {_short(row.get('location_status', ''), 20)}; "
        f"{_short(row.get('location_evidence', ''), 100)}",
        f"Maturity: {_short(row.get('maturity_evidence', ''), 120)}",
        f"Risk evidence: {_short(row.get('denial_evidence', ''), 160)}",
    ]
    return "\n".join(part for part in parts if not part.endswith(": "))[:1800]


def build_training_dataset(
    *, trusted_only: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    baskets = []
    for label, path in (("yes", YES_BASKET_DATA), ("no", NO_BASKET_DATA)):
        frame = read_basket_data(path)
        frame["domain"] = frame["domain_url"].map(stable_domain)
        frame["label"] = label
        baskets.append(frame)
    labels = pd.concat(baskets, ignore_index=True).drop_duplicates(
        "domain", keep="last"
    )
    evidence = _latest_evidence()
    merged = labels.merge(evidence, on="domain", how="left", suffixes=("", "_web"))

    excerpt = merged.get("homepage_excerpt", pd.Series("", index=merged.index))
    title = merged.get("page_title", pd.Series("", index=merged.index))
    usable = excerpt.astype(str).str.strip().ne("") | title.astype(str).str.strip().ne("")
    excluded = merged[~usable].copy()
    excluded["exclusion_reason"] = "no_usable_website_evidence"
    ready = merged[usable].copy()

    portfolio = {stable_domain(value) for value in GLASSWING_PORTFOLIO_DOMAINS}
    claude = _claude_provenance()
    metadata = read_label_metadata()
    metadata_by_domain = (
        metadata.set_index("domain").to_dict("index") if not metadata.empty else {}
    )
    def provenance(row: pd.Series) -> str:
        saved = metadata_by_domain.get(row["domain"], {})
        if clean(saved.get("label", "")) == row["label"]:
            return clean(saved.get("source", "")) or "weak_rule"
        if row["domain"] in portfolio:
            return "glasswing_portfolio"
        expected = "KEEP" if row["label"] == "yes" else "REJECT"
        if claude.get(row["domain"]) == expected:
            return "claude"
        return "weak_rule"

    if ready.empty:
        # pandas' `.apply(axis=1)` on a zero-row frame probes the callable
        # against a dummy row to infer a return shape; every callable below
        # indexes into row["..."], so the probe raises and apply falls back
        # to returning a DataFrame instead of a Series, breaking the
        # `ready["col"] = ready.apply(...)` assignments below. Skip straight
        # to empty typed columns instead -- there is nothing to compute.
        for column in (
            "split", "label_source", "label_reason", "trust_tier",
            "sample_weight", "input_text", "target_json",
        ):
            ready[column] = pd.Series(dtype=object)
    else:
        ready["label_source"] = ready.apply(provenance, axis=1)
        ready["label_reason"] = ready["domain"].map(
            lambda domain: clean(metadata_by_domain.get(domain, {}).get("reason", ""))
        )
        ready["trust_tier"] = ready["label_source"].map({
            "human": "gold",
            "manual": "gold",
            "glasswing_dashboard": "gold",
            "ai_search_page": "gold",
            "zone_universe_page": "gold",
            "glasswing_portfolio": "gold",
        }).fillna("weak")
        ready["rating"] = pd.to_numeric(ready["rating"], errors="coerce").fillna(0).clip(0, 100).astype(int)
        ready["split"] = ready["domain"].map(_split)
        ready["input_text"] = ready.apply(_input_text, axis=1)
        label_counts = ready["label"].value_counts()
        ready["sample_weight"] = ready["label"].map(
            lambda label: round(
                min(6.0, len(ready) / max(2 * int(label_counts.get(label, 1)), 1)),
                3,
            )
        )
        ready["target_json"] = ready.apply(
            lambda row: json.dumps(
                {"label": row["label"], "rating": int(row["rating"])},
                separators=(",", ":"),
            ),
            axis=1,
        )
    columns = [
        "domain", "domain_url", "split", "label", "rating", "label_source",
        "trust_tier", "label_reason",
        "sample_weight", "input_text", "target_json",
    ]
    ready = ready[columns].sort_values(["split", "label", "domain"])
    if trusted_only:
        ready = ready[ready["trust_tier"].isin(["gold", "silver"])].copy()
    excluded = excluded[["domain", "domain_url", "label", "rating", "exclusion_reason"]]
    tuning = balanced_instruction_frame(ready)
    manifest = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model_ready": len(ready),
        "excluded": len(excluded),
        "labels": ready["label"].value_counts().to_dict(),
        "splits": ready["split"].value_counts().to_dict(),
        "sources": ready["label_source"].value_counts().to_dict(),
        "trust_tiers": ready["trust_tier"].value_counts().to_dict(),
        "instruction_tuning_examples": len(tuning),
        "instruction_tuning_labels": tuning["label"].value_counts().to_dict(),
        "input_character_limit": 1800,
        "targets": ["label", "rating"],
    }
    return ready.reset_index(drop=True), excluded.reset_index(drop=True), manifest


def build_active_review_queue(limit: int = 250) -> pd.DataFrame:
    evidence = _latest_evidence()
    if evidence.empty:
        return pd.DataFrame()
    metadata = read_label_metadata()
    trusted = set(
        metadata[
            metadata["source"].isin(["human", "glasswing_portfolio"])
        ]["domain"]
    ) if not metadata.empty else set()
    evidence = evidence[~evidence["domain"].isin(trusted)].copy()
    usable = evidence.get("homepage_excerpt", "").astype(str).str.strip().ne("")
    evidence = evidence[usable].copy()
    score = pd.to_numeric(
        evidence.get("diligence_rank_score", evidence.get("rule_score", 0)),
        errors="coerce",
    ).fillna(0).clip(0, 100)
    confidence = pd.to_numeric(
        evidence.get("confidence_score", 0),
        errors="coerce",
    ).fillna(0).clip(0, 100)
    routing = evidence.get(
        "routing_bucket",
        pd.Series("", index=evidence.index),
    ).astype(str)
    claude = evidence.get(
        "llm_decision",
        pd.Series("", index=evidence.index),
    ).astype(str).str.upper()
    probability = pd.to_numeric(
        evidence.get("preference_yes_probability", 0.5),
        errors="coerce",
    ).fillna(0.5).clip(0, 1)
    rule_positive = routing.eq("priority")
    learned_positive = probability.ge(0.65)
    decisive_disagreement = (
        (rule_positive & claude.eq("REJECT"))
        | (~rule_positive & claude.eq("KEEP"))
        | (rule_positive != learned_positive)
    )
    why = pd.Series("Borderline Glasswing fit", index=evidence.index)
    why.loc[routing.eq("priority")] = "Audit suggested Investable"
    why.loc[routing.eq("review")] = "Resolve Needs Review"
    why.loc[routing.isin(["insufficient", "ghost"])] = (
        "High-scoring evidence held back"
    )
    why.loc[decisive_disagreement] = "Models disagree"

    # Active learning favors disagreement and plausible positives because the
    # human set currently contains far fewer Yes examples than No examples.
    evidence["_review_priority"] = (
        100 - (score - 55).abs()
        + score.ge(40).astype(int) * 30
        + rule_positive.astype(int) * 45
        + decisive_disagreement.astype(int) * 80
        + confidence.ge(60).astype(int) * 15
        + evidence.get(
            "clear_enterprise_idea",
            pd.Series("", index=evidence.index),
        ).astype(str).str.lower()
        .isin(["true", "1", "yes"]).astype(int) * 20
    )
    yes = read_basket_data(YES_BASKET_DATA)
    no = read_basket_data(NO_BASKET_DATA)
    label_map = {
        stable_domain(row["domain_url"]): ("yes", int(row["rating"]))
        for _, row in yes.iterrows()
    }
    label_map.update({
        stable_domain(row["domain_url"]): ("no", int(row["rating"]))
        for _, row in no.iterrows()
    })
    queue = pd.DataFrame(index=evidence.index)
    queue["Review"] = False
    queue["Domain"] = evidence["domain"]
    queue["Company"] = evidence.get("page_title", evidence["domain"])
    queue["Current Label"] = evidence["domain"].map(
        lambda domain: label_map.get(domain, ("unlabeled", 0))[0]
    )
    queue["Python suggestion"] = routing.map({
        "priority": "Investable",
        "review": "Needs Review",
        "comp": "Potential Comp",
        # Legacy stored values from before 2026-08-08 -- route_company
        # only produces "review" or "reject" now.
        "insufficient": "Needs Review",
        "ghost": "Needs Review",
    }).fillna(routing.str.replace("_", " ").str.title())
    queue["Why review"] = why
    queue["Claude"] = claude.replace("", "Not reviewed")
    queue["Decision"] = ""
    queue["Rating"] = [
        label_map.get(domain, ("", int(score.loc[index])))[1]
        for index, domain in evidence["domain"].items()
    ]
    queue["Reason"] = ""
    queue["Description"] = evidence.get("meta_description", "")
    queue["Website"] = evidence.get("final_url", "")
    queue["Suggested Score"] = score.astype(int)
    queue["Evidence confidence"] = confidence.astype(int)
    queue["_priority"] = evidence["_review_priority"]
    return queue.sort_values("_priority", ascending=False).head(limit).drop(
        columns="_priority"
    ).reset_index(drop=True)


def balanced_instruction_frame(ready: pd.DataFrame) -> pd.DataFrame:
    """Keep every positive and at most two negatives per positive per split."""
    selected = []
    for _, split_frame in ready.groupby("split", sort=False):
        positives = split_frame[split_frame["label"].eq("yes")]
        negatives = split_frame[split_frame["label"].eq("no")]
        negative_limit = min(len(negatives), max(1, len(positives) * 2))
        selected.extend([positives, negatives.head(negative_limit)])
    if not selected:
        return ready.head(0).copy()
    return pd.concat(selected, ignore_index=True).sort_values(
        ["split", "label", "domain"]
    )


def write_training_exports() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    all_ready, excluded, _ = build_training_dataset()
    ready = all_ready[all_ready["trust_tier"].eq("gold")].copy()
    tuning = balanced_instruction_frame(ready)
    manifest = {
        "schema_version": 2,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "trusted_model_ready": len(ready),
        "weak_examples_preserved": int((all_ready["trust_tier"] == "weak").sum()),
        "trusted_labels": ready["label"].value_counts().to_dict(),
        "trusted_splits": ready["split"].value_counts().to_dict(),
        "trusted_sources": ready["label_source"].value_counts().to_dict(),
        "instruction_tuning_examples": len(tuning),
        "readiness_targets": {"yes": 300, "no": 600, "manual_test": 150},
        "ready_to_train": bool(
            (ready["label"] == "yes").sum() >= 300
            and (ready["label"] == "no").sum() >= 600
        ),
        "input_character_limit": 1800,
        "targets": ["label", "rating"],
    }
    TRAINING.mkdir(parents=True, exist_ok=True)
    ready.to_csv(MODEL_READY_CSV, index=False)
    all_ready.to_csv(WEAK_LABELS_CSV, index=False)
    excluded.to_csv(EXCLUSIONS_CSV, index=False)
    with MODEL_READY_JSONL.open("w", encoding="utf-8") as handle:
        for _, row in tuning.iterrows():
            record = {
                "messages": [
                    {
                        "role": "system",
                        "content": "Classify Glasswing Ventures investment fit and score it 0-100.",
                    },
                    {"role": "user", "content": row["input_text"]},
                    {"role": "assistant", "content": row["target_json"]},
                ],
                "metadata": {
                    "domain": row["domain"],
                    "split": row["split"],
                    "source": row["label_source"],
                },
            }
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    MANIFEST_JSON.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return ready, excluded, manifest


if __name__ == "__main__":
    backfill_label_metadata()
    _, _, summary = write_training_exports()
    print(json.dumps(summary, indent=2))
