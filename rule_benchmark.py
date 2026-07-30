#!/usr/bin/env python3
"""Evaluate scraper rules on saved labeled websites without network calls."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import pandas as pd

import filter as screening
from common import FILTERED


DEFAULT_LABELS = (
    Path(__file__).resolve().parent
    / "data"
    / "training"
    / "glasswing_model_ready.csv"
)


def usable_labels(path: Path) -> pd.DataFrame:
    labels = pd.read_csv(path, dtype=str).fillna("")
    placeholder = (
        labels["input_text"].str.lower().str.count(r"\bnan\b").ge(3)
    )
    return labels[~placeholder].copy()


async def evaluate(labels_path: Path) -> pd.DataFrame:
    labels = usable_labels(labels_path)
    master = pd.read_csv(
        FILTERED / "all_screened.csv",
        dtype=str,
        low_memory=False,
    ).fillna("")
    source = labels.merge(master, on="domain", how="inner")
    saved_pages = {
        row["domain"]: screening.Fetch(
            url=row.get("final_url", "") or f"https://{row['domain']}",
            status=int(row["http_status"]) if row.get("http_status", "").isdigit() else 200,
            title=row.get("page_title", ""),
            description=row.get("meta_description", ""),
            text=row.get("homepage_excerpt", ""),
            error=row.get("fetch_error", ""),
            pages_scraped=int(float(row.get("pages_scraped", 1) or 1)),
            meaningful_internal_links=int(float(
                row.get("meaningful_internal_links", 0) or 0
            )),
        )
        for _, row in source.iterrows()
    }

    original_fetch = screening.fetch

    async def saved_fetch(_session, domain, _timeout, homepage_only=False):
        return saved_pages[domain]

    screening.fetch = saved_fetch
    try:
        records = []
        semaphore = asyncio.Semaphore(1)
        for _, row in source.iterrows():
            result = await screening.process(
                {"domain": row["domain"]},
                session=None,
                sem=semaphore,
                timeout=1,
                homepage_only=True,
            )
            result["truth"] = row["label"]
            result["split"] = row["split"]
            records.append(result)
    finally:
        screening.fetch = original_fetch

    output = pd.DataFrame(records)
    return screening.apply_run_preferences(
        output,
        screening.load_run_preferences(),
    )


def decision(bucket: str) -> str:
    if bucket == "priority":
        return "Investable"
    if bucket == "reject":
        return "Hard Denied"
    return "Unresolved"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--labels", type=Path, default=DEFAULT_LABELS)
    args = parser.parse_args()
    output = asyncio.run(evaluate(args.labels))
    output["decision"] = output["routing_bucket"].map(decision)

    table = pd.crosstab(
        output["truth"],
        output["decision"],
    ).reindex(
        index=["yes", "no"],
        columns=["Investable", "Hard Denied", "Unresolved"],
        fill_value=0,
    )
    print(table.to_string())
    print()

    yes = output[output["truth"].eq("yes")]
    no = output[output["truth"].eq("no")]
    promoted = output[output["decision"].eq("Investable")]
    print(f"Usable labeled websites: {len(output)}")
    print(
        "Yes recognition: "
        f"{yes['decision'].eq('Investable').sum()}/{len(yes)}"
    )
    print(
        "No hard-denial recall: "
        f"{no['decision'].eq('Hard Denied').sum()}/{len(no)}"
    )
    precision = (
        promoted["truth"].eq("yes").mean()
        if not promoted.empty else 0.0
    )
    print(f"Investable precision: {precision:.1%}")
    print(
        "Unresolved: "
        f"{output['decision'].eq('Unresolved').sum()}/{len(output)} "
        f"({output['decision'].eq('Unresolved').mean():.1%})"
    )

    mistakes = output[
        ((output["truth"] == "yes") & (output["decision"] != "Investable"))
        | ((output["truth"] == "no") & (output["decision"] == "Investable"))
    ][
        ["domain", "truth", "decision", "rule_score", "routing_reason"]
    ]
    if not mistakes.empty:
        print("\nRecognition errors:")
        print(mistakes.sort_values(
            ["truth", "rule_score"],
            ascending=[False, False],
        ).to_string(index=False))


if __name__ == "__main__":
    main()
