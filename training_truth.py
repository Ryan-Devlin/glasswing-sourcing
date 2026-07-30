"""Canonical, human-authored training labels.

Python routing and LLM decisions are useful product signals, but they are not
ground truth.  This module keeps the compatibility Yes/No basket files limited
to explicit human decisions plus verified Glasswing portfolio positives.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from classification_store import CLASSIFICATION_DB, connect_store, utc_now
from common import (
    MANUAL,
    NO_BASKET_DATA,
    YES_BASKET_DATA,
    atomic_write_csv,
    clean,
    label_metadata_path,
    read_basket_data,
    read_label_metadata,
    stable_domain,
)


GROUND_TRUTH_CSV = MANUAL / "human_ground_truth.csv"
OVERRIDES_CSV = MANUAL / "domain_overrides.csv"
HUMAN_SOURCES = {
    "human",
    "manual",
    "glasswing_dashboard",
    "ai_search_page",
    "zone_universe_page",
    "legacy_manual_override",
}
TRUSTED_SOURCES = HUMAN_SOURCES | {"glasswing_portfolio"}
def is_human_source(value: object) -> bool:
    return clean(value).lower() in HUMAN_SOURCES


def is_trusted_source(value: object) -> bool:
    return clean(value).lower() in TRUSTED_SOURCES


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def protected_training_domains() -> set[str]:
    """Return domains whose human/portfolio labels must never be purged."""
    protected: set[str] = set()
    metadata = read_label_metadata()
    if not metadata.empty:
        protected.update(
            metadata.loc[
                metadata["source"].map(is_trusted_source),
                "domain",
            ].map(stable_domain)
        )
    overrides = _read_csv(OVERRIDES_CSV)
    if not overrides.empty and "domain" in overrides:
        protected.update(overrides["domain"].map(stable_domain))
    if Path(CLASSIFICATION_DB).exists():
        with connect_store(CLASSIFICATION_DB, read_only=True) as conn:
            protected.update(
                stable_domain(row["domain"])
                for row in conn.execute(
                    "SELECT domain,source FROM manual_decisions"
                )
                if is_human_source(row["source"])
            )
    return {domain for domain in protected if domain}


def _basket_ratings() -> dict[str, int]:
    ratings: dict[str, int] = {}
    for path in (YES_BASKET_DATA, NO_BASKET_DATA):
        for _, row in read_basket_data(path).iterrows():
            domain = stable_domain(row.get("domain_url", ""))
            if domain:
                ratings[domain] = int(row.get("rating", 0))
    return ratings


def rebuild_human_ground_truth(
    *,
    classification_db: Path | str = CLASSIFICATION_DB,
) -> dict[str, int]:
    """Remove automatic basket labels and rebuild explicit ground truth."""
    ratings = _basket_ratings()
    records: dict[str, dict[str, object]] = {}

    # Portfolio examples are verified positives, but a later human decision
    # is allowed to supersede one.
    metadata = read_label_metadata()
    for _, row in metadata.iterrows():
        source = clean(row.get("source", "")).lower()
        if source != "glasswing_portfolio":
            continue
        domain = stable_domain(row.get("domain", ""))
        if domain:
            records[domain] = {
                "domain": domain,
                "label": "yes",
                "rating": int(pd.to_numeric(row.get("rating", 100), errors="coerce") or 100),
                "source": "glasswing_portfolio",
                "reason": clean(row.get("reason", "")) or "verified_portfolio_company",
                "labeled_at": clean(row.get("labeled_at", "")) or utc_now(),
            }

    # Existing human metadata is authoritative.
    for _, row in metadata.iterrows():
        if not is_human_source(row.get("source", "")):
            continue
        domain = stable_domain(row.get("domain", ""))
        label = clean(row.get("label", "")).lower()
        if domain and label in {"yes", "no"}:
            records[domain] = {
                "domain": domain,
                "label": label,
                "rating": int(pd.to_numeric(row.get("rating", ratings.get(domain, 0)), errors="coerce") or 0),
                "source": "human",
                "reason": clean(row.get("reason", "")) or "manual_training_label",
                "labeled_at": clean(row.get("labeled_at", "")) or utc_now(),
            }

    # The durable override ledger captures older UI decisions that predate
    # provenance metadata. Treat every row here as an explicit human action.
    overrides = _read_csv(OVERRIDES_CSV)
    if not overrides.empty and "domain" in overrides:
        overrides = overrides.drop_duplicates("domain", keep="last")
        for _, row in overrides.iterrows():
            target = clean(row.get("override_target", ""))
            label = {
                "Investable Candidate": "yes",
                "Hard Denied": "no",
            }.get(target)
            domain = stable_domain(row.get("domain", ""))
            if not domain or not label:
                continue
            default_rating = 70 if label == "yes" else 0
            records[domain] = {
                "domain": domain,
                "label": label,
                "rating": max(0, min(100, ratings.get(domain, default_rating))),
                "source": "human",
                "reason": (
                    clean(row.get("override_note", ""))
                    or f"manual_{label}_override"
                ),
                "labeled_at": clean(row.get("updated_at", "")) or utc_now(),
            }

    columns = [
        "domain", "label", "rating", "source", "reason", "labeled_at",
    ]
    ground_truth = pd.DataFrame(records.values(), columns=columns)
    if not ground_truth.empty:
        ground_truth["rating"] = (
            pd.to_numeric(ground_truth["rating"], errors="coerce")
            .fillna(0).clip(0, 100).astype(int)
        )
        ground_truth = ground_truth.sort_values(
            ["source", "label", "domain"]
        ).reset_index(drop=True)

    yes = ground_truth[ground_truth["label"].eq("yes")].copy()
    no = ground_truth[ground_truth["label"].eq("no")].copy()
    atomic_write_csv(
        YES_BASKET_DATA,
        pd.DataFrame({
            "domain_url": yes["domain"].map(lambda value: f"https://{value}"),
            "rating": yes["rating"],
        }),
    )
    atomic_write_csv(
        NO_BASKET_DATA,
        pd.DataFrame({
            "domain_url": no["domain"].map(lambda value: f"https://{value}"),
            "rating": no["rating"],
        }),
    )
    atomic_write_csv(label_metadata_path(), ground_truth[columns])
    atomic_write_csv(GROUND_TRUTH_CSV, ground_truth[columns])

    database = Path(classification_db)
    manual_decisions = 0
    if database.exists():
        with connect_store(database) as conn:
            conn.execute("BEGIN IMMEDIATE")
            # This table is reserved for actions made by a person. Historical
            # jobs incorrectly wrote automated sale detections here.
            conn.execute(
                "DELETE FROM manual_decisions WHERE lower(source) NOT IN "
                "(?,?,?,?,?,?)",
                tuple(sorted(HUMAN_SOURCES)),
            )
            conn.execute(
                "DELETE FROM movement_log WHERE lower(source) LIKE 'automated%' "
                "OR lower(source) LIKE 'crawler%' "
                "OR lower(source) LIKE 'python%'"
            )
            for row in ground_truth.to_dict(orient="records"):
                target_label = (
                    "Investable Candidate"
                    if row["label"] == "yes"
                    else "Hard Denied"
                )
                target_bucket = "priority" if row["label"] == "yes" else "reject"
                existing = conn.execute(
                    "SELECT record_json FROM classifications WHERE domain=?",
                    (row["domain"],),
                ).fetchone()
                if existing is not None:
                    try:
                        record_json = json.loads(
                            existing["record_json"] or "{}"
                        )
                    except (TypeError, json.JSONDecodeError):
                        record_json = {}
                    record_json.update({
                        "routing_bucket": target_bucket,
                        "routing_reason": (
                            f"human_ground_truth_{row['label']}"
                            if row["source"] == "human"
                            else "verified_portfolio_positive"
                        ),
                        "manual_override": row["source"] == "human",
                        "manual_override_target": (
                            target_label
                            if row["source"] == "human"
                            else ""
                        ),
                        "training_basket": row["label"],
                        "human_rating": int(row["rating"]),
                    })
                    conn.execute(
                        """
                        UPDATE classifications SET
                            routing_bucket=?,
                            routing_reason=?,
                            priority_score=?,
                            manual_override=?,
                            manual_override_target=?,
                            record_json=?,
                            updated_at=?
                        WHERE domain=?
                        """,
                        (
                            target_bucket,
                            record_json["routing_reason"],
                            (
                                max(55, int(row["rating"]))
                                if row["label"] == "yes"
                                else 0
                            ),
                            int(row["source"] == "human"),
                            (
                                target_label
                                if row["source"] == "human"
                                else ""
                            ),
                            json.dumps(
                                record_json,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            utc_now(),
                            row["domain"],
                        ),
                    )
                if row["source"] != "human":
                    continue
                conn.execute(
                    """
                    INSERT INTO manual_decisions(
                        domain,target_label,target_bucket,objective_score,
                        note,source,decided_at
                    ) VALUES (?,?,?,?,?,'human',?)
                    ON CONFLICT(domain) DO UPDATE SET
                        target_label=excluded.target_label,
                        target_bucket=excluded.target_bucket,
                        objective_score=excluded.objective_score,
                        note=excluded.note,
                        source='human',
                        decided_at=excluded.decided_at
                    """,
                    (
                        row["domain"],
                        target_label,
                        target_bucket,
                        int(row["rating"]),
                        row["reason"],
                        row["labeled_at"],
                    ),
                )
                manual_decisions += 1
            conn.commit()

    return {
        "ground_truth": len(ground_truth),
        "human": int(ground_truth["source"].eq("human").sum()),
        "portfolio": int(
            ground_truth["source"].eq("glasswing_portfolio").sum()
        ),
        "yes": len(yes),
        "no": len(no),
        "manual_decisions": manual_decisions,
    }
