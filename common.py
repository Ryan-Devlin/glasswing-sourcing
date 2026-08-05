from __future__ import annotations

import os
import re
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
AI_PROJECT_ROOT = ROOT / "ai-domain-search-engine"
ZONE_PROJECT_ROOT = ROOT / "zone-universe"
DATA = Path(
    os.getenv("GLASSWING_DATA_ROOT", str(ROOT / "data"))
).expanduser().resolve()
RAW = DATA / "raw"
FILTERED = DATA / "filtered"
LLM = DATA / "llm"
REVIEWS = DATA / "reviews"
MANUAL = DATA / "manual"
YES_BASKET_DATA = MANUAL / "yes_basket.csv"
NO_BASKET_DATA = MANUAL / "no_basket.csv"
LOGS = Path(
    os.getenv("GLASSWING_LOG_ROOT", str(ROOT / "logs"))
).expanduser().resolve()
RUN_STATUS = LOGS / "pipeline_status.json"
RUN_LOG = LOGS / "pipeline.log"
RUN_LOCK = LOGS / "pipeline.lock"
STREAM_PID = LOGS / "continuous_pipeline.pid"
STREAM_STOP = LOGS / "continuous_pipeline.stop"
STREAM_STATUS = LOGS / "continuous_status.json"
QUEUE_DB = DATA / "pipeline_queue.sqlite3"
RUN_CONFIG = DATA / "run_config.json"
_ai_database = Path(os.getenv("AI_SEARCH_DB", "data/search.sqlite3")).expanduser()
AI_SEARCH_DB = (
    _ai_database
    if _ai_database.is_absolute()
    else AI_PROJECT_ROOT / _ai_database
).resolve()
_zone_database = Path(
    os.getenv(
        "ZONE_UNIVERSE_DB",
        str(ZONE_PROJECT_ROOT / "data" / "universe.sqlite3"),
    )
).expanduser()
ZONE_UNIVERSE_DB = (
    _zone_database
    if _zone_database.is_absolute()
    else ZONE_PROJECT_ROOT / _zone_database
).resolve()

for folder in (RAW, FILTERED, LLM, REVIEWS, MANUAL, LOGS):
    folder.mkdir(parents=True, exist_ok=True)

def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def atomic_write_json(path: Path, payload: object) -> None:
    """Atomically write JSON with a unique temporary file per writer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def atomic_write_csv(path: Path, frame: "pd.DataFrame") -> None:
    """Write a CSV by atomic replacement so readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            frame.to_csv(handle, index=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)

def stable_domain(value: object) -> str:
    value = clean(value).lower()
    value = re.sub(r"^https?://", "", value).split("/")[0].split(":")[0].strip(".")
    return value[4:] if value.startswith("www.") else value

def get_model(default: str = "claude-haiku-4-5") -> str:
    return os.getenv("ANTHROPIC_MODEL", default).strip() or default


import pandas as pd


def blank_missing(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with missing cells blanked without dtype downcasting."""
    result = frame.copy()
    return result.where(result.notna(), "")

def upsert_csv(path: Path, new_rows: pd.DataFrame, key: str = "domain") -> pd.DataFrame:
    """Append new rows and update existing rows by key, preserving one row per key."""
    path.parent.mkdir(parents=True, exist_ok=True)

    if new_rows is None or new_rows.empty:
        if path.exists() and path.stat().st_size > 0:
            try:
                return blank_missing(pd.read_csv(path, dtype=str))
            except pd.errors.EmptyDataError:
                return pd.DataFrame()
        return pd.DataFrame()

    incoming = blank_missing(new_rows)
    if key not in incoming.columns:
        raise ValueError(f"Upsert key '{key}' not found in incoming columns.")

    incoming[key] = incoming[key].map(stable_domain)
    incoming = incoming[incoming[key] != ""].drop_duplicates(subset=[key], keep="last")

    if path.exists() and path.stat().st_size > 0:
        try:
            existing = blank_missing(pd.read_csv(path, dtype=str))
        except pd.errors.EmptyDataError:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    if not existing.empty:
        if key not in existing.columns:
            raise ValueError(f"Upsert key '{key}' not found in existing file {path}.")
        existing[key] = existing[key].map(stable_domain)
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming

    combined = combined.drop_duplicates(subset=[key], keep="last")
    atomic_write_csv(path, combined)
    return combined


def read_basket_data(path: Path) -> pd.DataFrame:
    """Read one two-column human training basket."""
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=["domain_url", "rating"])
    try:
        frame = blank_missing(pd.read_csv(path, dtype=str))
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=["domain_url", "rating"])
    if not {"domain_url", "rating"}.issubset(frame.columns):
        return pd.DataFrame(columns=["domain_url", "rating"])
    frame = frame[["domain_url", "rating"]].copy()
    frame["rating"] = pd.to_numeric(
        frame["rating"],
        errors="coerce",
    ).fillna(0).clip(0, 100).astype(int)
    return frame.drop_duplicates("domain_url", keep="last")


def normalize_domain_url(value: object) -> str:
    domain = stable_domain(value)
    if not domain:
        return ""
    return f"https://{domain}"


def label_metadata_path() -> Path:
    return YES_BASKET_DATA.parent / "training_label_metadata.csv"


def normalized_label_source(value: object) -> str:
    """Collapse every explicit UI action to the canonical human provenance."""
    source = clean(value).lower() or "unknown"
    if source in {
        "human",
        "manual",
        "glasswing_dashboard",
        "ai_search_page",
        "zone_universe_page",
        "legacy_manual_override",
    }:
        return "human"
    return source


def read_label_metadata() -> pd.DataFrame:
    columns = ["domain", "label", "rating", "source", "reason", "labeled_at"]
    path = label_metadata_path()
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    try:
        frame = blank_missing(pd.read_csv(path, dtype=str))
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=columns)
    for column in columns:
        if column not in frame:
            frame[column] = ""
    frame["domain"] = frame["domain"].map(stable_domain)
    return frame[columns].drop_duplicates("domain", keep="last")


def sync_training_labels_to_shared() -> int:
    """Mirror local labels to the database visible to every cloud worker."""
    from classification_store import (
        upsert_training_labels,
        using_classification_postgres,
    )

    if not using_classification_postgres():
        return 0
    metadata = read_label_metadata()
    if metadata.empty:
        return 0
    return upsert_training_labels(metadata.to_dict(orient="records"))


def save_label_metadata(
    domain_url: str,
    label: str,
    rating: int,
    *,
    source: str,
    reason: str = "",
) -> None:
    domain = stable_domain(domain_url)
    metadata = read_label_metadata()
    metadata = metadata[~metadata["domain"].eq(domain)].copy()
    row = pd.DataFrame([{
        "domain": domain,
        "label": label,
        "rating": int(rating),
        "source": normalized_label_source(source),
        "reason": clean(reason),
        "labeled_at": datetime.now(timezone.utc).isoformat(),
    }])
    metadata = pd.concat([metadata, row], ignore_index=True)
    path = label_metadata_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(path, metadata)


def save_basket_entry(
    domain_url: str,
    basket: str,
    rating: int,
    *,
    source: str = "human",
    reason: str = "",
) -> None:
    """Save one exclusive Yes/No example with a 0-100 human rating."""
    save_basket_entries(
        [(domain_url, rating)],
        basket,
        source=source,
        reason=reason,
    )


def save_basket_entries(
    entries: object,
    basket: str,
    *,
    source: str = "human",
    reason: str = "",
) -> int:
    """Save many exclusive basket labels with one atomic disk transaction."""
    normalized_basket = str(basket).strip().lower()
    if normalized_basket not in {"yes", "no"}:
        raise ValueError("Basket must be 'yes' or 'no'.")
    normalized_entries: dict[str, int] = {}
    for domain_url, rating in entries:
        domain = stable_domain(domain_url)
        if not domain:
            raise ValueError("A domain URL is required.")
        numeric_rating = int(rating)
        if not 0 <= numeric_rating <= 100:
            raise ValueError("Rating must be between 0 and 100.")
        normalized_entries[domain] = numeric_rating
    if not normalized_entries:
        return 0

    yes = read_basket_data(YES_BASKET_DATA)
    no = read_basket_data(NO_BASKET_DATA)
    domains = set(normalized_entries)
    yes = yes[
        ~yes["domain_url"].map(stable_domain).isin(domains)
    ].copy()
    no = no[
        ~no["domain_url"].map(stable_domain).isin(domains)
    ].copy()
    rows = pd.DataFrame([
        {
            "domain_url": normalize_domain_url(domain),
            "rating": rating,
        }
        for domain, rating in normalized_entries.items()
    ])
    if normalized_basket == "yes":
        yes = pd.concat([yes, rows], ignore_index=True)
    else:
        no = pd.concat([no, rows], ignore_index=True)

    YES_BASKET_DATA.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(YES_BASKET_DATA, yes[["domain_url", "rating"]])
    atomic_write_csv(NO_BASKET_DATA, no[["domain_url", "rating"]])
    metadata = read_label_metadata()
    metadata = metadata[~metadata["domain"].isin(domains)].copy()
    labeled_at = datetime.now(timezone.utc).isoformat()
    metadata_rows = pd.DataFrame([
        {
            "domain": domain,
            "label": normalized_basket,
            "rating": rating,
            "source": normalized_label_source(source),
            "reason": clean(reason),
            "labeled_at": labeled_at,
        }
        for domain, rating in normalized_entries.items()
    ])
    metadata = pd.concat([metadata, metadata_rows], ignore_index=True)
    atomic_write_csv(label_metadata_path(), metadata)
    sync_training_labels_to_shared()
    if (
        normalized_label_source(source) == "human"
        and YES_BASKET_DATA.parent.resolve() == MANUAL.resolve()
    ):
        from manual_truth_backup import create_manual_snapshot

        create_manual_snapshot(reason=f"training_label:{normalized_basket}")
    return len(normalized_entries)


def seed_yes_basket(domain_urls: object, rating: int = 100) -> int:
    """Add missing ground-truth positives without overriding human decisions."""
    numeric_rating = int(rating)
    if not 0 <= numeric_rating <= 100:
        raise ValueError("Rating must be between 0 and 100.")

    yes = read_basket_data(YES_BASKET_DATA)
    no = read_basket_data(NO_BASKET_DATA)
    known_domains = {
        stable_domain(value)
        for value in pd.concat(
            [yes["domain_url"], no["domain_url"]], ignore_index=True
        )
        if stable_domain(value)
    }
    additions = []
    for value in domain_urls:
        domain = stable_domain(value)
        if not domain or domain in known_domains:
            continue
        additions.append({
            "domain_url": normalize_domain_url(domain),
            "rating": numeric_rating,
        })
        known_domains.add(domain)

    if additions:
        yes = pd.concat([yes, pd.DataFrame(additions)], ignore_index=True)
        YES_BASKET_DATA.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(YES_BASKET_DATA, yes[["domain_url", "rating"]])
        for row in additions:
            save_label_metadata(
                row["domain_url"], "yes", row["rating"],
                source="glasswing_portfolio", reason="verified_portfolio_company",
            )
        sync_training_labels_to_shared()
    return len(additions)


def seed_no_basket(domain_urls: object, rating: int = 0) -> int:
    """Add missing hard denials without overriding human basket decisions."""
    numeric_rating = int(rating)
    if not 0 <= numeric_rating <= 100:
        raise ValueError("Rating must be between 0 and 100.")

    yes = read_basket_data(YES_BASKET_DATA)
    no = read_basket_data(NO_BASKET_DATA)
    known_domains = {
        stable_domain(value)
        for value in pd.concat(
            [yes["domain_url"], no["domain_url"]], ignore_index=True
        )
        if stable_domain(value)
    }
    additions = []
    for value in domain_urls:
        domain = stable_domain(value)
        if not domain or domain in known_domains:
            continue
        additions.append({
            "domain_url": normalize_domain_url(domain),
            "rating": numeric_rating,
        })
        known_domains.add(domain)

    if additions:
        no = pd.concat([no, pd.DataFrame(additions)], ignore_index=True)
        NO_BASKET_DATA.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(NO_BASKET_DATA, no[["domain_url", "rating"]])
        for row in additions:
            save_label_metadata(
                row["domain_url"], "no", row["rating"],
                source="weak_rule", reason="automatic_hard_denial",
            )
        sync_training_labels_to_shared()
    return len(additions)


def seed_scored_basket(
    entries: object,
    basket: str,
    *,
    source: str = "weak_rule",
    reason: str = "automatic_routing",
    move_existing: bool = False,
) -> int:
    """Add missing (domain, objective score) pairs to one training basket."""
    normalized_basket = str(basket).strip().lower()
    if normalized_basket not in {"yes", "no"}:
        raise ValueError("Basket must be 'yes' or 'no'.")

    yes = read_basket_data(YES_BASKET_DATA)
    no = read_basket_data(NO_BASKET_DATA)
    normalized_entries = []
    for domain_value, score_value in entries:
        domain = stable_domain(domain_value)
        if not domain:
            continue
        numeric_score = int(pd.to_numeric(score_value, errors="coerce") or 0)
        normalized_entries.append(
            (domain, max(0, min(numeric_score, 100)))
        )

    if move_existing and normalized_entries:
        moving_domains = {domain for domain, _ in normalized_entries}
        if normalized_basket == "no":
            yes = yes[
                ~yes["domain_url"].map(stable_domain).isin(moving_domains)
            ].copy()
            YES_BASKET_DATA.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_csv(
                YES_BASKET_DATA, yes[["domain_url", "rating"]]
            )
        else:
            no = no[
                ~no["domain_url"].map(stable_domain).isin(moving_domains)
            ].copy()
            NO_BASKET_DATA.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_csv(
                NO_BASKET_DATA, no[["domain_url", "rating"]]
            )

    known_domains = {
        stable_domain(value)
        for value in pd.concat(
            [yes["domain_url"], no["domain_url"]], ignore_index=True
        )
        if stable_domain(value)
    }
    additions = []
    for domain, numeric_score in normalized_entries:
        if not domain or domain in known_domains:
            continue
        additions.append({
            "domain_url": normalize_domain_url(domain),
            "rating": numeric_score,
        })
        known_domains.add(domain)

    if not additions:
        return 0
    target = yes if normalized_basket == "yes" else no
    target = pd.concat([target, pd.DataFrame(additions)], ignore_index=True)
    path = YES_BASKET_DATA if normalized_basket == "yes" else NO_BASKET_DATA
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(path, target[["domain_url", "rating"]])
    for row in additions:
        save_label_metadata(
            row["domain_url"], normalized_basket, row["rating"],
            source=source, reason=reason,
        )
    sync_training_labels_to_shared()
    return len(additions)


def remove_basket_entry(domain_url: str) -> None:
    """Remove a basket label without touching any screening history."""
    domain = stable_domain(domain_url)
    for path in (YES_BASKET_DATA, NO_BASKET_DATA):
        frame = read_basket_data(path)
        frame = frame[
            ~frame["domain_url"].map(stable_domain).eq(domain)
        ].copy()
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_csv(path, frame[["domain_url", "rating"]])
    metadata = read_label_metadata()
    metadata = metadata[~metadata["domain"].eq(domain)].copy()
    atomic_write_csv(label_metadata_path(), metadata)
    from classification_store import (
        remove_training_labels,
        using_classification_postgres,
    )

    if using_classification_postgres():
        remove_training_labels([domain])
    if YES_BASKET_DATA.parent.resolve() == MANUAL.resolve():
        from manual_truth_backup import create_manual_snapshot

        create_manual_snapshot(reason="training_label_removed")
