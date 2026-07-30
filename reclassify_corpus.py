#!/usr/bin/env python3
"""Rescore locally cached crawler content without making network requests.

The crawler databases remain the source of website content, while
classification_store.py remains the canonical source of screening decisions.
This job deliberately bypasses filter.persist_screening_output() for each
batch so large rescoring runs do not repeatedly rewrite every compatibility
CSV and basket. Those views are reconciled once, after the run completes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

import filter as screening_filter
from classification_store import (
    CLASSIFICATION_DB,
    MODEL_VERSION,
    connect_store,
    export_all_screened_csv,
    upsert_classifications,
)
from common import AI_SEARCH_DB, DATA, ZONE_UNIVERSE_DB, stable_domain
from db_backend import using_postgres


ZONE_DB = ZONE_UNIVERSE_DB
STATE_PATH = DATA / "reclassification" / "cached_corpus_state.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def read_state(path: Path = STATE_PATH) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_domains(
    path: Path,
    query: str,
) -> set[str]:
    if not path.exists():
        return set()
    with sqlite3.connect(
        f"file:{path}?mode=ro",
        uri=True,
        timeout=30,
    ) as conn:
        conn.execute("PRAGMA query_only=ON")
        return {
            stable_domain(row[0])
            for row in conn.execute(query)
            if stable_domain(row[0])
        }


def corpus_domains(
    scope: str,
    *,
    ai_search_db: Path = AI_SEARCH_DB,
    zone_db: Path = ZONE_DB,
) -> list[str]:
    """Return domains with retained, locally readable website content."""
    domains: set[str] = set()
    if scope in {"all", "ai"}:
        domains.update(
            _read_domains(
                ai_search_db,
                """
                SELECT DISTINCT d.domain
                FROM domains d
                JOIN pages p ON p.domain_id=d.id
                """,
            )
        )
    if scope in {"all", "zone"}:
        domains.update(
            _read_domains(
                zone_db,
                "SELECT domain FROM pages",
            )
        )
    return sorted(domains)


def zone_import_active(path: Path = ZONE_DB) -> bool:
    """Avoid competing with the large atomic .com zone import."""
    if not path.exists():
        return False
    try:
        with sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            timeout=30,
        ) as conn:
            conn.execute("PRAGMA query_only=ON")
            row = conn.execute(
                """
                SELECT count(*)
                FROM zones
                WHERE lower(import_status)='importing'
                """
            ).fetchone()
            return bool(row and int(row[0]) > 0)
    except sqlite3.Error:
        # A busy import can briefly prevent a read. Waiting is safer than
        # launching another CPU-heavy job in that ambiguous state.
        return True


def classification_records(
    domains: Iterable[str],
    *,
    path: Path = CLASSIFICATION_DB,
) -> dict[str, dict[str, object]]:
    normalized = list(dict.fromkeys(stable_domain(item) for item in domains))
    normalized = [item for item in normalized if item]
    if not normalized or (not using_postgres() and not path.exists()):
        return {}

    results: dict[str, dict[str, object]] = {}
    with connect_store(path, read_only=True) as conn:
        for start in range(0, len(normalized), 800):
            batch = normalized[start : start + 800]
            marks = ",".join("?" for _ in batch)
            rows = conn.execute(
                f"""
                SELECT domain,routing_bucket,model_version,record_json,
                       final_url,company_name,description,excerpt,http_status,
                       crawl_status,content_source,fetch_error,classified_at
                FROM classifications
                WHERE domain IN ({marks})
                """,
                batch,
            ).fetchall()
            for row in rows:
                plain = dict(row)
                try:
                    record = json.loads(plain.pop("record_json") or "{}")
                except (TypeError, json.JSONDecodeError):
                    record = {}
                if not isinstance(record, dict):
                    record = {}
                record.setdefault("domain", plain["domain"])
                record.setdefault("registrable", plain["domain"])
                record.setdefault("page_title", plain["company_name"])
                record.setdefault("meta_description", plain["description"])
                record.setdefault("homepage_excerpt", plain["excerpt"])
                record.setdefault("final_url", plain["final_url"])
                record.setdefault("http_status", plain["http_status"])
                record.setdefault("crawl_status", plain["crawl_status"])
                record.setdefault("content_source", plain["content_source"])
                record.setdefault("fetch_error", plain["fetch_error"])
                record["_previous_bucket"] = plain["routing_bucket"]
                record["_previous_model_version"] = plain["model_version"]
                results[str(plain["domain"])] = record
    return results


def cached_fetch_from_record(
    domain: str,
    record: dict[str, object] | None,
) -> screening_filter.Fetch | None:
    """Use the canonical excerpt only if a crawler cache lookup is unavailable."""
    if not record:
        return None
    text = str(record.get("homepage_excerpt", "") or "").strip()
    title = str(record.get("page_title", "") or "").strip()
    description = str(record.get("meta_description", "") or "").strip()
    if not any((text, title, description)):
        return None
    raw_status = str(record.get("http_status", "") or "").strip()
    try:
        status = int(float(raw_status)) if raw_status else None
    except ValueError:
        status = None
    return screening_filter.Fetch(
        url=str(record.get("final_url", "") or f"https://{domain}"),
        status=status,
        title=title,
        description=description,
        text=text,
        error="",
        pages_scraped=max(1, int(record.get("pages_scraped", 1) or 1)),
        source="classification_cache_fallback",
        crawl_status=str(record.get("crawl_status", "") or ""),
        crawled_at=str(
            record.get("crawler_crawled_at", "")
            or record.get("classified_at", "")
            or ""
        ),
    )


async def classify_cached_batch(
    domains: list[str],
    prior_records: dict[str, dict[str, object]],
) -> pd.DataFrame:
    """Apply the current rules using local content only."""
    fetches = await asyncio.to_thread(
        screening_filter.load_crawler_fetches,
        domains,
    )
    semaphore = asyncio.Semaphore(1)
    output: list[dict[str, object]] = []
    now = utc_now()
    for domain in domains:
        previous = dict(prior_records.get(domain, {}))
        cached = fetches.get(domain) or cached_fetch_from_record(
            domain,
            previous,
        )
        if cached is None:
            continue
        previous.pop("_previous_bucket", None)
        previous.pop("_previous_model_version", None)
        previous.update(
            {
                "domain": domain,
                "registrable": domain,
                "observed_at": previous.get("observed_at", "") or now,
                "rescore_model_version": MODEL_VERSION,
                "rescore_source": "cached_corpus",
                "_cached_page": cached,
            }
        )
        # process() does not touch the session when _cached_page is supplied.
        result = await screening_filter.process(
            previous,
            None,  # type: ignore[arg-type]
            semaphore,
            0,
            cached_page=cached,
        )
        output.append(result)
    return pd.DataFrame(output)


def finalize_compatibility_views() -> dict[str, int]:
    frame = export_all_screened_csv(force=True)
    if frame is None or frame.empty:
        return {}
    return screening_filter.persist_crawled_corpus_views(frame)


def _state_matches(
    state: dict[str, object],
    *,
    scope: str,
    fresh: bool,
) -> bool:
    return bool(
        state.get("status") == "running"
        and state.get("model_version") == MODEL_VERSION
        and state.get("scope") == scope
        and bool(state.get("fresh")) == fresh
    )


async def run_rescore(options: argparse.Namespace) -> int:
    if options.wait_for_zone_import and zone_import_active():
        waiting_state = {
            "status": "waiting_for_zone_import",
            "model_version": MODEL_VERSION,
            "scope": options.scope,
            "fresh": bool(options.fresh),
            "started_at": utc_now(),
            "updated_at": utc_now(),
            "message": (
                "The .com zone import has priority. Cached rescoring will "
                "start automatically when that import commits."
            ),
        }
        if not options.dry_run:
            atomic_write_json(options.state, waiting_state)
        print(waiting_state["message"], flush=True)
        while zone_import_active():
            await asyncio.sleep(30)

    all_domains = corpus_domains(options.scope)
    prior = classification_records(all_domains)
    state = read_state(options.state)
    resume = _state_matches(
        state,
        scope=options.scope,
        fresh=options.fresh,
    ) and not options.restart
    cursor = str(state.get("cursor", "")) if resume else ""

    candidates = [
        domain
        for domain in all_domains
        if domain > cursor
        and (
            options.fresh
            or str(
                prior.get(domain, {}).get(
                    "_previous_model_version",
                    "",
                )
            )
            != MODEL_VERSION
        )
    ]
    total_eligible = len(candidates)
    if options.limit:
        candidates = candidates[: options.limit]

    initial_counts = Counter(
        str(record.get("_previous_bucket", "unclassified"))
        for record in prior.values()
    )
    resumed_processed = int(state.get("processed", 0)) if resume else 0
    run_target = (
        max(
            int(state.get("eligible_at_start", 0)),
            resumed_processed + len(candidates),
        )
        if resume
        else len(candidates)
    )
    run_state: dict[str, object] = {
        "status": "running",
        "model_version": MODEL_VERSION,
        "scope": options.scope,
        "fresh": bool(options.fresh),
        "started_at": (
            str(state.get("started_at", ""))
            if resume
            else utc_now()
        ),
        "updated_at": utc_now(),
        "cursor": cursor,
        "corpus_domains": len(all_domains),
        "eligible_at_start": run_target,
        "processed": resumed_processed,
        "classified": int(state.get("classified", 0)) if resume else 0,
        "missing_cache": int(state.get("missing_cache", 0)) if resume else 0,
        "changed_bucket": int(state.get("changed_bucket", 0)) if resume else 0,
        "errors": int(state.get("errors", 0)) if resume else 0,
        "initial_buckets": dict(initial_counts),
    }
    if not options.dry_run:
        atomic_write_json(options.state, run_state)

    print(
        f"Cached corpus: {len(all_domains):,}; "
        f"eligible for {MODEL_VERSION}: {total_eligible:,}"
    )
    if options.limit and len(candidates) < total_eligible:
        print(f"This run is limited to {len(candidates):,} domains.")
    if resume:
        print(f"Resuming after {cursor or '(start)'}")
    if options.dry_run:
        print("Dry run: classifications will not be saved.")

    for start in range(0, len(candidates), options.batch_size):
        batch = candidates[start : start + options.batch_size]
        try:
            output = await classify_cached_batch(batch, prior)
            if not output.empty:
                output = screening_filter.apply_run_preferences(
                    output,
                    screening_filter.load_run_preferences(),
                )
                output = screening_filter.apply_persistent_overrides(output)
            changed = 0
            for record in output.to_dict("records"):
                old = str(
                    prior.get(
                        stable_domain(record.get("domain", "")),
                        {},
                    ).get("_previous_bucket", "unclassified")
                )
                if old != str(record.get("routing_bucket", "")):
                    changed += 1
            if not options.dry_run and not output.empty:
                upsert_classifications(output)
            missing = len(batch) - len(output)
            run_state["processed"] = int(run_state["processed"]) + len(batch)
            run_state["classified"] = int(run_state["classified"]) + len(output)
            run_state["missing_cache"] = (
                int(run_state["missing_cache"]) + missing
            )
            run_state["changed_bucket"] = (
                int(run_state["changed_bucket"]) + changed
            )
            run_state["cursor"] = batch[-1]
            run_state["updated_at"] = utc_now()
            if not options.dry_run:
                atomic_write_json(options.state, run_state)
            print(
                f"{int(run_state['processed']):,}/{run_target:,} "
                f"processed · {int(run_state['changed_bucket']):,} moved · "
                f"{int(run_state['missing_cache']):,} missing cache",
                flush=True,
            )
        except Exception as exc:
            run_state["errors"] = int(run_state["errors"]) + 1
            run_state["updated_at"] = utc_now()
            run_state["last_error"] = f"{type(exc).__name__}: {exc}"[:1000]
            if not options.dry_run:
                atomic_write_json(options.state, run_state)
            print(
                f"Batch ending at {batch[-1]} failed: {exc}",
                file=sys.stderr,
                flush=True,
            )
            return 1
        if options.pause_seconds > 0 and start + options.batch_size < len(candidates):
            await asyncio.sleep(options.pause_seconds)

    if options.dry_run:
        print("Dry run complete.")
        return 0

    view_counts = finalize_compatibility_views()
    run_state.update(
        {
            "status": "complete",
            "completed_at": utc_now(),
            "updated_at": utc_now(),
            "compatibility_views": view_counts,
        }
    )
    atomic_write_json(options.state, run_state)
    print(
        f"Complete: {int(run_state['classified']):,} classified; "
        f"{int(run_state['changed_bucket']):,} changed buckets."
    )
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description=(
            "Rescore saved .AI/.app/.com website content with the current "
            "Glasswing model. No websites are contacted."
        )
    )
    commands = root.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run")
    run.add_argument(
        "--scope",
        choices=("all", "ai", "zone"),
        default="all",
    )
    run.add_argument("--batch-size", type=int, default=50)
    run.add_argument(
        "--pause-seconds",
        type=float,
        default=0.35,
        help="Yield between batches to reduce laptop load.",
    )
    run.add_argument("--limit", type=int, default=0)
    run.add_argument(
        "--fresh",
        action="store_true",
        help="Rescore even rows already on the current model.",
    )
    run.add_argument(
        "--restart",
        action="store_true",
        help="Ignore an incomplete checkpoint and begin a new pass.",
    )
    run.add_argument("--dry-run", action="store_true")
    run.add_argument(
        "--wait-for-zone-import",
        action="store_true",
        help="Wait quietly until any active zone-file import completes.",
    )
    run.add_argument("--state", type=Path, default=STATE_PATH)

    status = commands.add_parser("status")
    status.add_argument("--state", type=Path, default=STATE_PATH)
    commands.add_parser("finalize")
    return root


def main() -> None:
    options = parser().parse_args()
    if options.command == "status":
        state = read_state(options.state)
        print(json.dumps(state or {"status": "not_started"}, indent=2))
        return
    if options.command == "finalize":
        print(json.dumps(finalize_compatibility_views(), indent=2))
        return
    if options.batch_size < 1 or options.batch_size > 1000:
        raise SystemExit("--batch-size must be between 1 and 1000")
    if options.pause_seconds < 0:
        raise SystemExit("--pause-seconds cannot be negative")
    raise SystemExit(asyncio.run(run_rescore(options)))


if __name__ == "__main__":
    main()
