from __future__ import annotations

import csv
import gzip
import json
import os
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path

from .domain_quality import assess_domain


def summarize_rejection_audits(
    rejection_root: Path,
    *,
    examples_per_reason: int = 25,
) -> dict[str, int]:
    """Replace massive rejected-domain logs with counts and small samples."""
    converted = 0
    removed_bytes = 0
    rejected_domains = 0
    for path in sorted(rejection_root.glob("*.tsv.gz")):
        reasons: Counter[str] = Counter()
        examples: dict[str, list[str]] = {}
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            header = next(handle, "")
            for line in handle:
                domain, _, remainder = line.partition("\t")
                reason, _, _score = remainder.partition("\t")
                domain = domain.strip()
                reason = reason.strip() or "unspecified"
                if not domain:
                    continue
                reasons[reason] += 1
                rejected_domains += 1
                sample = examples.setdefault(reason, [])
                if len(sample) < max(1, int(examples_per_reason)):
                    sample.append(domain)
        summary_path = path.with_name(
            path.name.removesuffix(".tsv.gz") + ".summary.json"
        )
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=summary_path.parent,
            prefix=f".{summary_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as target:
            temporary = Path(target.name)
            json.dump(
                {
                    "source_audit": path.name,
                    "rejected_domains": sum(reasons.values()),
                    "by_reason": dict(reasons.most_common()),
                    "examples": examples,
                },
                target,
                indent=2,
                sort_keys=True,
            )
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, summary_path)
        removed_bytes += path.stat().st_size
        path.unlink()
        converted += 1
    return {
        "audits_converted": converted,
        "rejected_domains_summarized": rejected_domains,
        "compressed_bytes_removed": removed_bytes,
    }


def prune_noise(conn: sqlite3.Connection) -> dict[str, object]:
    """Remove previously crawled labels that now fail the quality gate."""
    page_rows = conn.execute("SELECT id,domain FROM pages").fetchall()
    rejected_pages = [
        (int(row["id"]), str(row["domain"]))
        for row in page_rows
        if not assess_domain(str(row["domain"])).accepted
    ]
    retry_rows = conn.execute(
        "SELECT domain FROM crawl_retries"
    ).fetchall()
    rejected_retries = [
        str(row["domain"])
        for row in retry_rows
        if not assess_domain(str(row["domain"])).accepted
    ]
    conn.execute("BEGIN IMMEDIATE")
    for page_id, _ in rejected_pages:
        conn.execute(
            "DELETE FROM page_search WHERE page_id=?",
            (page_id,),
        )
    conn.executemany(
        "DELETE FROM pages WHERE domain=?",
        [(domain,) for _, domain in rejected_pages],
    )
    conn.executemany(
        "DELETE FROM crawl_retries WHERE domain=?",
        [(domain,) for domain in rejected_retries],
    )
    conn.commit()
    return {
        "removed_pages": len(rejected_pages),
        "removed_retries": len(rejected_retries),
        "domains": sorted(
            {
                domain for _, domain in rejected_pages
            }
            | set(rejected_retries)
        ),
        "note": (
            "Append-only archive bytes become harmless orphans and are "
            "reclaimed by a future archive compaction."
        ),
    }


def _normalized_domain(value: object) -> str:
    domain = str(value or "").strip().lower()
    for prefix in ("https://", "http://"):
        if domain.startswith(prefix):
            domain = domain[len(prefix) :]
    domain = domain.split("/", 1)[0].split(":", 1)[0].strip(".")
    return domain[4:] if domain.startswith("www.") else domain


def _filter_csv(path: Path, domains: set[str]) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    temporary_name = ""
    removed = 0
    try:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames:
                return 0
            domain_column = next(
                (
                    column
                    for column in ("domain", "domain_url", "registrable")
                    if column in reader.fieldnames
                ),
                "",
            )
            if not domain_column:
                return 0
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as target:
                temporary_name = target.name
                writer = csv.DictWriter(
                    target,
                    fieldnames=reader.fieldnames,
                )
                writer.writeheader()
                for row in reader:
                    if _normalized_domain(row.get(domain_column, "")) in domains:
                        removed += 1
                        continue
                    writer.writerow(row)
                target.flush()
                os.fsync(target.fileno())
        if removed:
            os.replace(temporary_name, path)
            temporary_name = ""
        return removed
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def purge_screening_records(
    domains: list[str],
    *,
    project_root: Path,
) -> dict[str, int]:
    """Remove noise labels from canonical screening data and CSV exports."""
    normalized = {
        _normalized_domain(domain)
        for domain in domains
        if _normalized_domain(domain)
    }
    try:
        from training_truth import protected_training_domains

        normalized -= protected_training_domains()
    except ImportError:
        # The standalone Zone package can run without the parent dashboard.
        # In that mode, avoid touching manual data altogether below.
        pass
    if not normalized:
        return {"database_rows": 0, "csv_rows": 0}
    database_rows = 0
    marks = ",".join("?" for _ in normalized)
    values = sorted(normalized)
    for database, statements in (
        (
            project_root / "data" / "classifications.sqlite3",
            (
                "DELETE FROM classifications "
                f"WHERE domain IN ({marks})",
            ),
        ),
        (
            project_root / "data" / "pipeline_queue.sqlite3",
            (
                "DELETE FROM domains "
                f"WHERE domain IN ({marks})",
            ),
        ),
    ):
        if not database.exists():
            continue
        with sqlite3.connect(database, timeout=30) as conn:
            for statement in statements:
                cursor = conn.execute(statement, values)
                database_rows += max(0, int(cursor.rowcount))
            conn.commit()
    csv_rows = 0
    for folder_name in ("filtered", "llm"):
        folder = project_root / "data" / folder_name
        if not folder.exists():
            continue
        for path in folder.glob("*.csv"):
            csv_rows += _filter_csv(path, normalized)
    return {
        "database_rows": database_rows,
        "csv_rows": csv_rows,
    }
