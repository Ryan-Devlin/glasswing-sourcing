"""Human-feedback metrics for frozen classifier versions."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

from classification_store import CLASSIFICATION_DB, MODEL_VERSION, connect_store
from common import DATA, atomic_write_json, stable_domain
from db_backend import using_postgres


METRICS_PATH = DATA / "metrics" / "model_performance.json"
HUMAN_SOURCES = {
    "human",
    "manual",
    "glasswing_dashboard",
    "ai_search_page",
    "zone_universe_page",
    "legacy_manual_override",
    "human_restore",
}


def _extension(domain: str) -> str:
    value = stable_domain(domain)
    return f".{value.rsplit('.', 1)[-1]}" if "." in value else ""


def performance_metrics(
    path: Path | str = CLASSIFICATION_DB,
) -> dict[str, object]:
    database = Path(path)
    empty: dict[str, object] = {
        "model_version": MODEL_VERSION,
        "human_decisions": 0,
        "decisive_comparisons": 0,
        "agreement_rate": None,
        "false_negative_reversals": 0,
        "false_positive_reversals": 0,
        "human_yes": 0,
        "human_no": 0,
        "investable_per_1000": 0.0,
        "by_model_version": {},
        "by_extension": {},
    }
    if not using_postgres() and not database.exists():
        return empty
    with connect_store(database, read_only=True) as conn:
        rows = conn.execute(
            """
            WITH latest AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY domain ORDER BY id DESC
                       ) AS position
                FROM movement_log
            )
            SELECT domain,to_bucket,
                   coalesce(nullif(predicted_bucket,''),from_bucket)
                       AS predicted_bucket,
                   coalesce(nullif(model_version,''),?) AS model_version,
                   source,moved_at
            FROM latest
            WHERE position=1
            """,
            (MODEL_VERSION,),
        ).fetchall()
        classification_total = int(
            conn.execute("SELECT count(*) FROM classifications").fetchone()[0]
        )
        investable_total = int(
            conn.execute(
                "SELECT count(*) FROM classifications "
                "WHERE routing_bucket='priority'"
            ).fetchone()[0]
        )
    decisions = [
        dict(row)
        for row in rows
        if str(row["source"]).lower() in HUMAN_SOURCES
        and str(row["to_bucket"]) in {"priority", "reject"}
    ]
    if not decisions:
        return {
            **empty,
            "investable_per_1000": round(
                investable_total * 1000 / max(classification_total, 1),
                3,
            ),
        }

    comparisons = [
        row
        for row in decisions
        if row["predicted_bucket"] in {"priority", "reject"}
    ]
    agreements = sum(
        row["predicted_bucket"] == row["to_bucket"]
        for row in comparisons
    )
    false_negative = sum(
        row["predicted_bucket"] == "reject"
        and row["to_bucket"] == "priority"
        for row in decisions
    )
    false_positive = sum(
        row["predicted_bucket"] == "priority"
        and row["to_bucket"] == "reject"
        for row in decisions
    )
    by_version: dict[str, Counter] = defaultdict(Counter)
    by_extension: dict[str, Counter] = defaultdict(Counter)
    for row in decisions:
        version = str(row["model_version"] or "unknown")
        extension = _extension(str(row["domain"])) or "other"
        for bucket in (by_version[version], by_extension[extension]):
            bucket["decisions"] += 1
            bucket["yes"] += row["to_bucket"] == "priority"
            bucket["no"] += row["to_bucket"] == "reject"
            bucket["false_negative_reversals"] += (
                row["predicted_bucket"] == "reject"
                and row["to_bucket"] == "priority"
            )
            bucket["false_positive_reversals"] += (
                row["predicted_bucket"] == "priority"
                and row["to_bucket"] == "reject"
            )
            if row["predicted_bucket"] in {"priority", "reject"}:
                bucket["decisive_comparisons"] += 1
                bucket["agreements"] += (
                    row["predicted_bucket"] == row["to_bucket"]
                )

    def serialize(groups: dict[str, Counter]) -> dict[str, dict[str, object]]:
        output: dict[str, dict[str, object]] = {}
        for key, counts in sorted(groups.items()):
            comparisons_count = int(counts["decisive_comparisons"])
            output[key] = {
                **{name: int(value) for name, value in counts.items()},
                "agreement_rate": (
                    round(counts["agreements"] / comparisons_count, 4)
                    if comparisons_count
                    else None
                ),
            }
        return output

    return {
        "model_version": MODEL_VERSION,
        "human_decisions": len(decisions),
        "decisive_comparisons": len(comparisons),
        "agreement_rate": (
            round(agreements / len(comparisons), 4)
            if comparisons
            else None
        ),
        "false_negative_reversals": false_negative,
        "false_positive_reversals": false_positive,
        "human_yes": sum(
            row["to_bucket"] == "priority" for row in decisions
        ),
        "human_no": sum(
            row["to_bucket"] == "reject" for row in decisions
        ),
        "investable_per_1000": round(
            investable_total * 1000 / max(classification_total, 1),
            3,
        ),
        "by_model_version": serialize(by_version),
        "by_extension": serialize(by_extension),
    }


def write_performance_metrics(
    path: Path | str = METRICS_PATH,
) -> dict[str, object]:
    payload = performance_metrics()
    atomic_write_json(Path(path), payload)
    return payload
