"""Read-only operational checks shared by local and future cloud UIs."""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from classification_store import CLASSIFICATION_DB, connect_store
from common import (
    DATA,
    LOGS,
    QUEUE_DB,
    STREAM_STATUS,
)
from db_backend import using_postgres
from manual_truth_backup import list_manual_snapshots
from model_evaluation import performance_metrics
from rejection_cleanup import AI_SEARCH_DB, ZONE_DB
from runtime_control import runtime_status


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _quick_check(path: Path) -> str:
    if not path.exists():
        return "missing"
    try:
        with sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            timeout=30,
        ) as conn:
            return str(conn.execute("PRAGMA quick_check").fetchone()[0])
    except sqlite3.Error as exc:
        return f"{type(exc).__name__}: {exc}"


def _database_counts() -> dict[str, int]:
    counts: dict[str, int] = {
        "classifications": 0,
        "manual_decisions": 0,
        "pipeline_pending": 0,
        "pipeline_processing": 0,
        "ai_pages": 0,
        "ai_indexed": 0,
        "ai_orphans": 0,
        "zone_pages": 0,
        "zone_indexed": 0,
        "zone_orphans": 0,
    }
    if using_postgres() or CLASSIFICATION_DB.exists():
        with connect_store(CLASSIFICATION_DB, read_only=True) as conn:
            counts["classifications"] = int(
                conn.execute(
                    "SELECT count(*) AS total FROM classifications"
                ).fetchone()["total"]
            )
            counts["manual_decisions"] = int(
                conn.execute(
                    "SELECT count(*) AS total FROM manual_decisions"
                ).fetchone()["total"]
            )
    if QUEUE_DB.exists():
        with sqlite3.connect(QUEUE_DB, timeout=30) as conn:
            for status, total in conn.execute(
                "SELECT status,count(*) FROM domains GROUP BY status"
            ):
                if status in {"pending", "processing"}:
                    counts[f"pipeline_{status}"] = int(total)
    for prefix, path, page_query in (
        (
            "ai",
            AI_SEARCH_DB,
            "SELECT count(*) FROM pages",
        ),
        (
            "zone",
            ZONE_DB,
            "SELECT count(*) FROM pages",
        ),
    ):
        if not path.exists():
            continue
        with sqlite3.connect(path, timeout=30) as conn:
            counts[f"{prefix}_pages"] = int(
                conn.execute(page_query).fetchone()[0]
            )
            counts[f"{prefix}_indexed"] = int(
                conn.execute("SELECT count(*) FROM page_search").fetchone()[0]
            )
            counts[f"{prefix}_orphans"] = int(
                conn.execute(
                    """
                    SELECT count(*) FROM page_search f
                    LEFT JOIN pages p
                      ON p.id=cast(f.page_id AS INTEGER)
                    WHERE p.id IS NULL
                    """
                ).fetchone()[0]
            )
    return counts


def _timestamp_age_seconds(value: object) -> float | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            (datetime.now(timezone.utc) - parsed).total_seconds(),
        )
    except ValueError:
        return None


def health_report(*, include_integrity: bool = False) -> dict[str, object]:
    counts = _database_counts()
    runtime = runtime_status()
    snapshots = list_manual_snapshots()
    stream = _json(STREAM_STATUS)
    free_gb = shutil.disk_usage(DATA).free / (1024 ** 3)
    checks: list[dict[str, str]] = []

    if using_postgres():
        try:
            with connect_store(CLASSIFICATION_DB, read_only=True):
                pass
            checks.append({
                "Check": "Classification database",
                "Status": "Healthy",
                "Detail": "Connected (Postgres)",
            })
        except Exception as exc:
            checks.append({
                "Check": "Classification database",
                "Status": "Error",
                "Detail": f"{type(exc).__name__}: {exc}",
            })
        remaining = (
            ("Pipeline queue", QUEUE_DB),
            (".AI search database", AI_SEARCH_DB),
            ("Zone database", ZONE_DB),
        )
    else:
        remaining = (
            ("Classification database", CLASSIFICATION_DB),
            ("Pipeline queue", QUEUE_DB),
            (".AI search database", AI_SEARCH_DB),
            ("Zone database", ZONE_DB),
        )
    for name, path in remaining:
        if not path.exists():
            checks.append({
                "Check": name,
                "Status": "Error",
                "Detail": f"Missing: {path}",
            })
        elif include_integrity:
            result = _quick_check(path)
            checks.append({
                "Check": name,
                "Status": "Healthy" if result == "ok" else "Error",
                "Detail": result,
            })
        else:
            checks.append({
                "Check": name,
                "Status": "Healthy",
                "Detail": "Present",
            })

    for prefix, label in (("ai", ".AI"), ("zone", ".COM/.APP")):
        pages = counts[f"{prefix}_pages"]
        indexed = counts[f"{prefix}_indexed"]
        orphans = counts[f"{prefix}_orphans"]
        healthy = pages == indexed and orphans == 0
        checks.append({
            "Check": f"{label} search index",
            "Status": "Healthy" if healthy else "Error",
            "Detail": (
                f"{pages:,} pages · {indexed:,} indexed · "
                f"{orphans:,} orphans"
            ),
        })

    checks.append({
        "Check": "Free disk reserve",
        "Status": "Healthy" if free_gb >= 20 else "Warning",
        "Detail": f"{free_gb:.1f} GB free",
    })
    pipeline_running = bool(
        runtime.get("services", {}).get("pipeline", {}).get("running", False)
    )
    processing_is_healthy = (
        counts["pipeline_processing"] == 0 or pipeline_running
    )
    checks.append({
        "Check": "Stuck pipeline claims",
        "Status": "Healthy" if processing_is_healthy else "Warning",
        "Detail": (
            f"{counts['pipeline_processing']:,} actively processing"
            if pipeline_running
            else f"{counts['pipeline_processing']:,} left processing"
        ),
    })
    valid_snapshots = sum(row["valid"] for row in snapshots)
    checks.append({
        "Check": "Manual truth backup",
        "Status": "Healthy" if valid_snapshots else "Warning",
        "Detail": (
            f"{valid_snapshots:,} valid snapshot(s)"
            if valid_snapshots
            else "No snapshot created yet"
        ),
    })

    status_rank = {"Healthy": 0, "Warning": 1, "Error": 2}
    worst = max(
        (status_rank[row["Status"]] for row in checks),
        default=0,
    )
    overall = ("Healthy", "Warning", "Error")[worst]
    stream_age = _timestamp_age_seconds(stream.get("updated_at"))
    return {
        "overall": overall,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "counts": counts,
        "runtime": runtime,
        "manual_snapshots": snapshots,
        "performance": performance_metrics(),
        "pipeline_status": stream,
        "pipeline_status_age_seconds": stream_age,
        "paths": {
            "data_root": str(DATA),
            "log_root": str(LOGS),
            "classification_db": str(CLASSIFICATION_DB),
            "ai_search_db": str(AI_SEARCH_DB),
            "zone_db": str(ZONE_DB),
        },
    }
