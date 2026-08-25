"""Read-only operational checks shared by local and future cloud UIs."""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ai_search.database import AI_SEARCH_DATABASE_ENV_VAR
from ai_search.database import connect as connect_ai_search
from classification_store import CLASSIFICATION_DB, connect_store
from common import (
    DATA,
    LOGS,
    QUEUE_DB,
    STREAM_STATUS,
)
from db_backend import PostgresConnection, connect_postgres, using_postgres
from manual_truth_backup import list_manual_snapshots
from model_evaluation import performance_metrics
from rejection_cleanup import AI_SEARCH_DB, ZONE_DB
from runtime_control import runtime_status
from zone_universe.database import ZONE_UNIVERSE_DATABASE_ENV_VAR
from zone_universe.database import connect as connect_zone_universe

# Same dedicated env var continuous_pipeline.py gates on -- never the
# classification store's DATABASE_URL (see continuous_pipeline.connect_db).
PIPELINE_DATABASE_ENV_VAR = "PIPELINE_DATABASE_URL"
_snapshot_cache: tuple[float, list[dict[str, object]]] | None = None
SNAPSHOT_CACHE_SECONDS = 300.0


@contextmanager
def _timed(label: str):
    """Temporary diagnostic: health_report() is the one thing every page
    load blocks on, and it opens 5+ separate Postgres connections in cloud
    mode with no visibility into which one is actually slow. Logs to
    stderr (captured by `railway logs`) rather than stdout, matching the
    other workers' logging.basicConfig default stream."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"[health_monitor] {label}: {elapsed_ms:.0f}ms", file=sys.stderr, flush=True)


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
        with _timed("classification_store connect+count"), connect_store(
            CLASSIFICATION_DB, read_only=True
        ) as conn:
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
    if using_postgres(PIPELINE_DATABASE_ENV_VAR):
        with _timed("pipeline connect+count"):
            conn = connect_postgres(read_only=True, env_var=PIPELINE_DATABASE_ENV_VAR)
            try:
                conn.execute("SET search_path TO pipeline, public")
                for row in conn.execute(
                    "SELECT status, count(*) AS total FROM domains GROUP BY status"
                ).fetchall():
                    if row["status"] in {"pending", "processing"}:
                        counts[f"pipeline_{row['status']}"] = int(row["total"])
            finally:
                conn.close()
    elif QUEUE_DB.exists():
        with sqlite3.connect(QUEUE_DB, timeout=30) as conn:
            for status, total in conn.execute(
                "SELECT status,count(*) FROM domains GROUP BY status"
            ):
                if status in {"pending", "processing"}:
                    counts[f"pipeline_{status}"] = int(total)
    for prefix, path, env_var, connect_fn in (
        ("ai", AI_SEARCH_DB, AI_SEARCH_DATABASE_ENV_VAR, connect_ai_search),
        ("zone", ZONE_DB, ZONE_UNIVERSE_DATABASE_ENV_VAR, connect_zone_universe),
    ):
        if not (using_postgres(env_var) or path.exists()):
            continue
        with _timed(f"{prefix}_search connect+count"), connect_fn(
            path, read_only=True
        ) as conn:
            postgres = isinstance(conn, PostgresConnection)
            counts[f"{prefix}_pages"] = int(
                conn.execute("SELECT count(*) AS total FROM pages").fetchone()["total"]
            )
            if postgres:
                # search_vector lives directly on the pages row, so there's
                # no separate index table that could go out of sync with it
                # -- "orphans" (an FTS5 shadow-table concept) can't occur.
                counts[f"{prefix}_indexed"] = int(
                    conn.execute(
                        "SELECT count(*) AS total FROM pages "
                        "WHERE search_vector IS NOT NULL"
                    ).fetchone()["total"]
                )
                counts[f"{prefix}_orphans"] = 0
            else:
                counts[f"{prefix}_indexed"] = int(
                    conn.execute(
                        "SELECT count(*) AS total FROM page_search"
                    ).fetchone()["total"]
                )
                counts[f"{prefix}_orphans"] = int(
                    conn.execute(
                        """
                        SELECT count(*) AS total FROM page_search f
                        LEFT JOIN pages p
                          ON p.id=cast(f.page_id AS INTEGER)
                        WHERE p.id IS NULL
                        """
                    ).fetchone()["total"]
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
    global _snapshot_cache
    with _timed("_database_counts total"):
        counts = _database_counts()
    with _timed("runtime_status"):
        runtime = runtime_status()
    # Snapshot validation reads every remote snapshot plus its sidecar and is
    # intentionally a deep check. Ordinary System Health loads reuse the last
    # deep result instead of paying that multi-second object-storage cost.
    cached_snapshots = bool(
        _snapshot_cache
        and time.monotonic() - _snapshot_cache[0] < SNAPSHOT_CACHE_SECONDS
    )
    if include_integrity:
        with _timed("list_manual_snapshots"):
            snapshots = list_manual_snapshots()
        _snapshot_cache = (time.monotonic(), snapshots)
    elif cached_snapshots:
        snapshots = _snapshot_cache[1]
    else:
        snapshots = []
    stream = _json(STREAM_STATUS)
    free_gb = shutil.disk_usage(DATA).free / (1024 ** 3)
    checks: list[dict[str, str]] = []

    if using_postgres():
        try:
            with _timed("classification_store ping"), connect_store(
                CLASSIFICATION_DB, read_only=True
            ):
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

    # Each of these has its own dedicated Postgres env var (see
    # _database_counts) -- checking the local file's mere existence would
    # be wrong once that subsystem has moved to Postgres, since the local
    # path is then an unused leftover, not a missing database.
    postgres_backed = (
        ("Pipeline queue", QUEUE_DB, PIPELINE_DATABASE_ENV_VAR, None),
        (".AI search database", AI_SEARCH_DB, AI_SEARCH_DATABASE_ENV_VAR, connect_ai_search),
        ("Zone database", ZONE_DB, ZONE_UNIVERSE_DATABASE_ENV_VAR, connect_zone_universe),
    )
    remaining = [] if using_postgres() else [("Classification database", CLASSIFICATION_DB)]
    for name, path, env_var, connect_fn in postgres_backed:
        if not using_postgres(env_var):
            remaining.append((name, path))
            continue
        try:
            with _timed(f"{name} ping"):
                if connect_fn is not None:
                    with connect_fn(path, read_only=True):
                        pass
                else:
                    connect_postgres(read_only=True, env_var=env_var).close()
            checks.append({
                "Check": name,
                "Status": "Healthy",
                "Detail": "Connected (Postgres)",
            })
        except Exception as exc:
            checks.append({
                "Check": name,
                "Status": "Error",
                "Detail": f"{type(exc).__name__}: {exc}",
            })

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

    # Corpus search was retired (157a496 removed the indexing call from both
    # crawlers), so "every page is indexed" is no longer an invariant -- it is
    # a description of a feature that no longer exists. Asserting it made this
    # a permanent Error the moment either corpus retained a page: .COM/.APP
    # read "4 pages, 0 indexed, Error", and .AI passed only because 0 == 0 and
    # would have failed on its first page too.
    #
    # What is still worth reporting is how many pages retention is holding,
    # and any index rows left over from before the retirement -- those point at
    # pages that may since have been deleted, and nothing writes new ones.
    for prefix, label in (("ai", ".AI"), ("zone", ".COM/.APP")):
        pages = counts[f"{prefix}_pages"]
        indexed = counts[f"{prefix}_indexed"]
        orphans = counts[f"{prefix}_orphans"]
        leftover = (
            f" · {indexed:,} stale index row(s) from before search was retired"
            if indexed else ""
        )
        checks.append({
            "Check": f"{label} retained pages",
            "Status": "Healthy" if orphans == 0 else "Warning",
            "Detail": (
                f"{pages:,} pages retained{leftover}"
                + (f" · {orphans:,} orphans" if orphans else "")
            ),
        })

    disk_total_bytes = shutil.disk_usage(DATA).total
    free_ratio = free_gb / (disk_total_bytes / (1024**3)) if disk_total_bytes else 0.0
    # A flat "20 GB free" threshold assumed a local disk with hundreds of GB
    # to spare -- on a cloud volume provisioned at 5 GB total, that bar can
    # never be met even at 0% used, so it was a permanent false Warning.
    # Treat either a large absolute cushion or a healthy fraction of a small
    # volume as fine.
    checks.append({
        "Check": "Free disk reserve",
        "Status": (
            "Healthy" if free_gb >= 20 or free_ratio >= 0.15 else "Warning"
        ),
        "Detail": f"{free_gb:.1f} GB free",
    })
    pipeline_running_raw = runtime.get("services", {}).get("pipeline", {}).get("running")
    if pipeline_running_raw is None:
        # Cloud mode: no local PID to check, so we can't tell "actively
        # processing" from "stuck" this way. Don't turn a normal nonzero
        # claim count into a permanent false Warning -- see runtime_status().
        checks.append({
            "Check": "Stuck pipeline claims",
            "Status": "Healthy",
            "Detail": (
                f"{counts['pipeline_processing']:,} in progress "
                "(cloud-managed, liveness unknown)"
            ),
        })
    else:
        pipeline_running = bool(pipeline_running_raw)
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
    snapshots_checked = include_integrity or cached_snapshots
    checks.append({
        "Check": "Manual truth backup",
        "Status": "Healthy" if valid_snapshots else ("Warning" if snapshots_checked else "Healthy"),
        "Detail": (
            f"{valid_snapshots:,} valid snapshot(s)"
            if valid_snapshots
            else ("No snapshot created yet" if snapshots_checked else "Run full checks to validate backups")
        ),
    })

    status_rank = {"Healthy": 0, "Warning": 1, "Error": 2}
    worst = max(
        (status_rank[row["Status"]] for row in checks),
        default=0,
    )
    overall = ("Healthy", "Warning", "Error")[worst]
    stream_age = _timestamp_age_seconds(stream.get("updated_at"))
    with _timed("performance_metrics"):
        performance = performance_metrics()
    return {
        "overall": overall,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "counts": counts,
        "runtime": runtime,
        "manual_snapshots": snapshots,
        "performance": performance,
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
