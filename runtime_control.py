"""Local worker control behind a cloud-replaceable interface."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
from zoneinfo import ZoneInfo
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common import (
    DATA,
    LOGS,
    QUEUE_DB,
    ROOT,
    STREAM_PID,
    STREAM_STATUS,
    STREAM_STOP,
    atomic_write_json,
)
from db_backend import (
    acquire_advisory_lock_with_timeout,
    connect_postgres,
    using_postgres,
)


RUNTIME_MODE = os.getenv("GLASSWING_RUNTIME_MODE", "local").strip().lower()
CONTROL_PATH = DATA / "runtime_control.json"
# Deliberately its own env var, not any subsystem's *_DATABASE_URL -- worker
# desired-state isn't "a subsystem", it's cross-cutting ops coordination that
# every service (web + however many worker services) needs to reach the same
# copy of. On Railway's single-Postgres-instance setup this can be the exact
# same connection string as the others, just its own schema, same as every
# other Postgres-backed piece in this app.
RUNTIME_CONTROL_DATABASE_ENV_VAR = "RUNTIME_CONTROL_DATABASE_URL"
SUPERVISOR_PID = LOGS / "worker_supervisor.pid"
SUPERVISOR_STATUS = LOGS / "worker_supervisor_status.json"
SUPERVISOR_LOG = LOGS / "worker_supervisor.log"
AI_PROJECT = ROOT / "ai-domain-search-engine"
ZONE_PROJECT = ROOT / "zone-universe"
AI_PID = AI_PROJECT / "logs" / "worker.pid"
ZONE_PID = ZONE_PROJECT / "logs" / "worker.pid"

SERVICES = ("pipeline", "ai_crawler", "zone_crawler")
_RUNTIME_SCHEMA_LOCK = threading.Lock()
_runtime_schema_initialized_for = ""


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def pid_from_file(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return 0


def _pgrep(pattern: str) -> list[int]:
    try:
        result = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    pids = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.isdigit():
            pids.append(int(line))
    return pids


def find_supervisor_pids(pattern: str = "worker_supervisor.py") -> list[int]:
    """Find live supervisor processes independent of the PID file.

    The PID file can go stale (deleted out from under a running process,
    lost to a crash before it was written, or a race between two callers)
    while the process itself keeps running untracked. Callers use this to
    reconcile against reality instead of trusting the file alone.
    """
    return [pid for pid in _pgrep(pattern) if process_alive(pid)]


def service_pid(service: str) -> int:
    path = {
        "pipeline": STREAM_PID,
        "ai_crawler": AI_PID,
        "zone_crawler": ZONE_PID,
    }[service]
    return pid_from_file(path)


def _connect_runtime_control():
    global _runtime_schema_initialized_for
    # A fresh Postgres control store gets the modern schema directly -- there
    # is no pre-existing local installation to carry forward, unlike the
    # SQLite/JSON path. Own "runtime" schema so it can't collide with
    # classification_store/pipeline/ai_search/zone_universe, which all
    # likely share the same physical Postgres instance on Railway today.
    database_key = os.getenv(RUNTIME_CONTROL_DATABASE_ENV_VAR, "").strip()
    conn = connect_postgres(env_var=RUNTIME_CONTROL_DATABASE_ENV_VAR)
    if _runtime_schema_initialized_for == database_key:
        conn.execute("SET search_path TO runtime, public")
        return conn
    # Defense in depth, same as every other Postgres-backed subsystem here
    # (see continuous_pipeline._connect_db_postgres): the lock is free when
    # uncontended, so there's no reason to skip it even though this store is
    # currently only touched by one caller (System Health) at a time.
    with _RUNTIME_SCHEMA_LOCK:
        if _runtime_schema_initialized_for != database_key:
            conn.execute("CREATE SCHEMA IF NOT EXISTS runtime")
            conn.commit()
            conn.execute("SET search_path TO runtime, public")
            locked = acquire_advisory_lock_with_timeout(
                conn, "runtime_control_migration"
            )
            try:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS worker_control (
                        service TEXT PRIMARY KEY,
                        desired BOOLEAN NOT NULL DEFAULT FALSE,
                        updated_at TEXT NOT NULL DEFAULT '',
                        requested_by TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS pipeline_status (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        payload TEXT NOT NULL DEFAULT '{}',
                        updated_at TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS schedule_state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS pipeline_config (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        payload TEXT NOT NULL DEFAULT '{}',
                        updated_at TEXT NOT NULL DEFAULT '',
                        requested_by TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                conn.commit()
                _runtime_schema_initialized_for = database_key
            finally:
                if locked:
                    conn.execute(
                        "SELECT pg_advisory_unlock(hashtext(?))",
                        ("runtime_control_migration",),
                    )
        else:
            conn.execute("SET search_path TO runtime, public")
    return conn


def control_backend_available() -> bool:
    """Whether *setting* desired state is possible right now.

    Local mode always can (a local JSON file needs no setup). Cloud mode
    can only if a shared store every service can reach is configured --
    the old local-file store doesn't cross a Railway service boundary."""
    return RUNTIME_MODE == "local" or using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR)


def shared_pipeline_backend_available() -> bool:
    """Whether the web and pipeline services can exchange live state."""
    return using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR)


def _read_shared_pipeline_payload(table: str) -> dict[str, object]:
    if table not in {"pipeline_status", "pipeline_config"}:
        raise ValueError(f"Unknown shared pipeline table: {table}")
    if not shared_pipeline_backend_available():
        return {}
    conn = _connect_runtime_control()
    try:
        row = conn.execute(
            f"SELECT payload, updated_at FROM {table} WHERE singleton = 1"
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    try:
        payload = json.loads(str(row["payload"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if table == "pipeline_status":
        payload["shared_status_updated_at"] = str(row["updated_at"] or "")
    return payload


def read_pipeline_status() -> dict[str, object]:
    """Read the pipeline worker's latest status from the shared cloud store."""
    return _read_shared_pipeline_payload("pipeline_status")


def read_pipeline_config() -> dict[str, object]:
    """Read the dashboard's latest pipeline settings from the shared store."""
    return _read_shared_pipeline_payload("pipeline_config")


def _publish_shared_pipeline_payload(
    table: str,
    payload: dict[str, object],
    *,
    requested_by: str = "",
) -> None:
    if table not in {"pipeline_status", "pipeline_config"}:
        raise ValueError(f"Unknown shared pipeline table: {table}")
    if not shared_pipeline_backend_available():
        return
    now = datetime.now(timezone.utc).isoformat()
    serialized = json.dumps(payload, separators=(",", ":"), default=str)
    conn = _connect_runtime_control()
    try:
        if table == "pipeline_config":
            conn.execute(
                """
                INSERT INTO pipeline_config
                    (singleton, payload, updated_at, requested_by)
                VALUES (1, ?, ?, ?)
                ON CONFLICT (singleton) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at,
                    requested_by = EXCLUDED.requested_by
                """,
                (serialized, now, requested_by),
            )
        else:
            conn.execute(
                """
                INSERT INTO pipeline_status (singleton, payload, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT (singleton) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at
                """,
                (serialized, now),
            )
        conn.commit()
    finally:
        conn.close()


def publish_pipeline_status(payload: dict[str, object]) -> None:
    """Publish a complete status snapshot for the dashboard."""
    _publish_shared_pipeline_payload("pipeline_status", payload)


def publish_pipeline_config(
    payload: dict[str, object],
    *,
    requested_by: str = "dashboard",
) -> None:
    """Publish pipeline settings so the separate worker sees changes."""
    _publish_shared_pipeline_payload(
        "pipeline_config",
        payload,
        requested_by=requested_by,
    )


def read_control() -> dict[str, object]:
    if using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR):
        conn = _connect_runtime_control()
        try:
            rows = conn.execute(
                "SELECT service, desired, updated_at, requested_by FROM worker_control"
            ).fetchall()
        finally:
            conn.close()
        by_service = {row["service"]: row for row in rows}
        latest = max(rows, key=lambda row: row["updated_at"], default=None)
        return {
            "schema_version": 1,
            "runtime_mode": RUNTIME_MODE,
            "desired": {
                service: bool(by_service[service]["desired"])
                if service in by_service
                else False
                for service in SERVICES
            },
            "updated_at": latest["updated_at"] if latest else "",
            "requested_by": latest["requested_by"] if latest else "",
        }
    try:
        payload = json.loads(CONTROL_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    desired = payload.get("desired", {})
    if not isinstance(desired, dict):
        desired = {}
    return {
        "schema_version": 1,
        "runtime_mode": RUNTIME_MODE,
        "desired": {
            service: bool(desired.get(service, False))
            for service in SERVICES
        },
        "updated_at": str(payload.get("updated_at", "")),
        "requested_by": str(payload.get("requested_by", "")),
    }


def service_requested_by(service: str) -> str:
    """Who last set this specific service's desired state.

    read_control()'s top-level requested_by is the single most-recently
    updated row across ALL services, not this one -- callers deciding
    whether to auto-resume a service need to know it was auto-paused
    (not paused by a human) before touching it again.
    """
    if using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR):
        conn = _connect_runtime_control()
        try:
            row = conn.execute(
                "SELECT requested_by FROM worker_control WHERE service=?",
                (service,),
            ).fetchone()
        finally:
            conn.close()
        return str(row["requested_by"]) if row else ""
    try:
        payload = json.loads(CONTROL_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    return str(payload.get("requested_by", ""))


DAILY_WINDOW_TIMEZONE = ZoneInfo("America/New_York")
# Eastern, by instruction: run through the working day and stop overnight.
PIPELINE_WINDOW_OPEN_HOUR = int(os.getenv("PIPELINE_WINDOW_OPEN_HOUR_ET", "8"))
PIPELINE_WINDOW_CLOSE_HOUR = int(os.getenv("PIPELINE_WINDOW_CLOSE_HOUR_ET", "20"))


def scheduled_pipeline_window(
    now: datetime | None = None,
) -> tuple[str, bool]:
    """The window boundary in force right now, and what it wants.

    Returns a stable key naming the boundary -- "2026-08-25:open" -- and the
    desired state. Acting on *boundaries* rather than on the current hour is
    the whole design: a scheduler that continuously enforced "it is 2pm so the
    pipeline should be running" would undo a deliberate mid-afternoon pause
    within the minute. One start each morning, one stop each evening, and the
    controls are left alone in between.

    Before the morning boundary the previous evening's close is still what is
    in force, so a container starting at 3am pauses rather than running until
    8pm carries it.
    """
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    eastern = current.astimezone(DAILY_WINDOW_TIMEZONE)
    if eastern.hour >= PIPELINE_WINDOW_CLOSE_HOUR:
        return f"{eastern.date().isoformat()}:close", False
    if eastern.hour >= PIPELINE_WINDOW_OPEN_HOUR:
        return f"{eastern.date().isoformat()}:open", True
    yesterday = (eastern - timedelta(days=1)).date().isoformat()
    return f"{yesterday}:close", False


AUTOMATION_KEY = "automation_enabled"


def automation_enabled() -> bool:
    """Whether the background schedulers may act at all.

    A pause is not enough to put this system down. The daily window scheduler
    starts the pipeline at 8am ET whether or not somebody paused it the night
    before -- that is the whole point of it -- and the founder collection fires
    every 24 hours on its own clock. So "stop everything until further notice"
    needs a switch those schedulers consult, and it has to outlive a restart,
    which means it lives in the control store rather than in an env var.

    Defaults to enabled, so an empty store behaves exactly as before.
    """
    return read_schedule_state(AUTOMATION_KEY) != "off"


def set_automation_enabled(enabled: bool) -> None:
    write_schedule_state(AUTOMATION_KEY, "on" if enabled else "off")


def read_schedule_state(key: str) -> str:
    if not using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR):
        return ""
    conn = _connect_runtime_control()
    try:
        row = conn.execute(
            "SELECT value FROM schedule_state WHERE key=?", (key,)
        ).fetchone()
        return str(row["value"]) if row else ""
    finally:
        conn.close()


def write_schedule_state(key: str, value: str) -> None:
    if not using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR):
        return
    conn = _connect_runtime_control()
    try:
        conn.execute(
            "INSERT INTO schedule_state (key, value, updated_at) "
            "VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET "
            "value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            (key, value, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def apply_pipeline_window(now: datetime | None = None) -> dict[str, object]:
    """Start or stop the pipeline if a window boundary has just passed."""
    # An indefinite stop has to beat the daily clock. Checked here rather than
    # only in the scheduler so that any caller -- a restart, a manual poke --
    # gets the same answer while automation is off.
    if not automation_enabled():
        return {"applied": False, "reason": "automation_off"}
    boundary, desired = scheduled_pipeline_window(now)
    if read_schedule_state("pipeline_window") == boundary:
        return {"applied": False, "boundary": boundary, "desired": desired}
    request_services({"pipeline": desired}, requested_by=f"daily_window:{boundary}")
    write_schedule_state("pipeline_window", boundary)
    return {"applied": True, "boundary": boundary, "desired": desired}


def request_services(
    desired: dict[str, bool],
    *,
    requested_by: str = "system_health",
) -> dict[str, object]:
    if not control_backend_available():
        raise RuntimeError(
            "No shared control store is configured for cloud mode -- set "
            f"{RUNTIME_CONTROL_DATABASE_ENV_VAR}."
        )
    for service in desired:
        if service not in SERVICES:
            raise ValueError(f"Unknown service: {service}")
    now = datetime.now(timezone.utc).isoformat()
    if using_postgres(RUNTIME_CONTROL_DATABASE_ENV_VAR):
        conn = _connect_runtime_control()
        try:
            for service, value in desired.items():
                conn.execute(
                    """
                    INSERT INTO worker_control (service, desired, updated_at, requested_by)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (service) DO UPDATE SET
                        desired = EXCLUDED.desired,
                        updated_at = EXCLUDED.updated_at,
                        requested_by = EXCLUDED.requested_by
                    """,
                    (service, bool(value), now, requested_by),
                )
            conn.commit()
        finally:
            conn.close()
        return read_control()
    current = read_control()
    merged = dict(current["desired"])
    for service, value in desired.items():
        merged[service] = bool(value)
    payload = {
        "schema_version": 1,
        "runtime_mode": RUNTIME_MODE,
        "desired": merged,
        "updated_at": now,
        "requested_by": requested_by,
    }
    atomic_write_json(CONTROL_PATH, payload)
    return payload


def recover_stuck_jobs() -> dict[str, int]:
    """Release claims left behind by interrupted local workers."""
    recovered = {"pipeline": 0, "registration": 0}
    if QUEUE_DB.exists():
        with sqlite3.connect(QUEUE_DB, timeout=30) as conn:
            cursor = conn.execute(
                """
                UPDATE domains SET
                    status='pending',
                    next_attempt_at=datetime('now'),
                    last_error='supervisor_recovered_interrupted_claim'
                WHERE status='processing'
                """
            )
            recovered["pipeline"] = max(0, int(cursor.rowcount))
            conn.commit()
    try:
        zone_src = str(ZONE_PROJECT / "src")
        if zone_src not in sys.path:
            sys.path.insert(0, zone_src)
        from zone_universe.rdap import recover_registration_queue

        recovered["registration"] = int(recover_registration_queue())
    except (ImportError, sqlite3.Error):
        pass
    return recovered


def _run_script(path: Path) -> str:
    result = subprocess.run(
        [str(path)],
        cwd=path.parent,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            (result.stderr or result.stdout or "worker script failed").strip()
        )
    return (result.stdout or result.stderr).strip()


def start_service(service: str) -> str:
    if service_pid(service) and process_alive(service_pid(service)):
        return "already_running"
    recover_stuck_jobs()
    if service == "ai_crawler":
        return _run_script(AI_PROJECT / "start_worker.sh")
    if service == "zone_crawler":
        return _run_script(ZONE_PROJECT / "start_worker.sh")
    if service != "pipeline":
        raise ValueError(f"Unknown service: {service}")
    STREAM_STOP.unlink(missing_ok=True)
    log_path = LOGS / "continuous_pipeline.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        process = subprocess.Popen(
            [str(ROOT / ".venv" / "bin" / "python"),
             str(ROOT / "continuous_pipeline.py")],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    deadline = time.time() + 3
    while time.time() < deadline:
        pid = service_pid("pipeline")
        if pid and process_alive(pid):
            return f"Started continuous pipeline (PID {pid})."
        if process.poll() is not None:
            raise RuntimeError(
                f"Continuous pipeline exited with code {process.returncode}."
            )
        time.sleep(0.1)
    return f"Continuous pipeline launch requested (PID {process.pid})."


def stop_service(service: str) -> str:
    pid = service_pid(service)
    if not pid or not process_alive(pid):
        return "already_stopped"
    if service == "ai_crawler":
        return _run_script(AI_PROJECT / "stop_worker.sh")
    if service == "zone_crawler":
        return _run_script(ZONE_PROJECT / "stop_worker.sh")
    if service != "pipeline":
        raise ValueError(f"Unknown service: {service}")
    STREAM_STOP.write_text("stop", encoding="utf-8")
    return "Continuous pipeline stop requested."


def ensure_supervisor() -> int:
    if RUNTIME_MODE != "local":
        return 0
    pid = pid_from_file(SUPERVISOR_PID)
    if process_alive(pid):
        return pid
    untracked = find_supervisor_pids()
    if untracked:
        # A supervisor is already running under a PID the file lost track of.
        # Adopt it rather than spawning a second, equally untracked instance.
        adopted = untracked[0]
        SUPERVISOR_PID.write_text(str(adopted), encoding="utf-8")
        return adopted
    SUPERVISOR_PID.unlink(missing_ok=True)
    SUPERVISOR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with SUPERVISOR_LOG.open("ab") as log:
        process = subprocess.Popen(
            [str(ROOT / ".venv" / "bin" / "python"),
             str(ROOT / "worker_supervisor.py")],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    SUPERVISOR_PID.write_text(str(process.pid), encoding="utf-8")
    return process.pid


def stop_supervisor(*, timeout_seconds: float = 10.0) -> bool:
    """Stop every live supervisor process and confirm exit before untracking.

    Signals both the PID-file process and any untracked process found by
    `find_supervisor_pids`, so a previously orphaned supervisor (see
    `ensure_supervisor`) can still be stopped from here. The PID file is only
    cleared once nothing is left alive, or after a bounded force-kill escalates
    past `timeout_seconds`. Returns True if no supervisor process remains.
    """
    pids = set(find_supervisor_pids())
    tracked_pid = pid_from_file(SUPERVISOR_PID)
    if tracked_pid:
        pids.add(tracked_pid)
    for pid in pids:
        if process_alive(pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
    deadline = time.time() + timeout_seconds
    while time.time() < deadline and any(process_alive(pid) for pid in pids):
        time.sleep(0.2)
    survivors = [pid for pid in pids if process_alive(pid)]
    for pid in survivors:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    if survivors:
        time.sleep(0.5)
        survivors = [pid for pid in survivors if process_alive(pid)]
    SUPERVISOR_PID.unlink(missing_ok=True)
    return not survivors


def runtime_status() -> dict[str, object]:
    control = read_control()
    try:
        supervisor = json.loads(
            SUPERVISOR_STATUS.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError):
        supervisor = {}
    supervisor_pid = pid_from_file(SUPERVISOR_PID)
    untracked_pids = [
        pid for pid in find_supervisor_pids() if pid != supervisor_pid
    ]
    return {
        "runtime_mode": RUNTIME_MODE,
        "supervisor": {
            **supervisor,
            # PID files and the OS are authoritative. A status payload can be
            # stale after a clean stop or machine shutdown.
            "pid": supervisor_pid,
            "running": process_alive(supervisor_pid),
            "untracked_pids": untracked_pids,
        },
        "services": {
            service: (
                {
                    "desired": bool(control["desired"][service]),
                    "pid": service_pid(service),
                    "running": process_alive(service_pid(service)),
                }
                if RUNTIME_MODE == "local"
                # A local PID file means nothing once each worker runs as its
                # own separate Railway service -- there's no local process to
                # check on the `web` container, so "running" would always
                # read back False and falsely claim a live cloud worker is
                # stopped. Report it as unknown instead of guessing.
                else {
                    "desired": bool(control["desired"][service]),
                    "pid": None,
                    "running": None,
                }
            )
            for service in SERVICES
        },
    }
