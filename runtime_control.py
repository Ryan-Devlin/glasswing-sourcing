"""Local worker control behind a cloud-replaceable interface."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
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


RUNTIME_MODE = os.getenv("GLASSWING_RUNTIME_MODE", "local").strip().lower()
CONTROL_PATH = DATA / "runtime_control.json"
SUPERVISOR_PID = LOGS / "worker_supervisor.pid"
SUPERVISOR_STATUS = LOGS / "worker_supervisor_status.json"
SUPERVISOR_LOG = LOGS / "worker_supervisor.log"
AI_PROJECT = ROOT / "ai-domain-search-engine"
ZONE_PROJECT = ROOT / "zone-universe"
AI_PID = AI_PROJECT / "logs" / "worker.pid"
ZONE_PID = ZONE_PROJECT / "logs" / "worker.pid"

SERVICES = ("pipeline", "ai_crawler", "zone_crawler")


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


def read_control() -> dict[str, object]:
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


def request_services(
    desired: dict[str, bool],
    *,
    requested_by: str = "system_health",
) -> dict[str, object]:
    if RUNTIME_MODE != "local":
        raise RuntimeError(
            "Process controls are delegated to the cloud runtime."
        )
    current = read_control()
    merged = dict(current["desired"])
    for service, value in desired.items():
        if service not in SERVICES:
            raise ValueError(f"Unknown service: {service}")
        merged[service] = bool(value)
    payload = {
        "schema_version": 1,
        "runtime_mode": RUNTIME_MODE,
        "desired": merged,
        "updated_at": datetime.now(timezone.utc).isoformat(),
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
            service: {
                "desired": bool(control["desired"][service]),
                "pid": service_pid(service),
                "running": process_alive(service_pid(service)),
            }
            for service in SERVICES
        },
    }
