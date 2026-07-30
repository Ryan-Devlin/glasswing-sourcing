#!/usr/bin/env python3
"""Restart desired local workers with bounded exponential backoff."""

from __future__ import annotations

import signal
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from common import atomic_write_json
from runtime_control import (
    SERVICES,
    SUPERVISOR_PID,
    SUPERVISOR_STATUS,
    process_alive,
    read_control,
    service_pid,
    start_service,
    stop_service,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    stopping = False

    def request_stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    SUPERVISOR_PID.parent.mkdir(parents=True, exist_ok=True)
    SUPERVISOR_PID.write_text(str(__import__("os").getpid()), encoding="utf-8")
    restart_history: dict[str, deque[float]] = defaultdict(deque)
    next_attempt: dict[str, float] = defaultdict(float)
    errors: dict[str, str] = {}
    try:
        while not stopping:
            control = read_control()
            now = time.time()
            service_rows: dict[str, object] = {}
            for service in SERVICES:
                desired = bool(control["desired"][service])
                pid = service_pid(service)
                running = process_alive(pid)
                while (
                    restart_history[service]
                    and restart_history[service][0] < now - 3600
                ):
                    restart_history[service].popleft()
                if desired and not running and now >= next_attempt[service]:
                    if len(restart_history[service]) >= 5:
                        errors[service] = (
                            "Restart limit reached: 5 attempts in one hour."
                        )
                    else:
                        try:
                            start_service(service)
                            restart_history[service].append(now)
                            delay = min(
                                300,
                                5 * (2 ** max(
                                    0, len(restart_history[service]) - 1
                                )),
                            )
                            next_attempt[service] = now + delay
                            errors[service] = ""
                        except Exception as exc:
                            errors[service] = (
                                f"{type(exc).__name__}: {exc}"
                            )[:500]
                            next_attempt[service] = now + 15
                elif not desired and running:
                    try:
                        stop_service(service)
                        errors[service] = ""
                    except Exception as exc:
                        errors[service] = (
                            f"{type(exc).__name__}: {exc}"
                        )[:500]
                pid = service_pid(service)
                service_rows[service] = {
                    "desired": desired,
                    "running": process_alive(pid),
                    "pid": pid,
                    "restarts_last_hour": len(restart_history[service]),
                    "next_restart_at": (
                        datetime.fromtimestamp(
                            next_attempt[service],
                            tz=timezone.utc,
                        ).isoformat()
                        if next_attempt[service] > now
                        else ""
                    ),
                    "last_error": errors.get(service, ""),
                }
            atomic_write_json(
                SUPERVISOR_STATUS,
                {
                    "running": True,
                    "updated_at": utc_now(),
                    "services": service_rows,
                },
            )
            time.sleep(5)
    finally:
        atomic_write_json(
            SUPERVISOR_STATUS,
            {"running": False, "updated_at": utc_now()},
        )
        SUPERVISOR_PID.unlink(missing_ok=True)


if __name__ == "__main__":
    main()

