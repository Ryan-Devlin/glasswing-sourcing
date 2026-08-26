#!/usr/bin/env python3
"""Stop every automated operation until somebody deliberately turns it back on.

Two switches, because one is not enough:

  * every service's desired flag goes false, which stops the listener, the
    .AI crawler and the zone crawler at the next poll
  * automation goes off, which stops the *schedulers* -- the 8am ET window
    that would otherwise start the pipeline again tomorrow morning, and the
    founder collection that runs every 24 hours on its own clock

Flipping only the first is the mistake worth naming: the daily window exists
specifically to override a pause, so a paused pipeline comes back at 08:00 ET
looking like nothing happened.

This does not stop Railway from billing for the containers. The processes go
idle, but idle containers still cost money -- to actually stop spending, the
services have to be scaled down or removed in Railway itself.

    BACKFILL_REPO=/app python3 pause_everything.py          # show state
    BACKFILL_REPO=/app APPLY=1 python3 pause_everything.py  # stop everything
    BACKFILL_REPO=/app APPLY=1 RESUME=1 python3 pause_everything.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(os.environ.get("BACKFILL_REPO", ".")).resolve()
sys.path.insert(0, str(REPO))
os.chdir(REPO)

from runtime_control import (  # noqa: E402
    SERVICES,
    automation_enabled,
    read_control,
    request_services,
    runtime_status,
    set_automation_enabled,
)

APPLY = os.environ.get("APPLY", "") == "1"
RESUME = os.environ.get("RESUME", "") == "1"


def report(label: str) -> None:
    status = runtime_status()
    print(f"--- {label} ---")
    print(f"automation: {'on' if automation_enabled() else 'OFF'}")
    for name, info in (status.get("services") or {}).items():
        print(f"  {name:14} desired={info.get('desired')} "
              f"actual={info.get('actual_status')}")


def main() -> None:
    report("before")
    if not APPLY:
        print("\nread-only -- set APPLY=1 to change anything")
        return

    want = bool(RESUME)
    request_services(
        {service: want for service in SERVICES},
        requested_by="operator_shutdown" if not want else "operator_resume",
    )
    set_automation_enabled(want)

    # Read it back rather than trusting the write: this is the switch that has
    # to still be off tomorrow morning.
    print()
    report("after")
    control = read_control()["desired"]
    wrong = [s for s in SERVICES if bool(control.get(s, False)) != want]
    if wrong or automation_enabled() != want:
        print(f"VERIFY FAILED: {wrong or 'automation flag'} did not take",
              file=sys.stderr)
        raise SystemExit(1)
    print(f"\nall services and the schedulers are "
          f"{'running' if want else 'stopped'}")
    if not want:
        print("Containers are idle but still billed -- scale them down in "
              "Railway to stop spending.")


if __name__ == "__main__":
    main()
