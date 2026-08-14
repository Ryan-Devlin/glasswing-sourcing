#!/usr/bin/env python3
"""Rate-aware scheduled collector for the native Founder Signals module."""

from __future__ import annotations

import os
import time

from founder_signals import (
    create_monitoring_job,
    founder_settings,
    run_monitoring_job,
)


def run_once() -> dict:
    job_id = create_monitoring_job("github_collection")
    return run_monitoring_job(job_id)


def main() -> None:
    while True:
        try:
            result = run_once()
            print(f"Founder Signals collection: {result}", flush=True)
        except Exception as exc:
            # A failed cycle is recorded by the job runner where possible; the
            # process remains alive so the next configured interval can retry.
            print(f"Founder Signals collection cycle failed: {exc}", flush=True)
        hours = max(1, int(founder_settings().get("collection_frequency_hours", 24)))
        time.sleep(hours * 3600)


if __name__ == "__main__":
    if not os.getenv("GITHUB_TOKEN"):
        print("GITHUB_TOKEN is not configured; collection will record a clear failure.", flush=True)
    main()
