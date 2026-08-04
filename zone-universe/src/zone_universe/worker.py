from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path

# Running as its own standalone service (zone-worker), so the repo root
# (holding runtime_control.py) isn't guaranteed to already be on sys.path.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from runtime_control import RUNTIME_MODE, read_control

from .config import PID_FILE, STOP_FILE, ZONEFILES, Settings, load_settings
from .crawler import crawl_once, queue_candidate_registrations
from .database import connect, log_event, set_state
from .rdap import (
    RegistrationRecord,
    automatic_lookups_today,
    claim_registration,
    finish_registration,
    lookup_and_save,
    recover_registration_queue,
)
from .zone_parser import import_zone, source_fingerprint


def retire_zonefile(conn, path: Path, *, tld: str, queue_version: str) -> int:
    """Delete a re-downloadable source only after its queue is durable."""
    imported_size = path.stat().st_size
    path.unlink()
    set_state(
        conn,
        "last_retired_zonefile",
        {
            "tld": tld,
            "bytes_released": imported_size,
            "queue_version": queue_version,
        },
    )
    return imported_size


def downloaded_zone(tld: str) -> Path | None:
    for name in (
        f"{tld}.txt.gz",
        f"{tld}.zone.gz",
        f"{tld}.zone",
    ):
        path = ZONEFILES / name
        if path.exists() and path.is_file():
            return path
    return None


async def import_new_zonefiles(
    conn,
    settings: Settings,
) -> list[str]:
    if not settings.auto_import_zones:
        return []
    imported: list[str] = []
    for tld in settings.tlds:
        path = downloaded_zone(tld)
        if path is None:
            continue
        fingerprint = source_fingerprint(path)
        existing = conn.execute(
            """
            SELECT source_fingerprint,import_status
            FROM zones WHERE tld=?
            """,
            (tld,),
        ).fetchone()
        if (
            existing is not None
            and str(existing["source_fingerprint"]) == fingerprint
            and str(existing["import_status"]) == "ready"
        ):
            if settings.delete_imported_zonefiles:
                zone = conn.execute(
                    "SELECT active_queue_version FROM zones WHERE tld=?",
                    (tld,),
                ).fetchone()
                retire_zonefile(
                    conn,
                    path,
                    tld=tld,
                    queue_version=str(zone["active_queue_version"]),
                )
            continue
        set_state(conn, "crawler_status", f"importing_{tld}")
        set_state(conn, "active_zone_import", str(path))
        result = await asyncio.to_thread(
            import_zone,
            path,
            tld=tld,
            shard_count=64,
            database=settings.database,
        )
        imported.append(tld)
        set_state(
            conn,
            "last_zone_import",
            {
                "tld": tld,
                "domains": result["delegated_domains"],
                "rejected": result["rejected_gibberish"],
            },
        )
        if settings.delete_imported_zonefiles:
            retire_zonefile(
                conn,
                path,
                tld=tld,
                queue_version=str(result["queue_version"]),
            )
    return imported


async def wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def enrich_registration_dates(
    settings: Settings,
    stop: asyncio.Event,
    should_pause,
) -> None:
    """Run low-volume RDAP lookups independently of website crawling."""
    if not settings.auto_registration:
        return
    recover_registration_queue(database=settings.database)
    while not stop.is_set() and not STOP_FILE.exists():
        if should_pause():
            await wait_or_stop(stop, 5)
            continue
        if (
            automatic_lookups_today(database=settings.database)
            >= settings.registration_daily_limit
        ):
            await wait_or_stop(stop, 60)
            continue
        domain = claim_registration(database=settings.database)
        if not domain:
            await wait_or_stop(stop, settings.poll_seconds)
            continue
        try:
            record = await asyncio.to_thread(
                lookup_and_save,
                domain,
                database=settings.database,
                timeout=settings.registration_timeout_seconds,
            )
        except Exception as exc:
            record = RegistrationRecord(
                domain=domain,
                tld=domain.rsplit(".", 1)[-1],
                lookup_status="error",
                lookup_error=f"{type(exc).__name__}: {exc}"[:300],
            )
        finish_registration(record, database=settings.database)
        with connect(settings.database) as status_conn:
            set_state(
                status_conn,
                "last_registration_lookup",
                {
                    "domain": domain,
                    "status": record.lookup_status,
                    "registered": record.registration_at,
                },
            )
        await wait_or_stop(
            stop,
            settings.registration_interval_seconds,
        )


async def run() -> None:
    settings = load_settings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, stop.set)
    STOP_FILE.unlink(missing_ok=True)
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    conn = connect(settings.database)

    def zone_crawler_requested() -> bool:
        if RUNTIME_MODE == "local":
            return True
        return bool(
            read_control()["desired"].get("zone_crawler", True)
        )

    def pause_requested() -> bool:
        return bool(
            stop.is_set()
            or STOP_FILE.exists()
            or not zone_crawler_requested()
        )
    if settings.auto_registration:
        from classification_store import classification_map

        crawled_domains = [
            str(row["domain"])
            for row in conn.execute("SELECT domain FROM pages")
        ]
        backfill_queued = queue_candidate_registrations(
            classification_map(crawled_domains),
            settings,
        )
        set_state(
            conn,
            "registration_backfill_queued",
            backfill_queued,
        )
    set_state(conn, "worker_status", "running")
    log_event(
        conn,
        "worker_started",
        f"TLDs={','.join(settings.tlds)}; "
        f"workers={settings.concurrency}; batch={settings.batch_size}; "
        f"preflight={settings.preflight_enabled}; "
        f"preflight_workers={settings.preflight_concurrency}",
    )
    registration_task = asyncio.create_task(
        enrich_registration_dates(settings, stop, pause_requested)
    )
    try:
        while not stop.is_set() and not STOP_FILE.exists():
            if not zone_crawler_requested():
                set_state(conn, "crawler_status", "paused")
                set_state(conn, "crawler_phase", "paused_by_user")
                await wait_or_stop(stop, 5)
                continue
            try:
                await import_new_zonefiles(conn, settings)
            except Exception as exc:
                set_state(
                    conn,
                    "last_import_error",
                    f"{type(exc).__name__}: {exc}"[:500],
                )
                log_event(
                    conn,
                    "zone_auto_import_error",
                    f"{type(exc).__name__}: {exc}",
                )
            made_progress = False
            for tld in settings.tlds:
                if stop.is_set() or STOP_FILE.exists():
                    break
                try:
                    set_state(conn, "crawler_status", f"crawling_{tld}")
                    counts = await crawl_once(
                        conn,
                        settings,
                        tld=tld,
                        should_pause=pause_requested,
                    )
                    made_progress = made_progress or bool(counts)
                    if pause_requested():
                        set_state(conn, "crawler_status", "paused")
                        break
                except RuntimeError as exc:
                    set_state(conn, "last_error", str(exc))
                    if str(exc).startswith("low_disk_space"):
                        await asyncio.sleep(30)
                except Exception as exc:
                    set_state(
                        conn,
                        "last_error",
                        f"{type(exc).__name__}: {exc}"[:500],
                    )
                    log_event(
                        conn,
                        "worker_error",
                        f"{type(exc).__name__}: {exc}",
                        tld=tld,
                    )
                    await asyncio.sleep(2)
            if not made_progress:
                set_state(conn, "crawler_status", "idle")
                try:
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=settings.poll_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
    finally:
        registration_task.cancel()
        await asyncio.gather(registration_task, return_exceptions=True)
        set_state(conn, "worker_status", "stopped")
        set_state(conn, "crawler_status", "stopped")
        log_event(conn, "worker_stopped", "Zone crawler stopped.")
        conn.close()
        STOP_FILE.unlink(missing_ok=True)
        PID_FILE.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    asyncio.run(run())


if __name__ == "__main__":
    main()
