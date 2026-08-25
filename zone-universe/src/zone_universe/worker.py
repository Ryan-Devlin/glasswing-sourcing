from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import errno
import fcntl
import os
import shutil
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Running as its own standalone service (zone-worker), so the repo root
# (holding runtime_control.py) isn't guaranteed to already be on sys.path.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from runtime_control import (
    RUNTIME_MODE,
    read_control,
    request_services,
    service_requested_by,
)

from .config import (
    PID_FILE,
    QUEUES,
    STOP_FILE,
    ZONEFILES,
    Settings,
    load_settings,
)
from .crawler import (
    crawl_once,
    discard_cached_pages,
    queue_candidate_registrations,
)
from .database import connect, diagnostics, log_event, set_state
from .rdap import (
    RegistrationRecord,
    automatic_lookups_today,
    claim_registration,
    finish_registration,
    lookup_and_save,
    recover_registration_queue,
)
from .zone_parser import import_zone, source_fingerprint


CZDS_CLIENT = _REPO_ROOT / "zone-universe" / "czds.py"
SNAPSHOT_TIMEZONE = ZoneInfo("America/New_York")
SNAPSHOT_HOUR_ET = 6
SNAPSHOT_CHECK_SECONDS = 300
SNAPSHOT_RETRY_SECONDS = 900


def scheduled_snapshot_day(now: datetime | None = None) -> str:
    """Return today's ET schedule key once the 6 AM window has opened."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    eastern = current.astimezone(SNAPSHOT_TIMEZONE)
    if eastern.hour < SNAPSHOT_HOUR_ET:
        return ""
    return eastern.date().isoformat()


def snapshot_day_key(tld: str) -> str:
    """State key recording the day a single zone last imported successfully."""
    return f"last_czds_snapshot_day_{tld}"


def _state_value(conn, key: str) -> str:
    row = conn.execute(
        "SELECT value FROM worker_state WHERE key=?", (key,)
    ).fetchone()
    return str(row["value"]) if row is not None else ""


def open_stream_for_write(fifo: Path, importer, *, timeout: float = 300.0):
    """Open the FIFO write end without deadlocking on an importer that died.

    A blocking write-open on a FIFO waits forever for a reader.  The importer
    creates a database row, 64 shard files and three gzip handles before it
    opens the read end, so anything that fails in there -- ENOSPC being the
    realistic one -- leaves the producer waiting on a reader that will never
    arrive.  Poll with ``O_NONBLOCK`` instead, which fails with ENXIO until a
    reader attaches, and stop as soon as the importer has finished, so the
    caller sees the importer's real error rather than a hang.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            descriptor = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
        except OSError as exc:
            if exc.errno != errno.ENXIO:
                raise
            if importer.done():
                # Re-raises whatever killed the importer; only reached with a
                # clean result if it somehow exited without reading anything.
                importer.result()
                raise OSError(
                    errno.EPIPE,
                    "zone importer exited without reading the stream",
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"zone importer never opened {fifo.name} for reading"
                )
            time.sleep(0.1)
            continue
        # Back to blocking for the copy itself: non-blocking pipe writes fail
        # with EAGAIN every time the importer falls behind the download.
        flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
        fcntl.fcntl(descriptor, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)
        return os.fdopen(descriptor, "wb", buffering=0)


def stream_czds_zone(settings: Settings, tld: str) -> dict[str, object]:
    """Pipe one authenticated CZDS response directly through the importer."""
    if str(CZDS_CLIENT.parent) not in sys.path:
        sys.path.insert(0, str(CZDS_CLIENT.parent))
    import czds

    session = czds.Session(
        os.environ["CZDS_USERNAME"],
        os.environ["CZDS_PASSWORD"],
    )
    url = czds.select_links(session.links(), [tld])[0]
    fifo = ZONEFILES / f".{tld}-{uuid.uuid4().hex}.stream.gz"
    os.mkfifo(fifo, mode=0o600)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            imported = pool.submit(
                import_zone,
                fifo,
                tld=tld,
                shard_count=64,
                database=settings.database,
            )
            try:
                with session.get(url, timeout=3600) as response:
                    with open_stream_for_write(fifo, imported) as output:
                        shutil.copyfileobj(response, output, 1024 * 1024)
            except Exception:
                if imported.done():
                    # An importer that already failed closed the read end, so
                    # the download only ever sees a broken pipe. Report the
                    # cause, not the symptom.
                    imported.result()
                    raise
                # Nothing consumed the stream yet: opening and closing the
                # write end is what lets the importer see EOF and unwind
                # instead of blocking on the FIFO forever.
                try:
                    open_stream_for_write(fifo, imported, timeout=30.0).close()
                except Exception:
                    pass
                raise
            return imported.result()
    finally:
        fifo.unlink(missing_ok=True)


async def download_scheduled_snapshots(conn, settings: Settings) -> bool:
    """Stream/import `.app` and `.com` once daily after 6 AM Eastern."""
    schedule_day = scheduled_snapshot_day()
    if not schedule_day:
        return False
    if _state_value(conn, "last_czds_snapshot_day") == schedule_day:
        return False

    last_attempt = _state_value(conn, "last_czds_attempt_at")
    prior_error = _state_value(conn, "last_czds_download_error")
    credentials_now_available = bool(
        os.getenv("CZDS_USERNAME") and os.getenv("CZDS_PASSWORD")
    )
    recovered_missing_credentials = bool(
        credentials_now_available
        and prior_error.startswith("CZDS_USERNAME and CZDS_PASSWORD")
    )
    if last_attempt and not recovered_missing_credentials:
        try:
            attempted_at = datetime.fromisoformat(
                last_attempt.replace("Z", "+00:00")
            )
            if (
                datetime.now(timezone.utc) - attempted_at
            ).total_seconds() < SNAPSHOT_RETRY_SECONDS:
                return False
        except ValueError:
            pass

    attempt_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    set_state(conn, "last_czds_attempt_at", attempt_at)
    if not os.getenv("CZDS_USERNAME") or not os.getenv("CZDS_PASSWORD"):
        set_state(
            conn,
            "last_czds_download_error",
            "CZDS_USERNAME and CZDS_PASSWORD must be configured",
        )
        return False
    if not CZDS_CLIENT.exists():
        set_state(conn, "last_czds_download_error", "CZDS client is missing")
        return False

    ZONEFILES.mkdir(parents=True, exist_ok=True)
    set_state(conn, "last_czds_download_error", "")
    imported: dict[str, dict[str, object]] = {}
    try:
        for tld in settings.tlds:
            # Re-importing a zone the same day destroys that day's delta: the
            # second run diffs the day's file against the snapshot the first
            # run just wrote, so added_domains collapses to zero and the newly
            # delegated names are never queued. Because success is only
            # recorded once every TLD finishes, a failure in one zone used to
            # drag the zones that already succeeded through exactly that.
            # Confirmed live: a killed `.com` import made the 14:36 retry
            # re-import `.app`, which reported "+0 new today" against its own
            # 14:08 snapshot.
            if _state_value(conn, snapshot_day_key(tld)) == schedule_day:
                continue
            set_state(conn, "crawler_status", f"streaming_daily_{tld}_snapshot")
            result = await asyncio.wait_for(
                asyncio.to_thread(stream_czds_zone, settings, tld),
                timeout=7200,
            )
            imported[tld] = result
            # CZDS sometimes serves yesterday's file again. When it does, the
            # diff is against content we already hold, so added and removed
            # both come back zero and the crawler has nothing queued -- which
            # on the dashboard is indistinguishable from a working day with no
            # new domains, and .app did it silently on 2026-08-23 before both
            # zones did on 2026-08-25.
            #
            # A real day always moves both counters: .com runs 100-165k added
            # against 95-128k removed, .app a few thousand each. Zero of both
            # against an existing prior snapshot means the same file.
            #
            # So the day is not recorded as done, and the existing
            # SNAPSHOT_RETRY_SECONDS backoff tries again later. Retrying is
            # safe here for the same reason the delta was empty: the snapshot
            # just written is byte-identical to the one before it, so a later
            # import diffs tomorrow's real file against yesterday's content and
            # produces the correct delta. That is *not* true after a successful
            # import, which is why the guard above still exists.
            # unchanged_domains only counts when there was a prior snapshot
            # to compare against, which is what separates a repeat from a
            # first import. The first import of a zone is deliberately a
            # baseline -- it records the universe without queueing 163 million
            # existing domains -- so it also reports zero added and zero
            # removed, and without this it would be mistaken for a repeat and
            # retried forever.
            repeated = (
                int(result.get("added_domains") or 0) == 0
                and int(result.get("removed_domains") or 0) == 0
                and int(result.get("unchanged_domains") or 0) > 0
            )
            result["repeated_snapshot"] = repeated
            if repeated:
                log_event(
                    conn,
                    "czds_repeat_snapshot",
                    f"{tld}: CZDS served a snapshot identical to the previous "
                    f"one ({int(result.get('delegated_domains') or 0):,} "
                    "domains, no additions or removals). Not counting the day "
                    "as done; will retry.",
                    tld=tld,
                )
                continue
            set_state(conn, snapshot_day_key(tld), schedule_day)
            set_state(
                conn,
                "last_zone_import",
                {
                    "tld": tld,
                    "domains": result["delegated_domains"],
                    "added": result["added_domains"],
                    "removed": result["removed_domains"],
                },
            )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"[:1000]
        set_state(conn, "last_czds_download_error", error)
        log_event(conn, "czds_stream_import_error", error)
        return False

    repeated_tlds = sorted(
        tld for tld, result in imported.items()
        if result.get("repeated_snapshot")
    )
    set_state(conn, "last_czds_repeat_snapshot_tlds", repeated_tlds)
    # Only close the day out once every zone actually produced a fresh delta.
    # Leaving it open is what lets the retry happen at all.
    if not repeated_tlds:
        set_state(conn, "last_czds_snapshot_day", schedule_day)
    set_state(conn, "last_czds_download_error", "")
    set_state(
        conn,
        "last_czds_download_output",
        {
            tld: {
                "domains": result["delegated_domains"],
                "added": result["added_domains"],
                "removed": result["removed_domains"],
            }
            for tld, result in imported.items()
        },
    )
    fresh = [tld for tld in settings.tlds if tld not in repeated_tlds]
    log_event(
        conn,
        "czds_daily_snapshots_downloaded",
        f"Stream-imported {','.join(fresh) or 'no'} fresh snapshots for "
        f"{schedule_day}."
        + (
            f" {','.join(repeated_tlds)} repeated the previous snapshot and "
            "will be retried."
            if repeated_tlds else ""
        ),
    )
    return True


def retire_full_universe_queues(conn) -> dict[str, int]:
    """Remove legacy non-delta crawl shards without touching snapshots.

    Older imports stored unchanged/baseline domains in priority bands 1-4.
    Daily-delta imports use priority 0 exclusively.  Deleting those rows and
    their plain-text queue files prevents the old 163M-domain backlog from
    ever running again; compressed snapshots remain available for tomorrow's
    set difference.
    """
    rows = conn.execute(
        "SELECT tld,queue_version,path FROM shards WHERE priority<>0"
    ).fetchall()
    bytes_released = 0
    files_removed = 0
    for row in rows:
        path = Path(str(row["path"]))
        try:
            bytes_released += path.stat().st_size
            path.unlink()
            files_removed += 1
        except FileNotFoundError:
            pass
    conn.execute("DELETE FROM shards WHERE priority<>0")
    conn.commit()
    result = {
        "shards_removed": len(rows),
        "files_removed": files_removed,
        "bytes_released": bytes_released,
    }
    set_state(conn, "daily_delta_queue_cleanup", result)
    return result


def reclaim_orphaned_queue_directories(
    conn,
    queue_root: Path = QUEUES,
) -> dict[str, object]:
    """Delete queue directories that no shards row and no zone points at.

    An import that failed after renaming its build directory into place left
    the whole thing behind -- snapshot plus 64 shards, several hundred MB for
    `.com` -- with nothing referencing it, so remove_obsolete_queue_versions
    could not find it either.  A handful of those fills the 5 GB volume and
    every daily snapshot after that dies on ENOSPC.

    Only safe at worker startup: nothing else is importing yet, so a leftover
    `.building-` directory can only be debris from a process that was killed
    mid-import (a redeploy), never a build in progress.
    """
    if not queue_root.exists():
        return {"directories_removed": 0, "bytes_released": 0, "paths": []}
    live = {
        Path(str(row["path"])).parent
        for row in conn.execute("SELECT path FROM shards")
        if str(row["path"])
    }
    live |= {
        Path(str(row["snapshot_path"])).parent
        for row in conn.execute("SELECT snapshot_path FROM zones")
        if str(row["snapshot_path"])
    }
    removed: list[str] = []
    bytes_released = 0
    for candidate in sorted(queue_root.glob("*/*")):
        if not candidate.is_dir() or candidate in live:
            continue
        for item in candidate.rglob("*"):
            if item.is_file():
                bytes_released += item.stat().st_size
        shutil.rmtree(candidate, ignore_errors=True)
        removed.append(str(candidate))
    result = {
        "directories_removed": len(removed),
        "bytes_released": bytes_released,
        "paths": removed[:20],
    }
    set_state(conn, "orphaned_queue_cleanup", result)
    if removed:
        log_event(
            conn,
            "orphaned_queues_reclaimed",
            f"Reclaimed {len(removed)} unreferenced queue "
            f"directories ({bytes_released:,} bytes).",
        )
    return result


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

    # Permanently cut over from full-zone crawling to daily set differences.
    # This is idempotent and preserves the compressed snapshot used as the
    # comparison baseline as well as every retained/scored company record.
    retire_full_universe_queues(conn)

    # Runs after the retirement above so it also collects the directories
    # that step empties out. Keeping the volume clear is what keeps the
    # daily CZDS snapshot from failing with "no space left on device".
    reclaim_orphaned_queue_directories(conn)

    # One-time/boot-time retention sweep: older worker versions kept every
    # successful page regardless of its eventual investment classification.
    # Repair that legacy corpus before new crawling resumes.
    existing_pages = [
        str(row["domain"])
        for row in conn.execute("SELECT domain FROM pages").fetchall()
    ]
    if existing_pages:
        from ai_search.extract import same_site_url
        from classification_store import (
            discard_uninvestable_classifications,
            query_classifications_by_domains,
        )

        classifications = {
            str(row["domain"]): row
            for row in query_classifications_by_domains(existing_pages)
        }
        stale_domains = [
            domain
            for domain in existing_pages
            if not (
                same_site_url(
                    str(classifications.get(domain, {}).get("final_url", "")),
                    f"https://{domain}",
                )
                and (
                    str(classifications.get(domain, {}).get("routing_bucket", ""))
                    == "priority"
                    or (
                        str(classifications.get(domain, {}).get("routing_bucket", ""))
                        == "review"
                        and int(classifications.get(domain, {}).get("fit_score", 0) or 0)
                        >= settings.html_archive_minimum_score
                    )
                )
            )
        ]
        pages_removed = discard_cached_pages(conn, stale_domains)
        classifications_removed = discard_uninvestable_classifications(
            stale_domains
        )
        set_state(
            conn,
            "last_retention_sweep",
            {
                "checked": len(existing_pages),
                "retained_qualified": len(existing_pages) - pages_removed,
                "pages_removed": pages_removed,
                "classifications_removed": classifications_removed,
            },
        )

    # Historical per-domain failure TSVs duplicate shard counters and grow
    # without bound. They contain no investable companies and are safe to
    # retire under the discard-only retention policy.
    failure_root = settings.database.parent / "failures"
    retired_failure_bytes = 0
    if failure_root.exists():
        for failure_path in failure_root.glob("*.tsv"):
            retired_failure_bytes += failure_path.stat().st_size
            failure_path.unlink()
    set_state(conn, "retired_failure_log_bytes", retired_failure_bytes)

    from .queue_migration import migrate_queue_files_from_object_storage

    migration = migrate_queue_files_from_object_storage(conn, settings)
    set_state(conn, "last_queue_migration", migration)

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

    def coverage_complete() -> bool:
        stats = diagnostics(conn)
        queued = int(stats.get("queued_domains", 0))
        attempted = int(stats.get("attempted", 0))
        return queued > 0 and attempted >= queued
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
    AUTO_PAUSE_REASON = "auto_full_coverage"
    next_snapshot_check = 0.0
    next_import_check = 0.0
    snapshot_task: asyncio.Task | None = None

    async def scheduled_snapshot_cycle() -> None:
        """Stream and import the daily snapshots on their own connection.

        The crawl loop commits on `conn` continuously; sharing one connection
        across both would let either side commit the other's half-finished
        transaction.
        """
        with connect(settings.database) as snapshot_conn:
            downloaded = await download_scheduled_snapshots(
                snapshot_conn, settings
            )
            if downloaded:
                await import_new_zonefiles(snapshot_conn, settings)

    try:
        while not stop.is_set() and not STOP_FILE.exists():
            now_monotonic = time.monotonic()
            if snapshot_task is not None and snapshot_task.done():
                try:
                    snapshot_task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    set_state(
                        conn,
                        "last_czds_download_error",
                        f"{type(exc).__name__}: {exc}"[:1000],
                    )
                    log_event(
                        conn,
                        "czds_download_error",
                        f"{type(exc).__name__}: {exc}",
                    )
                snapshot_task = None
            # Deliberately not awaited: a `.com` stream runs for roughly an
            # hour, and awaiting it inline stalled every crawl for that whole
            # window once a day. Confirmed live: 71,988 queued `.app` domains
            # sat untouched behind `streaming_daily_com_snapshot` with the
            # crawler already resumed. One cycle at a time, so a slow import
            # cannot pile up concurrent duplicates of itself.
            if snapshot_task is None and now_monotonic >= next_snapshot_check:
                next_snapshot_check = now_monotonic + SNAPSHOT_CHECK_SECONDS
                snapshot_task = asyncio.create_task(
                    scheduled_snapshot_cycle()
                )
            # The slower scan supports an operator dropping a file in by hand
            # without turning the worker into an idle file watcher. Skipped
            # while a snapshot cycle owns the importer.
            if snapshot_task is None and now_monotonic >= next_import_check:
                next_import_check = now_monotonic + SNAPSHOT_CHECK_SECONDS
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
            if not zone_crawler_requested():
                # Coverage can only have auto-paused this; a human pausing
                # it via the dashboard must never be silently overridden.
                if (
                    RUNTIME_MODE != "local"
                    and service_requested_by("zone_crawler") == AUTO_PAUSE_REASON
                    and not coverage_complete()
                ):
                    request_services(
                        {"zone_crawler": True},
                        requested_by="auto_resume_new_domains",
                    )
                    continue
                set_state(conn, "crawler_status", "paused")
                set_state(conn, "crawler_phase", "paused_by_user")
                await wait_or_stop(stop, 5)
                continue
            made_progress = False
            for tld in settings.tlds:
                if stop.is_set() or STOP_FILE.exists():
                    break
                try:
                    set_state(conn, "crawler_status", f"crawling_{tld}")
                    # A hang inside crawl_once (confirmed live: worker stuck
                    # self-reporting "paused by user"/"caught up" while
                    # Quick checks and Admitted to full crawl both stayed at
                    # exactly 0 for hours, despite millions of queued
                    # candidates and a "Desired: running" state) used to
                    # block this entire worker forever -- per-request
                    # timeouts inside crawl_once bound one HTTP fetch, not
                    # the whole call. This outer deadline is deliberately
                    # generous so it only fires on a genuine hang, turning
                    # it into the same logged-error/retry-next-tld path as
                    # any other exception instead of an indefinite stall.
                    counts = await asyncio.wait_for(
                        crawl_once(
                            conn,
                            settings,
                            tld=tld,
                            should_pause=pause_requested,
                        ),
                        timeout=600.0,
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
                if RUNTIME_MODE != "local" and coverage_complete():
                    request_services(
                        {"zone_crawler": False}, requested_by=AUTO_PAUSE_REASON
                    )
                    set_state(conn, "crawler_phase", "paused_full_coverage")
                try:
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=settings.poll_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
    finally:
        registration_task.cancel()
        if snapshot_task is not None:
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)
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
