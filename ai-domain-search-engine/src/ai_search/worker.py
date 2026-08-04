from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

# Running as its own standalone service (ai-worker), so the repo root
# (holding runtime_control.py) isn't guaranteed to already be on sys.path.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from runtime_control import RUNTIME_MODE, read_control

from .config import PROJECT_ROOT, load_settings
from .crawler import crawl_pending
from .database import add_domain, connect, set_worker_state, utc_now
from .discovery import stream_domains
from .domains import normalize_domain
from .web_discovery import discover_query, provider_name
from .og_bridge import enqueue_for_screening, screenable_domains

PID_FILE = PROJECT_ROOT / "logs" / "worker.pid"


def claim_worker_pid() -> None:
    """Make the worker, rather than its launcher, own the PID lifecycle."""
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    if PID_FILE.exists():
        try:
            existing = int(PID_FILE.read_text(encoding="utf-8").strip())
            if existing != os.getpid():
                os.kill(existing, 0)
                raise RuntimeError(
                    f"Discovery/crawl worker already running (PID {existing})."
                )
        except (ProcessLookupError, ValueError):
            PID_FILE.unlink(missing_ok=True)
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")


def release_worker_pid() -> None:
    try:
        owned_pid = int(PID_FILE.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return
    if owned_pid == os.getpid():
        PID_FILE.unlink(missing_ok=True)


def sync_crawled_domains_to_original_queue(database) -> dict[str, int]:
    queue_db = PROJECT_ROOT.parent / "data" / "pipeline_queue.sqlite3"
    if not queue_db.exists():
        return {"requested": 0, "queued": 0, "already_known": 0}
    domains = screenable_domains(
        database,
        limit=5000,
        minimum_words=1,
        exclude_queue_db=queue_db,
        include_stale=True,
    )
    return enqueue_for_screening(
        queue_db,
        domains,
        source_run_id="ai_search_cache_reuse_v1",
        requeue_existing=True,
    )


async def crawl_loop(database, settings, stop: asyncio.Event) -> None:
    while not stop.is_set():
        if RUNTIME_MODE != "local" and not read_control()["desired"].get(
            "ai_crawler", True
        ):
            conn = connect(database)
            set_worker_state(conn, "crawler_status", "paused")
            conn.close()
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
            continue
        conn = connect(database)
        try:
            set_worker_state(conn, "crawler_status", "running")
            counts = await crawl_pending(
                conn, settings, settings.crawl_batch_size
            )
            if counts:
                set_worker_state(conn, "last_crawl_batch", counts)
                set_worker_state(conn, "last_crawl_error", "")
                sync_result = await asyncio.to_thread(
                    sync_crawled_domains_to_original_queue,
                    database,
                )
                set_worker_state(
                    conn,
                    "original_queue_sync",
                    (
                        f"queued {sync_result['queued']}; "
                        f"already known {sync_result['already_known']}"
                    ),
                )
        except Exception as exc:
            logging.exception("Crawler batch failed")
            set_worker_state(
                conn, "crawler_status", f"error: {type(exc).__name__}"
            )
            set_worker_state(conn, "last_crawl_error", str(exc)[:300])
        finally:
            conn.close()
        try:
            await asyncio.wait_for(
                stop.wait(), timeout=settings.crawl_poll_seconds
            )
        except asyncio.TimeoutError:
            pass


async def query_discovery_loop(database, settings, stop: asyncio.Event) -> None:
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": settings.user_agent}
    async with aiohttp.ClientSession(
        timeout=timeout, headers=headers
    ) as session:
        while not stop.is_set():
            conn = connect(database)
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(hours=settings.rediscover_hours)
            ).isoformat()
            watches = conn.execute("""
                SELECT id, query FROM watch_queries
                WHERE active=1 AND (
                    last_discovered_at IS NULL OR last_discovered_at < ?
                )
                ORDER BY created_at
            """, (cutoff,)).fetchall()
            for watch in watches:
                try:
                    provider, results = await discover_query(
                        session, watch["query"], settings
                    )
                    for rank, result in enumerate(results, 1):
                        domain = normalize_domain(result.url)
                        add_domain(conn, domain, f"web_search:{provider}")
                        domain_id = conn.execute(
                            "SELECT id FROM domains WHERE domain=?", (domain,)
                        ).fetchone()["id"]
                        conn.execute("""
                            INSERT INTO query_discoveries(
                                watch_id, domain_id, provider, result_url,
                                result_rank, discovered_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(watch_id, domain_id, provider)
                            DO UPDATE SET result_url=excluded.result_url,
                                result_rank=excluded.result_rank,
                                discovered_at=excluded.discovered_at
                        """, (
                            watch["id"], domain_id, provider, result.url,
                            rank, utc_now(),
                        ))
                    conn.execute("""
                        UPDATE watch_queries SET last_discovered_at=?,
                            last_discovery_status=? WHERE id=?
                    """, (
                        utc_now(), f"{provider}: {len(results)} domains",
                        watch["id"],
                    ))
                    conn.commit()
                    set_worker_state(
                        conn, "web_discovery_status",
                        f"{provider}: found {len(results)} for {watch['query']}"
                    )
                except Exception as exc:
                    logging.exception("Web discovery failed")
                    conn.execute("""
                        UPDATE watch_queries SET last_discovery_status=?
                        WHERE id=?
                    """, (f"error: {type(exc).__name__}", watch["id"]))
                    conn.commit()
                    set_worker_state(
                        conn, "web_discovery_status",
                        f"error: {type(exc).__name__}: {str(exc)[:120]}"
                    )
            conn.close()
            try:
                await asyncio.wait_for(stop.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass


async def run() -> None:
    claim_worker_pid()
    settings = load_settings()
    settings.database.parent.mkdir(parents=True, exist_ok=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, stop.set)

    try:
        conn = connect(settings.database)
        set_worker_state(conn, "worker_status", "running")
        set_worker_state(conn, "last_crawl_error", "")
        conn.close()
        tasks = [
            asyncio.create_task(crawl_loop(settings.database, settings, stop)),
        ]
        if settings.web_provider != "off":
            tasks.append(asyncio.create_task(query_discovery_loop(
                settings.database, settings, stop
            )))
        if settings.discovery_stream:
            tasks.append(asyncio.create_task(stream_domains(
                settings.database, settings.discovery_stream, stop
            )))
        await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        conn = connect(settings.database)
        set_worker_state(conn, "worker_status", "stopped")
        set_worker_state(conn, "crawler_status", "stopped")
        set_worker_state(conn, "discovery_status", "stopped")
        set_worker_state(conn, "web_discovery_status", "stopped")
        conn.close()
    finally:
        release_worker_pid()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
