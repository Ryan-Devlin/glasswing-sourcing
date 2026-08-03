from __future__ import annotations

import asyncio
import hashlib
import socket
import shutil
import sqlite3
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
from ai_search.extract import (
    ExtractedPage,
    extract_page,
    homepage_enrichment_plan,
    same_site_url,
)
from db_backend import PostgresConnection

from .config import DATA_ROOT, Settings
from .database import log_event, set_state, utc_now
from .domain_quality import crawl_priority
from .html_archive import append_html


@dataclass
class CrawlOutcome:
    domain: str
    tld: str
    status: str
    requested_url: str = ""
    final_url: str = ""
    http_status: int | None = None
    html: str = ""
    page: ExtractedPage | None = None
    duration_ms: int = 0
    error: str = ""
    document_count: int = 0
    enrichment_reason: str = ""
    from_retry: bool = False


def free_disk_gb(path: Path) -> float:
    return shutil.disk_usage(path).free / (1024 ** 3)


def claim_batch(
    conn: sqlite3.Connection,
    tld: str,
    batch_size: int,
) -> tuple[sqlite3.Row | None, list[str], int]:
    row = conn.execute(
        """
        SELECT s.*
        FROM shards s
        JOIN zones z
          ON z.tld=s.tld
         AND z.active_queue_version=s.queue_version
        WHERE s.tld=? AND s.byte_offset < s.total_bytes
        ORDER BY
          s.priority,
          CAST(s.byte_offset AS REAL) / max(s.total_bytes, 1),
          s.shard
        LIMIT 1
        """,
        (tld,),
    ).fetchone()
    if row is None:
        return None, [], 0
    path = Path(row["path"])
    if not path.exists():
        raise FileNotFoundError(path)
    domains: list[str] = []
    with path.open("rb") as handle:
        handle.seek(int(row["byte_offset"]))
        while len(domains) < batch_size:
            raw = handle.readline()
            if not raw:
                break
            domain = raw.decode("ascii", errors="ignore").strip().lower()
            if domain:
                domains.append(domain)
        next_offset = handle.tell()
    return row, domains, next_offset


def exclude_already_successful(
    conn: sqlite3.Connection,
    domains: list[str],
) -> tuple[list[str], int]:
    if not domains:
        return [], 0
    existing: set[str] = set()
    for start in range(0, len(domains), 800):
        batch = domains[start : start + 800]
        marks = ",".join("?" for _ in batch)
        existing.update(
            str(row["domain"])
            for row in conn.execute(
                f"SELECT domain FROM pages WHERE domain IN ({marks})",
                batch,
            )
        )
    return [
        domain for domain in domains if domain not in existing
    ], len(existing)


def quarantine_low_priority_domains(
    domains: list[str],
    *,
    maximum_priority: int,
    bypass: bool = False,
) -> tuple[list[str], list[str]]:
    """Keep the active crawl lean while preserving cold names in zone queues."""
    if bypass:
        return list(domains), []
    kept: list[str] = []
    quarantined: list[str] = []
    for domain in domains:
        if crawl_priority(domain) <= int(maximum_priority):
            kept.append(domain)
        else:
            quarantined.append(domain)
    return kept, quarantined


async def _read_html(
    response: aiohttp.ClientResponse,
    maximum_bytes: int,
) -> str:
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.content.iter_chunked(65_536):
        size += len(chunk)
        if size > maximum_bytes:
            raise ValueError("response_too_large")
        chunks.append(chunk)
    return b"".join(chunks).decode(
        response.charset or "utf-8",
        errors="replace",
    )


def classify_exception(exc: Exception) -> str:
    if isinstance(exc, asyncio.TimeoutError):
        return "timeout"
    dns_error_type = getattr(aiohttp, "ClientConnectorDNSError", ())
    if dns_error_type and isinstance(exc, dns_error_type):
        return "dns_error"
    if isinstance(exc, aiohttp.ClientConnectorCertificateError):
        return "other_error"
    if isinstance(exc, aiohttp.ClientConnectionError):
        return "other_error"
    if isinstance(exc, ValueError):
        return str(exc).split(":", 1)[0]
    return "other_error"


async def domain_resolves(domain: str, timeout_seconds: float = 3.0) -> bool:
    """Return quickly when neither apex nor www has a web-address record."""
    loop = asyncio.get_running_loop()

    async def resolve(host: str) -> bool:
        try:
            await asyncio.wait_for(
                loop.getaddrinfo(
                    host,
                    443,
                    type=socket.SOCK_STREAM,
                ),
                timeout=timeout_seconds,
            )
            return True
        except (OSError, asyncio.TimeoutError):
            return False

    apex, www = await asyncio.gather(
        resolve(domain),
        resolve(f"www.{domain}"),
    )
    return apex or www


def merge_extracted_pages(
    pages: list[ExtractedPage],
    *,
    canonical_url: str,
) -> ExtractedPage:
    primary = pages[0]
    headings = " ".join(
        dict.fromkeys(page.headings for page in pages if page.headings)
    )
    body = " ".join(page.body for page in pages if page.body)
    cleaned = " ".join(page.cleaned_text for page in pages if page.cleaned_text)
    links = tuple(
        dict.fromkeys(
            link
            for page in pages
            for link in page.internal_links
        )
    )
    return ExtractedPage(
        title=primary.title,
        description=primary.description,
        headings=headings,
        body=body,
        cleaned_text=cleaned,
        canonical_url=primary.canonical_url or canonical_url,
        language=primary.language,
        emails=primary.emails,
        social_links=primary.social_links,
        json_ld=primary.json_ld,
        internal_links=links,
        word_count=sum(page.word_count for page in pages),
        content_hash=hashlib.sha256(cleaned.encode("utf-8")).hexdigest(),
        is_parked=any(page.is_parked for page in pages),
        is_coming_soon=all(page.is_coming_soon for page in pages),
    )


async def _fetch_html_page(
    session: aiohttp.ClientSession,
    requested: str,
    settings: Settings,
) -> tuple[aiohttp.ClientResponse, str, ExtractedPage]:
    response = await session.get(
        requested,
        allow_redirects=True,
        max_redirects=5,
    )
    try:
        content_type = response.headers.get("Content-Type", "")
        if response.status in {401, 403, 429}:
            raise PermissionError(f"http_{response.status}")
        if response.status >= 400:
            raise ValueError(f"http_{response.status}")
        if "html" not in content_type.lower():
            raise ValueError(f"non_html:{content_type}"[:300])
        html = await _read_html(response, settings.max_response_bytes)
        page = extract_page(html, str(response.url))
        return response, html, page
    except Exception:
        response.release()
        raise


async def crawl_domain(
    session: aiohttp.ClientSession,
    domain: str,
    settings: Settings,
) -> CrawlOutcome:
    tld = domain.rsplit(".", 1)[-1]
    started = time.monotonic()
    if settings.dns_preflight and not await domain_resolves(domain):
        return CrawlOutcome(
            domain=domain,
            tld=tld,
            status="dns_error",
            duration_ms=int((time.monotonic() - started) * 1000),
            error="dns_preflight_no_apex_or_www_address",
        )
    candidates = (
        f"https://{domain}",
        f"https://www.{domain}",
        f"http://{domain}",
    )
    final_status = "other_error"
    final_error = ""
    final_http: int | None = None
    for requested in candidates:
        try:
            response, html, page = await _fetch_html_page(
                session,
                requested,
                settings,
            )
            try:
                final_http = response.status
                documents = [(str(response.url), html)]
                pages = [page]
                (
                    extra_page_budget,
                    enrichment_reason,
                ) = homepage_enrichment_plan(
                    page,
                    max(0, settings.max_pages_per_domain - 1),
                )
                extra_pages_saved = 0
                link_attempts = 0
                link_attempt_limit = min(
                    len(page.internal_links),
                    max(extra_page_budget, extra_page_budget * 3),
                )
                seen = {str(response.url)}
                for link in page.internal_links:
                    if (
                        extra_pages_saved >= extra_page_budget
                        or link_attempts >= link_attempt_limit
                    ):
                        break
                    if link in seen or not same_site_url(
                        link,
                        str(response.url),
                    ):
                        continue
                    seen.add(link)
                    link_attempts += 1
                    subresponse = None
                    try:
                        (
                            subresponse,
                            subhtml,
                            subpage,
                        ) = await _fetch_html_page(session, link, settings)
                        subfinal = str(subresponse.url)
                        if (
                            not same_site_url(
                                subfinal,
                                str(response.url),
                            )
                            or not subpage.cleaned_text
                            or (
                                subfinal in seen
                                and subfinal != link
                            )
                        ):
                            continue
                        seen.add(subfinal)
                        documents.append((str(subresponse.url), subhtml))
                        pages.append(subpage)
                        extra_pages_saved += 1
                    except Exception:
                        continue
                    finally:
                        if subresponse is not None:
                            subresponse.release()
                merged = merge_extracted_pages(
                    pages,
                    canonical_url=str(response.url),
                )
                bundled_html = "\n".join(
                    f"<!-- SOURCE URL: {url} -->\n{document_html}"
                    for url, document_html in documents
                )
                status = "success" if merged.cleaned_text else "empty"
                return CrawlOutcome(
                    domain=domain,
                    tld=tld,
                    status=status,
                    requested_url=requested,
                    final_url=str(response.url),
                    http_status=response.status,
                    html=bundled_html,
                    page=merged,
                    duration_ms=int(
                        (time.monotonic() - started) * 1000
                    ),
                    document_count=len(documents),
                    enrichment_reason=enrichment_reason,
                )
            finally:
                response.release()
        except PermissionError as exc:
            final_error = str(exc)
            final_http = int(final_error.rsplit("_", 1)[-1])
            final_status = "blocked"
            continue
        except ValueError as exc:
            final_error = str(exc)[:300]
            if final_error.startswith("http_"):
                final_status = "http_error"
                try:
                    final_http = int(final_error.rsplit("_", 1)[-1])
                except ValueError:
                    pass
            else:
                final_status = "other_error"
            continue
        except Exception as exc:
            final_status = classify_exception(exc)
            final_error = f"{type(exc).__name__}: {exc}"[:300]
    return CrawlOutcome(
        domain=domain,
        tld=tld,
        status=final_status,
        http_status=final_http,
        duration_ms=int((time.monotonic() - started) * 1000),
        error=final_error,
    )


def save_success(
    conn: sqlite3.Connection,
    outcome: CrawlOutcome,
    *,
    retain_raw_html: bool,
    archive_root: Path | None = None,
) -> None:
    if outcome.page is None:
        return
    raw = outcome.html.encode("utf-8", errors="replace")
    raw_hash = hashlib.sha256(raw).hexdigest()
    compressed = None
    archive = None
    if retain_raw_html:
        if archive_root is not None:
            existing_blob = conn.execute(
                """
                SELECT archive_path,archive_offset,archive_length,
                       archive_codec,raw_bytes
                FROM html_blobs WHERE raw_html_hash=?
                """,
                (raw_hash,),
            ).fetchone()
            if existing_blob is not None:
                from .html_archive import ArchivePointer

                archive = ArchivePointer(
                    path=str(existing_blob["archive_path"]),
                    offset=int(existing_blob["archive_offset"]),
                    length=int(existing_blob["archive_length"]),
                    codec=str(existing_blob["archive_codec"]),
                    raw_bytes=int(existing_blob["raw_bytes"]),
                )
            else:
                archive = append_html(
                    archive_root,
                    domain=outcome.domain,
                    tld=outcome.tld,
                    html=outcome.html,
                )
                conn.execute(
                    """
                    INSERT INTO html_blobs(
                        raw_html_hash,archive_path,archive_offset,
                        archive_length,archive_codec,raw_bytes,created_at
                    ) VALUES (?,?,?,?,?,?,?)
                    ON CONFLICT (raw_html_hash) DO NOTHING
                    """,
                    (
                        raw_hash,
                        archive.path,
                        archive.offset,
                        archive.length,
                        archive.codec,
                        archive.raw_bytes,
                        utc_now(),
                    ),
                )
        else:
            compressed = zlib.compress(raw, level=6)
    now = utc_now()
    conn.execute(
        """
        INSERT INTO pages(
            domain,tld,requested_url,final_url,title,meta_description,
            headings,cleaned_text,raw_html_zlib,raw_html_hash,raw_html_bytes,
            html_archive_path,html_archive_offset,html_archive_length,
            html_archive_codec,document_count,compressed_html_bytes,
            http_status,word_count,content_hash,
            language,is_parked,is_coming_soon,crawl_duration_ms,
            enrichment_reason,crawled_at,updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(domain) DO UPDATE SET
            tld=excluded.tld,
            requested_url=excluded.requested_url,
            final_url=excluded.final_url,
            title=excluded.title,
            meta_description=excluded.meta_description,
            headings=excluded.headings,
            cleaned_text=excluded.cleaned_text,
            raw_html_zlib=excluded.raw_html_zlib,
            raw_html_hash=excluded.raw_html_hash,
            html_archive_path=excluded.html_archive_path,
            html_archive_offset=excluded.html_archive_offset,
            html_archive_length=excluded.html_archive_length,
            html_archive_codec=excluded.html_archive_codec,
            document_count=excluded.document_count,
            raw_html_bytes=excluded.raw_html_bytes,
            compressed_html_bytes=excluded.compressed_html_bytes,
            http_status=excluded.http_status,
            word_count=excluded.word_count,
            content_hash=excluded.content_hash,
            language=excluded.language,
            is_parked=excluded.is_parked,
            is_coming_soon=excluded.is_coming_soon,
            crawl_duration_ms=excluded.crawl_duration_ms,
            enrichment_reason=excluded.enrichment_reason,
            crawled_at=excluded.crawled_at,
            updated_at=excluded.updated_at
        """,
        (
            outcome.domain,
            outcome.tld,
            outcome.requested_url,
            outcome.final_url,
            outcome.page.title,
            outcome.page.description,
            outcome.page.headings,
            outcome.page.cleaned_text,
            compressed,
            raw_hash,
            len(raw),
            archive.path if archive else "",
            archive.offset if archive else 0,
            archive.length if archive else 0,
            archive.codec if archive else "",
            max(1, outcome.document_count),
            archive.length if archive else len(compressed or b""),
            outcome.http_status,
            outcome.page.word_count,
            outcome.page.content_hash,
            outcome.page.language,
            int(outcome.page.is_parked),
            int(outcome.page.is_coming_soon),
            outcome.duration_ms,
            outcome.enrichment_reason,
            now,
            now,
        ),
    )
    page_id = conn.execute(
        "SELECT id FROM pages WHERE domain=?",
        (outcome.domain,),
    ).fetchone()["id"]
    if isinstance(conn, PostgresConnection):
        # Postgres has no FTS5-style contentless index table; the ranked
        # text lives directly on the row instead, as a weighted tsvector.
        # Weights mirror ai_search's translation: domain (A) > title (B) >
        # description (C) > headings/body (D).
        conn.execute("""
            UPDATE pages SET search_vector =
                setweight(to_tsvector('english', coalesce(?, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'C') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'D') ||
                setweight(to_tsvector('english', coalesce(?, '')), 'D')
            WHERE id = ?
        """, (
            outcome.domain, outcome.page.title, outcome.page.description,
            outcome.page.headings, outcome.page.cleaned_text, page_id,
        ))
    else:
        conn.execute(
            "DELETE FROM page_search WHERE page_id=?",
            (page_id,),
        )
        conn.execute(
            """
            INSERT INTO page_search(
                page_id,domain,title,description,headings,body
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                page_id,
                outcome.domain,
                outcome.page.title,
                outcome.page.description,
                outcome.page.headings,
                outcome.page.cleaned_text,
            ),
        )


def append_failures(outcomes: list[CrawlOutcome]) -> None:
    failures = [
        outcome
        for outcome in outcomes
        if outcome.status not in {"success", "empty"}
    ]
    if not failures:
        return
    failure_root = DATA_ROOT / "failures"
    failure_root.mkdir(parents=True, exist_ok=True)
    by_tld: dict[str, list[CrawlOutcome]] = {}
    for outcome in failures:
        by_tld.setdefault(outcome.tld, []).append(outcome)
    for tld, rows in by_tld.items():
        with (failure_root / f"{tld}.tsv").open(
            "a",
            encoding="utf-8",
        ) as handle:
            for row in rows:
                handle.write(
                    f"{row.domain}\t{row.status}\t"
                    f"{row.http_status or ''}\t{row.error}\n"
                )


def claim_due_retries(
    conn: sqlite3.Connection,
    *,
    tld: str,
    limit: int,
) -> list[str]:
    return [
        str(row["domain"])
        for row in conn.execute(
            """
            SELECT domain
            FROM crawl_retries
            WHERE tld=? AND next_attempt_at<=?
            ORDER BY next_attempt_at,domain
            LIMIT ?
            """,
            (tld, utc_now(), max(0, int(limit))),
        ).fetchall()
    ]


def retryable(outcome: CrawlOutcome) -> bool:
    if outcome.status in {"timeout", "other_error"}:
        return True
    if outcome.status == "blocked":
        return outcome.http_status == 429
    if outcome.status == "http_error":
        return bool(outcome.http_status and outcome.http_status >= 500)
    return False


def update_retry_state(
    conn: sqlite3.Connection,
    outcome: CrawlOutcome,
    *,
    max_retries: int,
) -> None:
    if outcome.status == "success":
        conn.execute(
            "DELETE FROM crawl_retries WHERE domain=?",
            (outcome.domain,),
        )
        return
    if not retryable(outcome) or max_retries <= 0:
        conn.execute(
            "DELETE FROM crawl_retries WHERE domain=?",
            (outcome.domain,),
        )
        return
    current = conn.execute(
        "SELECT attempts FROM crawl_retries WHERE domain=?",
        (outcome.domain,),
    ).fetchone()
    attempts = int(current["attempts"]) + 1 if current else 1
    if attempts > max_retries:
        conn.execute(
            "DELETE FROM crawl_retries WHERE domain=?",
            (outcome.domain,),
        )
        return
    delay_minutes = (5, 60, 24 * 60, 3 * 24 * 60)[
        min(attempts - 1, 3)
    ]
    retry_at = (
        datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
    ).isoformat().replace("+00:00", "Z")
    conn.execute(
        """
        INSERT INTO crawl_retries(
            domain,tld,attempts,next_attempt_at,last_status,
            last_error,updated_at
        ) VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(domain) DO UPDATE SET
            attempts=excluded.attempts,
            next_attempt_at=excluded.next_attempt_at,
            last_status=excluded.last_status,
            last_error=excluded.last_error,
            updated_at=excluded.updated_at
        """,
        (
            outcome.domain,
            outcome.tld,
            attempts,
            retry_at,
            outcome.status,
            outcome.error[:500],
            utc_now(),
        ),
    )


def handoff_to_screening(
    domains: list[str],
    settings: Settings,
) -> dict[str, int]:
    if not settings.auto_screen or not domains:
        return {"requested": 0, "queued": 0, "already_known": 0}
    from ai_search.og_bridge import enqueue_for_screening

    return enqueue_for_screening(
        settings.screening_queue,
        domains,
        source_run_id="zone_universe_cache",
    )


def queue_candidate_registrations(
    classifications: dict[str, str],
    settings: Settings,
) -> int:
    if not settings.auto_registration or not classifications:
        return 0
    from .rdap import queue_registrations

    queued = 0
    registration_priorities = {
        "priority": 100,
        "coming_soon": 80,
        "review": 60,
        "comp": 50,
    }
    for bucket, priority in registration_priorities.items():
        candidates = [
            domain
            for domain, classification in classifications.items()
            if classification == bucket
        ]
        queued += queue_registrations(
            candidates,
            database=settings.database,
            priority=priority,
        )
    return queued


async def classify_cached_successes(
    domains: list[str],
    settings: Settings,
) -> dict[str, int]:
    if not settings.auto_classify or not domains:
        return {"classified": 0}
    import filter as screening_filter

    observed = utc_now()
    records = [
        {
            "observed_at": observed,
            "registrable": domain,
            "hostname": domain,
            "run_id": "zone_universe_cache",
            "run_started_at": observed,
        }
        for domain in domains
    ]
    output, result = await screening_filter.screen_records(
        records,
        workers=settings.classifier_workers,
        timeout=settings.timeout_seconds,
        homepage_only=False,
        persist=True,
    )
    if output.empty:
        return {"classified": 0, **result}
    from classification_store import classification_map

    classifications = classification_map(domains)
    registration_queued = queue_candidate_registrations(
        classifications,
        settings,
    )
    with sqlite3.connect(settings.screening_queue, timeout=30) as queue:
        queue.execute("PRAGMA journal_mode=WAL")
        queue.executemany(
            """
            UPDATE domains SET
                status='complete',
                next_attempt_at=NULL,
                last_processed_at=?,
                classification=?,
                last_error=''
            WHERE domain=?
            """,
            [
                (utc_now(), bucket, domain)
                for domain, bucket in classifications.items()
            ],
        )
        queue.commit()
    return {
        "classified": len(classifications),
        "registration_queued": registration_queued,
        **result,
    }


async def crawl_once(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    tld: str,
) -> dict[str, int]:
    if free_disk_gb(settings.database.parent) < settings.minimum_free_gb:
        set_state(
            conn,
            "crawler_status",
            "paused_low_disk",
        )
        raise RuntimeError(
            f"low_disk_space: fewer than {settings.minimum_free_gb:g} GB free"
        )
    batch_started = time.monotonic()
    retry_limit = max(1, settings.batch_size // 10)
    retry_domains = claim_due_retries(
        conn,
        tld=tld,
        limit=retry_limit,
    )
    shard, claimed_domains, next_offset = claim_batch(
        conn,
        tld,
        settings.batch_size,
    )
    new_domains, skipped_existing = exclude_already_successful(
        conn,
        claimed_domains,
    )
    new_domains, quarantined_domains = quarantine_low_priority_domains(
        new_domains,
        maximum_priority=settings.max_crawl_priority,
        # Newly delegated names are scarce and time-sensitive. They remain
        # first-pass candidates even when their brand is unconventional.
        bypass=bool(shard is not None and int(shard["priority"]) == 0),
    )
    skipped_low_priority = len(quarantined_domains)
    domains = list(dict.fromkeys(retry_domains + new_domains))
    if not domains:
        if shard is not None and claimed_domains:
            conn.execute(
                """
                UPDATE shards SET
                    byte_offset=?,
                    skipped_existing=skipped_existing+?,
                    skipped_low_priority=skipped_low_priority+?,
                    updated_at=?
                WHERE tld=? AND queue_version=? AND shard=?
                """,
                (
                    next_offset,
                    skipped_existing,
                    skipped_low_priority,
                    utc_now(),
                    shard["tld"],
                    shard["queue_version"],
                    shard["shard"],
                ),
            )
            conn.commit()
            return {
                "skipped_existing": skipped_existing,
                "skipped_low_priority": skipped_low_priority,
            }
        return {}
    timeout = aiohttp.ClientTimeout(
        total=settings.timeout_seconds,
        connect=settings.timeout_seconds,
        sock_connect=settings.timeout_seconds,
        sock_read=settings.timeout_seconds,
    )
    connector = aiohttp.TCPConnector(
        limit=settings.concurrency,
        ttl_dns_cache=600,
    )
    semaphore = asyncio.Semaphore(settings.concurrency)
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.8",
    }
    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:
        async def guarded(domain: str) -> CrawlOutcome:
            async with semaphore:
                return await crawl_domain(session, domain, settings)

        outcomes = await asyncio.gather(
            *(guarded(domain) for domain in domains)
        )
    retry_set = set(retry_domains)
    for outcome in outcomes:
        outcome.from_retry = outcome.domain in retry_set

    counts = {
        key: sum(outcome.status == key for outcome in outcomes)
        for key in (
            "success",
            "dns_error",
            "timeout",
            "blocked",
            "http_error",
            "empty",
            "other_error",
        )
    }
    counts["skipped_low_priority"] = skipped_low_priority
    conn.execute("BEGIN IMMEDIATE")
    for outcome in outcomes:
        if outcome.status == "success":
            save_success(
                conn,
                outcome,
                retain_raw_html=settings.retain_raw_html,
                archive_root=settings.html_archive,
            )
        update_retry_state(
            conn,
            outcome,
            max_retries=settings.max_retries,
        )
    new_outcomes = [
        outcome for outcome in outcomes if not outcome.from_retry
    ]
    if shard is not None and new_domains:
        new_counts = {
            key: sum(outcome.status == key for outcome in new_outcomes)
            for key in counts
        }
        conn.execute(
            """
            UPDATE shards SET
                byte_offset=?,
                attempted=attempted+?,
                success=success+?,
                dns_error=dns_error+?,
                timeout=timeout+?,
                blocked=blocked+?,
                http_error=http_error+?,
                empty=empty+?,
                other_error=other_error+?,
                skipped_existing=skipped_existing+?,
                skipped_low_priority=skipped_low_priority+?,
                updated_at=?
            WHERE tld=? AND queue_version=? AND shard=?
            """,
            (
                next_offset,
                len(new_outcomes),
                new_counts["success"],
                new_counts["dns_error"],
                new_counts["timeout"],
                new_counts["blocked"],
                new_counts["http_error"],
                new_counts["empty"],
                new_counts["other_error"],
                skipped_existing,
                skipped_low_priority,
                utc_now(),
                shard["tld"],
                shard["queue_version"],
                shard["shard"],
            ),
        )
    conn.commit()
    append_failures(outcomes)
    successful_domains = [
        outcome.domain
        for outcome in outcomes
        if outcome.status == "success"
    ]
    try:
        handoff = await asyncio.to_thread(
            handoff_to_screening,
            successful_domains,
            settings,
        )
        set_state(conn, "last_screening_handoff", handoff)
        classification = await classify_cached_successes(
            successful_domains,
            settings,
        )
        set_state(conn, "last_python_classification", classification)
    except Exception as exc:
        set_state(
            conn,
            "last_screening_error",
            f"{type(exc).__name__}: {exc}"[:500],
        )
    elapsed = max(time.monotonic() - batch_started, 0.001)
    set_state(conn, "last_batch_seconds", f"{elapsed:.3f}")
    set_state(
        conn,
        "last_batch_domains_per_second",
        f"{len(outcomes) / elapsed:.2f}",
    )
    set_state(conn, "last_batch_retry_count", len(retry_domains))
    set_state(conn, "last_batch", counts)
    set_state(conn, "last_batch_tld", tld)
    return counts
