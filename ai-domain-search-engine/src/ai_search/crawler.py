from __future__ import annotations

import asyncio
import ssl
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from db_backend import using_postgres

from .config import Settings
from .database import AI_SEARCH_DATABASE_ENV_VAR, connect, utc_now
from .extract import (
    ExtractedPage,
    extract_page,
    homepage_enrichment_plan,
    same_site_url,
)

SCORING_TEXT_LIMIT = 15_000


def classify_error(exc: Exception) -> str:
    if isinstance(exc, asyncio.TimeoutError):
        return "timeout"
    if isinstance(exc, aiohttp.ClientConnectorCertificateError):
        return "ssl_error"
    dns_error_type = getattr(aiohttp, "ClientConnectorDNSError", ())
    if dns_error_type and isinstance(exc, dns_error_type):
        return "dns_error"
    if isinstance(exc, aiohttp.ClientConnectionError):
        return "connection_error"
    return "unknown_error"


def retry_delay_days(status: str, failures: int) -> int:
    """Back off permanent-looking failures more than transient failures."""
    status = str(status).lower()
    base = 3
    if status == "dns_error":
        base = 30
    elif status in {"http_error", "non_html", "response_too_large"}:
        base = 14
    elif status == "blocked":
        base = 7
    elif status in {"timeout", "connection_error", "ssl_error"}:
        base = 2
    multiplier = 2 ** min(max(int(failures) - 1, 0), 3)
    return min(base * multiplier, 180)


async def _fetch(
    session: aiohttp.ClientSession, url: str, settings: Settings
) -> tuple[str, str, int, str, int]:
    started = time.monotonic()
    async with session.get(url, allow_redirects=True, max_redirects=5) as response:
        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type.lower():
            raise ValueError(f"non_html:{content_type}")
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.content.iter_chunked(65_536):
            size += len(chunk)
            if size > settings.max_response_bytes:
                raise ValueError("response_too_large")
            chunks.append(chunk)
        html = b"".join(chunks).decode(
            response.charset or "utf-8", errors="replace"
        )
        return (
            html, str(response.url), response.status, content_type,
            int((time.monotonic() - started) * 1000),
        )


def _save_attempt(
    conn: Any, domain_id: int, requested: str, status: str, duration: int,
    *, final_url: str = "", http_status: int | None = None,
    error_type: str = "", error_message: str = "",
) -> None:
    conn.execute("""
        INSERT INTO crawl_attempts(
            domain_id, requested_url, final_url, status, error_type,
            error_message, http_status, duration_ms, attempted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        domain_id, requested, final_url, status, error_type,
        error_message[:500], http_status, duration, utc_now(),
    ))


def _save_page(
    conn: Any, domain_id: int, requested: str, final_url: str,
    status: int, content_type: str, html: str, page: ExtractedPage,
    settings: Settings, domain: str,
) -> int:
    # HTML exists only for extraction during this crawl. Never archive it.
    # Keep one bounded text window for the asynchronous scoring handoff; the
    # unused full-text search feature must not duplicate entire websites.
    scoring_text = page.cleaned_text[:SCORING_TEXT_LIMIT]
    conn.execute("""
        INSERT INTO pages(
            domain_id, url, final_url, title, meta_description, headings,
            visible_text, cleaned_text, raw_html, canonical_url, language,
            emails, social_links, json_ld, http_status, content_type,
            fetch_method, word_count, content_hash, crawled_at,
            raw_html_hash,html_archive_path,html_archive_offset,
            html_archive_length,html_archive_codec,raw_html_bytes,
            compressed_html_bytes
        ) VALUES (?, ?, ?, ?, ?, ?, '', ?, '', ?, ?, ?, ?, ?, ?, ?,
                  'compact_archive', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(domain_id, final_url) DO UPDATE SET
            url=excluded.url, title=excluded.title,
            meta_description=excluded.meta_description,
            headings=excluded.headings, visible_text='',
            cleaned_text=excluded.cleaned_text, raw_html=excluded.raw_html,
            canonical_url=excluded.canonical_url, language=excluded.language,
            emails=excluded.emails, social_links=excluded.social_links,
            json_ld=excluded.json_ld, http_status=excluded.http_status,
            content_type=excluded.content_type, word_count=excluded.word_count,
            content_hash=excluded.content_hash, crawled_at=excluded.crawled_at,
            fetch_method=excluded.fetch_method,
            raw_html_hash=excluded.raw_html_hash,
            html_archive_path=excluded.html_archive_path,
            html_archive_offset=excluded.html_archive_offset,
            html_archive_length=excluded.html_archive_length,
            html_archive_codec=excluded.html_archive_codec,
            raw_html_bytes=excluded.raw_html_bytes,
            compressed_html_bytes=excluded.compressed_html_bytes
    """, (
        domain_id, requested, final_url, page.title, page.description,
        page.headings, scoring_text, page.canonical_url,
        page.language, page.emails, page.social_links, page.json_ld, status,
        content_type, page.word_count, page.content_hash, utc_now(),
        "", "", 0, 0, "", 0, 0,
    ))
    return conn.execute(
        "SELECT id FROM pages WHERE domain_id=? AND final_url=?",
        (domain_id, final_url),
    ).fetchone()["id"]


async def crawl_domain(
    conn: Any, session: aiohttp.ClientSession, row: Any, settings: Settings
) -> str:
    domain_id, domain = row["id"], row["domain"]
    candidates = [
        f"https://{domain}", f"https://www.{domain}",
        f"http://{domain}", f"http://www.{domain}",
    ]
    homepage: tuple[str, str, int, str, str, ExtractedPage] | None = None
    last_status = "unknown_error"
    dns_failed_hosts: set[str] = set()
    for requested in candidates:
        requested_host = urlsplit(requested).hostname or ""
        if requested_host in dns_failed_hosts:
            continue
        started = time.monotonic()
        try:
            html, final_url, http_status, content_type, duration = await _fetch(
                session, requested, settings
            )
            extracted = extract_page(html, final_url)
            status = "success" if extracted.cleaned_text else "empty"
            _save_attempt(
                conn, domain_id, requested, status, duration,
                final_url=final_url, http_status=http_status,
            )
            if http_status >= 400:
                last_status = "blocked" if http_status in {401, 403, 429} else "http_error"
                continue
            homepage = (
                requested, final_url, http_status, content_type, html, extracted
            )
            break
        except Exception as exc:
            duration = int((time.monotonic() - started) * 1000)
            last_status = (
                str(exc).split(":", 1)[0]
                if isinstance(exc, ValueError) else classify_error(exc)
            )
            if last_status == "dns_error":
                dns_failed_hosts.add(requested_host)
            _save_attempt(
                conn, domain_id, requested, last_status, duration,
                error_type=type(exc).__name__, error_message=str(exc),
            )

    now = datetime.now(timezone.utc)
    if not homepage:
        failures = int(row["crawl_failures"] or 0) + 1
        retry_days = retry_delay_days(last_status, failures)
        conn.execute("""
            UPDATE domains SET latest_crawl_status=?, last_crawled_at=?,
                next_crawl_at=?, crawl_failures=?,
                last_document_count=0,
                last_enrichment_reason='homepage_unavailable'
            WHERE id=?
        """, (
            last_status, utc_now(),
            (now + timedelta(days=retry_days)).isoformat(),
            failures,
            domain_id,
        ))
        conn.commit()
        return last_status

    requested, final_url, code, content_type, html, extracted = homepage
    _save_page(
        conn,
        domain_id,
        requested,
        final_url,
        code,
        content_type,
        html,
        extracted,
        settings,
        domain,
    )
    seen = {final_url}
    extra_page_budget, enrichment_reason = homepage_enrichment_plan(
        extracted,
        max(0, settings.max_pages_per_domain - 1),
    )
    extra_pages_saved = 0
    link_attempts = 0
    link_attempt_limit = min(
        len(extracted.internal_links),
        max(extra_page_budget, extra_page_budget * 3),
    )
    for link in extracted.internal_links:
        if (
            extra_pages_saved >= extra_page_budget
            or link_attempts >= link_attempt_limit
        ):
            break
        if link in seen or not same_site_url(link, final_url):
            continue
        seen.add(link)
        link_attempts += 1
        try:
            inner_html, inner_final, inner_code, inner_type, duration = await _fetch(
                session, link, settings
            )
            if not same_site_url(inner_final, final_url):
                _save_attempt(
                    conn,
                    domain_id,
                    link,
                    "redirected_external",
                    duration,
                    final_url=inner_final,
                    http_status=inner_code,
                )
                continue
            if inner_code >= 400:
                status = (
                    "blocked"
                    if inner_code in {401, 403, 429}
                    else "http_error"
                )
                _save_attempt(
                    conn,
                    domain_id,
                    link,
                    status,
                    duration,
                    final_url=inner_final,
                    http_status=inner_code,
                )
                continue
            inner = extract_page(inner_html, inner_final)
            _save_attempt(
                conn,
                domain_id,
                link,
                "success" if inner.cleaned_text else "empty",
                duration,
                final_url=inner_final, http_status=inner_code,
            )
            if not inner.cleaned_text:
                continue
            if inner_final in seen and inner_final != link:
                continue
            seen.add(inner_final)
            _save_page(
                conn, domain_id, link, inner_final, inner_code, inner_type,
                inner_html, inner, settings, domain,
            )
            extra_pages_saved += 1
        except Exception as exc:
            _save_attempt(
                conn, domain_id, link, classify_error(exc), 0,
                error_type=type(exc).__name__, error_message=str(exc),
            )

    conn.execute("""
        UPDATE domains SET latest_crawl_status='success', last_crawled_at=?,
            next_crawl_at=?, is_parked=?, is_coming_soon=?,
            crawl_failures=0, last_success_at=?,
            last_document_count=?,last_enrichment_reason=?
        WHERE id=?
    """, (
        utc_now(), (now + timedelta(days=settings.recrawl_days)).isoformat(),
        int(extracted.is_parked), int(extracted.is_coming_soon),
        utc_now(), 1 + extra_pages_saved, enrichment_reason, domain_id,
    ))
    conn.commit()
    return "success"


async def crawl_pending(conn: Any, settings: Settings, limit: int) -> dict[str, int]:
    rows = conn.execute("""
        SELECT id, domain, crawl_failures FROM domains
        WHERE active=1 AND (
            last_crawled_at IS NULL OR next_crawl_at <= ?
        ) ORDER BY last_crawled_at IS NOT NULL, first_seen_at LIMIT ?
    """, (utc_now(), limit)).fetchall()
    # A later corpus import can rediscover a domain whose expensive payload was
    # intentionally purged. Consult the tiny canonical tombstone before making
    # another network request, then remove that reimported inventory row.
    from classification_store import excluded_domain_set

    excluded = await asyncio.to_thread(
        excluded_domain_set,
        [str(row["domain"]) for row in rows],
    )
    if excluded:
        for start in range(0, len(excluded), 700):
            batch = sorted(excluded)[start:start + 700]
            marks = ",".join("?" for _ in batch)
            conn.execute(
                f"DELETE FROM domains WHERE domain IN ({marks})",
                batch,
            )
        conn.commit()
        rows = [row for row in rows if str(row["domain"]) not in excluded]
    if not rows:
        return {"storage_excluded": len(excluded)} if excluded else {}
    timeout = aiohttp.ClientTimeout(total=settings.timeout_seconds)
    connector = aiohttp.TCPConnector(
        limit=settings.concurrency,
        ssl=ssl.create_default_context(),
        resolver=aiohttp.AsyncResolver(),
    )
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.8",
    }
    counts: dict[str, int] = {"storage_excluded": len(excluded)} if excluded else {}
    semaphore = asyncio.Semaphore(settings.concurrency)
    async with aiohttp.ClientSession(
        timeout=timeout, connector=connector, headers=headers
    ) as session:
        async def guarded(row: Any) -> str:
            async with semaphore:
                # psycopg connections are transaction state machines and may
                # not be shared by concurrent crawl coroutines. Doing so made
                # one domain's commit/deadlock abort unrelated domains and
                # could permanently stop the worker heartbeat while Railway
                # still reported the container as online. Give each concurrent
                # Postgres crawl its own pooled connection. SQLite remains on
                # the single connection because it serializes writes itself.
                if not using_postgres(AI_SEARCH_DATABASE_ENV_VAR):
                    return await crawl_domain(conn, session, row, settings)
                # Pool acquisition is blocking. Running it on the event loop
                # lets queued coroutines freeze the active network requests,
                # exhaust the 10-second pool timeout, and stop the heartbeat.
                row_conn = await asyncio.to_thread(connect, settings.database)
                try:
                    return await crawl_domain(row_conn, session, row, settings)
                finally:
                    await asyncio.to_thread(row_conn.close)
        for status in await asyncio.gather(*(guarded(row) for row in rows)):
            counts[status] = counts.get(status, 0) + 1
    return counts
