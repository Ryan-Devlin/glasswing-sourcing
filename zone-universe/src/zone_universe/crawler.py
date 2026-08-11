from __future__ import annotations

import asyncio
import hashlib
import shutil
import sqlite3
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import aiodns
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
from .preflight import assess_homepage


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
    preflight_reason: str = ""
    preflight_score: int = 0
    response_bytes: int = 0


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
          CAST(s.byte_offset AS REAL) /
              CASE WHEN s.total_bytes = 0 THEN 1 ELSE s.total_bytes END,
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
) -> tuple[list[str], list[str]]:
    """Keep the active crawl lean while preserving cold names in zone queues."""
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


async def _read_html_prefix(
    response: aiohttp.ClientResponse,
    maximum_bytes: int,
) -> tuple[str, int]:
    """Read only enough of a homepage for its metadata and opening copy."""
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.content.iter_chunked(32_768):
        remaining = maximum_bytes - size
        if remaining <= 0:
            break
        selected = chunk[:remaining]
        chunks.append(selected)
        size += len(selected)
        if len(selected) < len(chunk) or size >= maximum_bytes:
            break
    return (
        b"".join(chunks).decode(
            response.charset or "utf-8",
            errors="replace",
        ),
        size,
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


_dns_resolver: aiodns.DNSResolver | None = None


def _shared_dns_resolver() -> aiodns.DNSResolver:
    global _dns_resolver
    if _dns_resolver is None:
        _dns_resolver = aiodns.DNSResolver()
    return _dns_resolver


async def domain_resolves(domain: str, timeout_seconds: float = 3.0) -> bool:
    """Return quickly when neither apex nor www has a web-address record.

    Deliberately not loop.getaddrinfo() -- confirmed live: that call runs
    on the event loop's small default executor thread pool (~32-40 threads),
    and cancelling the asyncio.wait_for around it does not stop the
    underlying blocking getaddrinfo() once it's running. Against a queue
    this size (100M+ mostly-unregistered .com names), enough slow/hanging
    lookups pile up to exhaust that pool and throttle the whole crawler far
    below its configured concurrency (confirmed live: 1.6 domains/sec
    against 100 configured concurrent quick checks). aiodns resolves
    natively on the event loop over non-blocking sockets, so a cancelled
    lookup never ties up a scarce thread.
    """
    resolver = _shared_dns_resolver()

    async def resolve(host: str) -> bool:
        try:
            await asyncio.wait_for(
                resolver.query(host, "A"), timeout=timeout_seconds
            )
            return True
        except (aiodns.error.DNSError, asyncio.TimeoutError):
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


async def _fetch_homepage_preflight(
    session: aiohttp.ClientSession,
    requested: str,
    settings: Settings,
) -> tuple[aiohttp.ClientResponse, str, ExtractedPage, int]:
    response = await session.get(
        requested,
        allow_redirects=True,
        max_redirects=5,
        headers={
            "Range": (
                "bytes=0-"
                f"{settings.preflight_max_response_bytes - 1}"
            )
        },
    )
    try:
        content_type = response.headers.get("Content-Type", "")
        if response.status in {401, 403, 429}:
            raise PermissionError(f"http_{response.status}")
        if response.status >= 400:
            raise ValueError(f"http_{response.status}")
        if "html" not in content_type.lower():
            raise ValueError(f"non_html:{content_type}"[:300])
        html, bytes_read = await _read_html_prefix(
            response,
            settings.preflight_max_response_bytes,
        )
        page = extract_page(html, str(response.url))
        return response, html, page, bytes_read
    except Exception:
        response.release()
        raise


async def preflight_domain(
    session: aiohttp.ClientSession,
    domain: str,
    settings: Settings,
) -> CrawlOutcome:
    """Run one capped homepage check without retaining HTML or subpages."""
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
    bytes_read = 0
    for requested in candidates:
        response = None
        try:
            response, _, page, response_bytes = (
                await _fetch_homepage_preflight(
                    session,
                    requested,
                    settings,
                )
            )
            final_http = response.status
            bytes_read += response_bytes
            if not same_site_url(str(response.url), f"https://{domain}"):
                return CrawlOutcome(
                    domain=domain,
                    tld=tld,
                    status="preflight_rejected",
                    requested_url=requested,
                    final_url=str(response.url),
                    http_status=response.status,
                    page=page,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    preflight_reason="redirected_to_unrelated_domain",
                    preflight_score=0,
                    response_bytes=bytes_read,
                )
            decision = assess_homepage(domain, page)
            return CrawlOutcome(
                domain=domain,
                tld=tld,
                status=(
                    "preflight_promising"
                    if decision.promising
                    else "preflight_rejected"
                ),
                requested_url=requested,
                final_url=str(response.url),
                http_status=response.status,
                page=page,
                duration_ms=int((time.monotonic() - started) * 1000),
                preflight_reason=decision.reason,
                preflight_score=decision.score,
                response_bytes=bytes_read,
            )
        except PermissionError as exc:
            final_error = str(exc)
            final_http = int(final_error.rsplit("_", 1)[-1])
            final_status = "blocked"
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
        except Exception as exc:
            final_status = classify_exception(exc)
            final_error = f"{type(exc).__name__}: {exc}"[:300]
        finally:
            if response is not None:
                response.release()
    return CrawlOutcome(
        domain=domain,
        tld=tld,
        status=final_status,
        http_status=final_http,
        duration_ms=int((time.monotonic() - started) * 1000),
        error=final_error,
        response_bytes=bytes_read,
    )


async def crawl_domain(
    session: aiohttp.ClientSession,
    domain: str,
    settings: Settings,
    *,
    skip_dns_preflight: bool = False,
    preferred_url: str = "",
) -> CrawlOutcome:
    tld = domain.rsplit(".", 1)[-1]
    started = time.monotonic()
    if (
        settings.dns_preflight
        and not skip_dns_preflight
        and not await domain_resolves(domain)
    ):
        return CrawlOutcome(
            domain=domain,
            tld=tld,
            status="dns_error",
            duration_ms=int((time.monotonic() - started) * 1000),
            error="dns_preflight_no_apex_or_www_address",
        )
    candidates = tuple(dict.fromkeys(filter(None, (
        preferred_url,
        f"https://{domain}",
        f"https://www.{domain}",
        f"http://{domain}",
    ))))
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


def discard_cached_pages(conn, domains: list[str]) -> int:
    """Remove temporary crawl content for companies that failed the gate."""
    values = list(dict.fromkeys(domain for domain in domains if domain))
    if not values:
        return 0
    removed = 0
    for start in range(0, len(values), 700):
        batch = values[start : start + 700]
        marks = ",".join("?" for _ in batch)
        if not isinstance(conn, PostgresConnection):
            page_ids = [
                int(row["id"])
                for row in conn.execute(
                    f"SELECT id FROM pages WHERE domain IN ({marks})", batch
                ).fetchall()
            ]
            if page_ids:
                id_marks = ",".join("?" for _ in page_ids)
                conn.execute(
                    f"DELETE FROM page_search WHERE page_id IN ({id_marks})",
                    page_ids,
                )
        cursor = conn.execute(
            f"DELETE FROM pages WHERE domain IN ({marks})", batch
        )
        removed += max(0, int(cursor.rowcount or 0))
        conn.execute(
            f"DELETE FROM crawl_retries WHERE domain IN ({marks})", batch
        )
        conn.execute(
            f"DELETE FROM registration_queue WHERE domain IN ({marks})", batch
        )
        conn.execute(
            f"DELETE FROM domain_registration WHERE domain IN ({marks})", batch
        )
    conn.commit()
    return removed


def append_failures(outcomes: list[CrawlOutcome]) -> None:
    failures = [
        outcome
        for outcome in outcomes
        if outcome.status not in {
            "success",
            "empty",
            "preflight_rejected",
        }
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
        return {"classified": 0, "scores": {}, **result}
    from classification_store import classification_map

    classifications = classification_map(domains)
    registration_queued = queue_candidate_registrations(
        classifications,
        settings,
    )
    from ai_search.og_bridge import complete_screening_records

    complete_screening_records(settings.screening_queue, classifications)
    # Caller (crawl_once) uses this to decide which successful crawls are
    # worth backfilling a raw HTML archive for -- see
    # html_archive_minimum_score. Real venture-quality score, not a
    # display-only recompute.
    import pandas as pd

    scores = dict(zip(
        output["domain"],
        pd.to_numeric(
            output.get("diligence_rank_score", output.get("rule_score", 0)),
            errors="coerce",
        ).fillna(0).astype(int),
    ))
    from classification_store import enqueue_new_claude_reviews
    claude_queued = await asyncio.to_thread(
        enqueue_new_claude_reviews,
        [
            domain for domain, score in scores.items()
            if score > 75 and classifications.get(domain) in {"priority", "review"}
        ],
    )
    return {
        "classified": len(classifications),
        "registration_queued": registration_queued,
        "scores": scores,
        "claude_queued": claude_queued,
        "buckets": classifications,
        **result,
    }


async def crawl_once(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    tld: str,
    should_pause: Callable[[], bool] | None = None,
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
        # Newness determines queue order, not whether a name deserves network
        # spend. The old bypass sent every newly delegated name to DNS/HTTP,
        # including obvious cold inventory, and produced enormous failure
        # volume with almost no venture yield.
    )
    skipped_low_priority = len(quarantined_domains)
    from classification_store import excluded_domain_set

    storage_excluded = await asyncio.to_thread(
        excluded_domain_set,
        list(dict.fromkeys(retry_domains + new_domains)),
    )
    if storage_excluded:
        retry_domains = [
            domain for domain in retry_domains if domain not in storage_excluded
        ]
        new_domains = [
            domain for domain in new_domains if domain not in storage_excluded
        ]
        for start in range(0, len(storage_excluded), 700):
            batch = sorted(storage_excluded)[start:start + 700]
            marks = ",".join("?" for _ in batch)
            conn.execute(
                f"DELETE FROM crawl_retries WHERE domain IN ({marks})",
                batch,
            )
        conn.commit()
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
            result = {
                "skipped_existing": skipped_existing,
                "skipped_low_priority": skipped_low_priority,
            }
            if storage_excluded:
                result["storage_excluded"] = len(storage_excluded)
            return result
        return {}
    timeout = aiohttp.ClientTimeout(
        total=settings.timeout_seconds,
        connect=settings.timeout_seconds,
        sock_connect=settings.timeout_seconds,
        sock_read=settings.timeout_seconds,
    )
    connector = aiohttp.TCPConnector(
        limit=max(settings.concurrency, settings.preflight_concurrency),
        ttl_dns_cache=600,
        resolver=aiohttp.AsyncResolver(),
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
        if settings.preflight_enabled:
            set_state(conn, "crawler_phase", "quick_homepage_preflight")
            preflight_semaphore = asyncio.Semaphore(
                settings.preflight_concurrency
            )

            async def guarded_preflight(domain: str) -> CrawlOutcome:
                async with preflight_semaphore:
                    return await preflight_domain(
                        session,
                        domain,
                        settings,
                    )

            preflight_outcomes = list(await asyncio.gather(*(
                guarded_preflight(domain) for domain in domains
            )))
            if should_pause is not None and should_pause():
                set_state(conn, "crawler_phase", "paused_after_preflight")
                return {
                    "pause_requested": 1,
                    "preflight_checked": len(preflight_outcomes),
                    "preflight_promising": sum(
                        outcome.status == "preflight_promising"
                        for outcome in preflight_outcomes
                    ),
                    "preflight_rejected": sum(
                        outcome.status == "preflight_rejected"
                        for outcome in preflight_outcomes
                    ),
                }
        else:
            preflight_outcomes = [
                CrawlOutcome(
                    domain=domain,
                    tld=tld,
                    status="preflight_promising",
                )
                for domain in domains
            ]

        promising = [
            outcome
            for outcome in preflight_outcomes
            if outcome.status == "preflight_promising"
        ]
        stopped_at_gate = [
            outcome
            for outcome in preflight_outcomes
            if outcome.status != "preflight_promising"
        ]
        # Preflight rejects are intentionally counter-only. At zone scale,
        # persisting one tombstone per bad domain simply rebuilds the zone
        # file inside Postgres and defeats the entire cheap rejection gate.
        set_state(conn, "crawler_phase", "full_crawl_promising_sites")

        async def guarded_crawl(preflight: CrawlOutcome) -> CrawlOutcome:
            async with semaphore:
                outcome = await crawl_domain(
                    session,
                    preflight.domain,
                    settings,
                    skip_dns_preflight=True,
                    preferred_url=preflight.final_url,
                )
                outcome.preflight_reason = preflight.preflight_reason
                outcome.preflight_score = preflight.preflight_score
                outcome.response_bytes += preflight.response_bytes
                return outcome

        full_outcomes = list(await asyncio.gather(*(
            guarded_crawl(preflight) for preflight in promising
        )))
        outcomes = stopped_at_gate + full_outcomes
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
    counts["preflight_checked"] = (
        len(preflight_outcomes) if settings.preflight_enabled else 0
    )
    counts["preflight_promising"] = (
        len(promising) if settings.preflight_enabled else 0
    )
    counts["preflight_rejected"] = sum(
        outcome.status == "preflight_rejected"
        for outcome in preflight_outcomes
    )
    counts["preflight_response_bytes"] = sum(
        outcome.response_bytes for outcome in preflight_outcomes
    )
    counts["skipped_low_priority"] = skipped_low_priority
    counts["storage_excluded"] = len(storage_excluded)
    if not isinstance(conn, PostgresConnection):
        conn.execute("BEGIN IMMEDIATE")
    for outcome in outcomes:
        if outcome.status == "success":
            # Always defer raw HTML at this point, regardless of
            # settings.retain_raw_html -- at ~170M .com/.app names,
            # archiving every crawl outright isn't viable. Metadata
            # (title/description/cleaned_text) is saved unconditionally
            # either way, since scoring itself depends on it; the actual
            # HTML bytes only get backfilled below for domains that clear
            # html_archive_minimum_score.
            save_success(
                conn,
                outcome,
                retain_raw_html=False,
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
    new_domain_set = set(new_domains)
    new_preflight_outcomes = [
        outcome
        for outcome in preflight_outcomes
        if outcome.domain in new_domain_set
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
                preflight_checked=preflight_checked+?,
                preflight_promising=preflight_promising+?,
                preflight_rejected=preflight_rejected+?,
                preflight_response_bytes=preflight_response_bytes+?,
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
                (
                    len(new_preflight_outcomes)
                    if settings.preflight_enabled
                    else 0
                ),
                (
                    sum(
                        outcome.status == "preflight_promising"
                        for outcome in new_preflight_outcomes
                    )
                    if settings.preflight_enabled
                    else 0
                ),
                sum(
                    outcome.status == "preflight_rejected"
                    for outcome in new_preflight_outcomes
                ),
                (
                    sum(
                        outcome.response_bytes
                        for outcome in new_preflight_outcomes
                    )
                    if settings.preflight_enabled
                    else 0
                ),
                skipped_existing,
                skipped_low_priority,
                utc_now(),
                shard["tld"],
                shard["queue_version"],
                shard["shard"],
            ),
        )
    conn.commit()
    # Per-status shard counters are durable. Do not append millions of bad
    # names to an unbounded TSV file merely to repeat those counters.
    successful_domains = [
        outcome.domain
        for outcome in outcomes
        if outcome.status == "success"
    ]
    retention_decided = False
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
        buckets = classification.get("buckets", {})
        scores = classification.get("scores", {})
        retained_domains = [
            domain
            for domain in successful_domains
            if (
                buckets.get(domain) == "priority"
                or (
                    buckets.get(domain) == "review"
                    and int(scores.get(domain, 0) or 0)
                    >= settings.html_archive_minimum_score
                )
            )
        ]
        discarded_domains = [
            domain
            for domain in successful_domains
            if domain not in set(retained_domains)
        ]
        discarded_pages = discard_cached_pages(conn, discarded_domains)
        from classification_store import discard_uninvestable_classifications

        discarded_classifications = await asyncio.to_thread(
            discard_uninvestable_classifications,
            discarded_domains,
        )
        retention_decided = True
        classification["retained_qualified"] = len(retained_domains)
        classification["retained_investable"] = sum(
            buckets.get(domain) == "priority" for domain in retained_domains
        )
        classification["retained_needs_review"] = sum(
            buckets.get(domain) == "review" for domain in retained_domains
        )
        classification["discarded_pages"] = discarded_pages
        classification["discarded_classifications"] = discarded_classifications
        set_state(conn, "last_python_classification", classification)
        if settings.retain_raw_html:
            archived = 0
            for outcome in outcomes:
                if outcome.status != "success":
                    continue
                if outcome.domain not in retained_domains:
                    continue
                if scores.get(outcome.domain, 0) < settings.html_archive_minimum_score:
                    continue
                # Re-saving with retain_raw_html=True is intentionally the
                # same save_success() path as the initial (deferred) save
                # -- ON CONFLICT(domain) DO UPDATE means this just fills in
                # the archive-pointer columns that were left empty above,
                # for this one domain that actually cleared the bar.
                save_success(
                    conn,
                    outcome,
                    retain_raw_html=True,
                    archive_root=settings.html_archive,
                )
                archived += 1
            if archived:
                conn.commit()
            set_state(conn, "last_batch_html_archived", archived)
    except Exception as exc:
        # A classification outage must not turn temporary crawl content into
        # permanent storage. The shard/retry machinery can revisit it later.
        if not retention_decided:
            discard_cached_pages(conn, successful_domains)
            try:
                from classification_store import discard_uninvestable_classifications

                await asyncio.to_thread(
                    discard_uninvestable_classifications,
                    successful_domains,
                )
            except Exception:
                pass
        set_state(
            conn,
            "last_screening_error",
            f"{type(exc).__name__}: {exc}"[:500],
        )
    elapsed = max(time.monotonic() - batch_started, 0.001)
    instantaneous_rate = len(outcomes) / elapsed
    try:
        previous_row = conn.execute(
            "SELECT value FROM worker_state WHERE key='rolling_domains_per_second'"
        ).fetchone()
        previous_rate = float(previous_row["value"]) if previous_row else instantaneous_rate
    except (TypeError, ValueError, KeyError):
        previous_rate = instantaneous_rate
    # EWMA over roughly the latest several batches. DNS failures, retries,
    # and cache-heavy batches have very different costs; exposing only the
    # latest batch made healthy throughput look erratic in the dashboard.
    rolling_rate = (0.2 * instantaneous_rate) + (0.8 * previous_rate)
    set_state(conn, "last_batch_seconds", f"{elapsed:.3f}")
    set_state(
        conn,
        "last_batch_domains_per_second",
        f"{instantaneous_rate:.2f}",
    )
    set_state(conn, "rolling_domains_per_second", f"{rolling_rate:.2f}")
    set_state(conn, "last_batch_retry_count", len(retry_domains))
    set_state(conn, "last_batch", counts)
    set_state(conn, "last_batch_tld", tld)
    set_state(conn, "crawler_phase", "batch_complete")
    return counts
