"""FastAPI web surface for the React Glasswing sourcing application."""

from __future__ import annotations

import asyncio
import ast
import csv
import io
import json
import hashlib
import hmac
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
AI_SRC = ROOT / "ai-domain-search-engine" / "src"
ZONE_SRC = ROOT / "zone-universe" / "src"
for source in (ROOT, AI_SRC, ZONE_SRC):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from ai_search.og_bridge import enqueue_for_screening, search_companies, search_stats
from ai_search.domains import normalize_any_domain
from ai_search.database import (
    connect as ai_connect,
    diagnostics as ai_diagnostics,
    worker_state as ai_worker_state,
)
from classification_store import (
    admin_audit_rows,
    claim_claude_review,
    claude_decisions_for_domains,
    claude_review_status,
    claude_review_queue_rows,
    connect_store,
    enqueue_claude_review,
    defer_claude_review,
    finish_claude_review,
    classification_bucket_counts,
    classification_record_for_domain,
    classification_source_label,
    hidden_domain_set,
    hide_domains,
    query_classifications,
    query_classifications_by_domains,
    query_source_classifications,
    query_training_labels,
    record_admin_audit,
    record_manual_move,
    recover_claude_review_queue,
    reconcile_claude_routing,
    search_classifications,
    source_bucket_counts,
    list_tracked_domains,
    set_tracking_status,
    track_domains,
    tracked_domain_set,
    top_claude_keeps,
    untrack_domains,
    unhide_domains,
)
from common import (
    AI_SEARCH_DB,
    NO_BASKET_DATA,
    QUEUE_DB,
    YES_BASKET_DATA,
    ZONE_UNIVERSE_DB,
    read_basket_data,
    remove_basket_entry,
    save_basket_entry,
    stable_domain,
    RUN_CONFIG,
    atomic_write_json,
)
from continuous_pipeline import (
    claude_allowance,
    connect_db as connect_pipeline_db,
    read_status as read_local_pipeline_status,
)
from diligence_logic import DILIGENCE_VERSION
from health_monitor import health_report
from runtime_control import (
    SERVICES,
    read_control,
    read_pipeline_status,
    read_pipeline_config,
    publish_pipeline_config,
    request_services,
    runtime_status,
    shared_pipeline_backend_available,
)
from zone_universe.search import search_pages
from zone_universe.database import (
    connect as zone_connect,
    diagnostics as zone_diagnostics,
    get_state as zone_worker_state,
)
from competitive_landscape import (
    JOB_ROOT as LANDSCAPE_ROOT,
    DEFAULT_LANDSCAPE_TEMPLATE,
    claim_job as claim_landscape_job,
    create_job as create_landscape_job,
    create_default_template,
    execute_job as execute_landscape_job,
    job as landscape_job,
    list_jobs as list_landscape_jobs,
    recover_jobs as recover_landscape_jobs,
    retry_job as retry_landscape_job,
)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    await asyncio.to_thread(reconcile_claude_routing)
    try:
        await asyncio.to_thread(recover_landscape_jobs)
    except Exception as exc:
        # Background-job recovery must never hold the public website offline
        # while another service finishes the one-time schema migration.
        print(f"Competitive landscape recovery deferred: {exc}", file=sys.stderr)
    tasks = [
        asyncio.create_task(_claude_queue_worker()),
        asyncio.create_task(_landscape_worker()),
        asyncio.create_task(_form_d_worker()),
    ]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="Glasswing Sourcing API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=800)


@app.middleware("http")
async def production_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if request.url.path.startswith("/assets/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    elif request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    else:
        # The HTML shell names a content-hashed JS bundle. It must always be
        # revalidated or an open/stale tab can keep rendering retired metrics
        # after a successful deployment.
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response

BUCKETS = {
    "all": ("priority", "review", "coming_soon", "insufficient", "ghost", "screened_out", "comp", "reject"),
    "investable": ("priority",),
    "review": ("review", "coming_soon", "insufficient", "ghost", "screened_out", "comp"),
    "denied": ("reject",),
}
CORPUS_SOURCES = {
    "ai": ("crawler_cache",),
    "zone": ("zone_universe",),
}
COUNT_CACHE_SECONDS = 15.0
_count_cache: tuple[float, dict[str, int]] | None = None
_health_cache: tuple[float, dict[str, object]] | None = None
_health_cache_lock = threading.Lock()
HEALTH_CACHE_SECONDS = 30.0
ADMIN_COOKIE = "glasswing_admin"
ADMIN_SESSION_SECONDS = 8 * 60 * 60
LOGIN_WINDOW_SECONDS = 5 * 60
LOGIN_MAX_FAILURES = 10
_login_failures: dict[str, list[float]] = {}


class LabelUpdate(BaseModel):
    domain: str
    label: Literal["yes", "no"]
    rating: int = Field(ge=0, le=100)
    reason: str = "react_review"


class WorkerUpdate(BaseModel):
    desired: bool


class ClaudeReviewRequest(BaseModel):
    override_daily_limit: bool = False


class HiddenUpdate(BaseModel):
    domains: list[str]
    hidden: bool = True


class TrackingUpdate(BaseModel):
    domain: str


class TrackingStatusUpdate(BaseModel):
    status: Literal["tracking", "reached_out", "meeting"]


class LoginRequest(BaseModel):
    password: str = Field(min_length=1, max_length=200)


class PipelineFilters(BaseModel):
    selected_extensions: list[str]
    include_keywords: list[str] = []
    exclude_keywords: list[str] = []
    minimum_score: int = Field(55, ge=0, le=100)
    claude_daily_limit: int = Field(400, ge=1, le=5000)


class DomainIntakeRequest(BaseModel):
    domain: str = Field(min_length=1, max_length=2048)


def _admin_password() -> str:
    return os.getenv("GLASSWING_ADMIN_PASSWORD", "Glasswing")


def _session_token(expires: int) -> str:
    signature = hmac.new(
        _admin_password().encode(), str(expires).encode(), hashlib.sha256
    ).hexdigest()
    return f"{expires}.{signature}"


def _is_admin(request: Request) -> bool:
    token = request.cookies.get(ADMIN_COOKIE, "")
    try:
        expires_text, signature = token.split(".", 1)
        expires = int(expires_text)
    except (TypeError, ValueError):
        return False
    return expires >= int(time.time()) and hmac.compare_digest(
        signature, _session_token(expires).split(".", 1)[1]
    )


def _require_admin(request: Request) -> None:
    if not _is_admin(request):
        raise HTTPException(401, "Admin login required")


def _basket_payload(path: Path) -> list[dict]:
    frame = read_basket_data(path)
    return [
        {"domain": stable_domain(row["domain_url"]), "url": row["domain_url"], "rating": int(row["rating"])}
        for row in frame.to_dict(orient="records")
    ]


def _counts() -> dict[str, int]:
    global _count_cache
    now = time.monotonic()
    if _count_cache and now - _count_cache[0] < COUNT_CACHE_SECONDS:
        return dict(_count_cache[1])
    raw = classification_bucket_counts()
    total = sum(raw.values())
    investable = int(raw.get("priority", 0))
    denied = int(raw.get("reject", 0))
    result = {
        "all": total,
        "investable": investable,
        "review": max(0, total - investable - denied),
        "denied": denied,
    }
    _count_cache = (now, result)
    return dict(result)


def _bucket_counts_for_run(run_id: str) -> dict[str, int]:
    raw = classification_bucket_counts(run_id=run_id)
    total = sum(raw.values())
    investable = int(raw.get("priority", 0))
    denied = int(raw.get("reject", 0))
    return {
        "all": total,
        "investable": investable,
        "review": max(0, total - investable - denied),
        "denied": denied,
    }


def _current_run_id() -> str:
    status = (
        read_pipeline_status()
        if shared_pipeline_backend_available()
        else read_local_pipeline_status()
    )
    return str(status.get("run_id", "") or "__no_current_run__")


def _clean_search_row(row: dict, source: str) -> dict:
    return {
        "domain": row.get("domain", ""),
        "url": row.get("final_url") or f"https://{row.get('domain', '')}",
        "company": row.get("company_name") or row.get("title") or row.get("domain", ""),
        "description": row.get("description") or row.get("snippet") or row.get("excerpt") or "",
        "score": row.get("fit_score", ""),
        "source": source,
        "updated": row.get("updated_at") or row.get("crawled_at") or "",
    }


def _safe_corpus_rows(rows: list[dict]) -> list[dict]:
    """Keep search highlighting useful without returning executable HTML."""
    safe = []
    for original in rows:
        row = dict(original)
        snippet = str(row.get("snippet", ""))
        row["snippet"] = re.sub(r"<(?!/?mark\b)[^>]*>", "", snippet, flags=re.I)
        safe.append(row)
    return safe


def _with_claude(rows: list[dict]) -> list[dict]:
    decisions: dict[str, dict] = {}
    queue_statuses: dict[str, str] = {}
    domains = [row.get("domain", "") for row in rows]
    for start in range(0, len(domains), 500):
        decisions.update(claude_decisions_for_domains(domains[start:start + 500]))
    form_d: dict[str, dict] = {}
    try:
        with connect_store(read_only=True) as conn:
            for start in range(0, len(domains), 500):
                batch = domains[start:start + 500]
                marks = ",".join("?" for _ in batch)
                matches = conn.execute(
                    "SELECT domain,filing_date,filing_url,offering_amount,amount_sold "
                    f"FROM form_d_filings WHERE domain IN ({marks}) ORDER BY filing_date DESC",
                    batch,
                ).fetchall()
                for match in matches:
                    form_d.setdefault(str(match["domain"]), dict(match))
                queued = conn.execute(
                    "SELECT domain,status FROM claude_review_queue "
                    f"WHERE domain IN ({marks})",
                    batch,
                ).fetchall()
                queue_statuses.update(
                    (str(item["domain"]), str(item["status"] or ""))
                    for item in queued
                )
    except Exception:
        pass
    return [
        {
            **row,
            "claude_decision": decisions.get(row.get("domain", ""), {}).get("decision", ""),
            "claude_score": decisions.get(row.get("domain", ""), {}).get("fit_score", ""),
            "claude_reasoning": decisions.get(row.get("domain", ""), {}).get("reasoning", ""),
            "claude_summary": decisions.get(row.get("domain", ""), {}).get("product_summary", ""),
            "claude_concerns": decisions.get(row.get("domain", ""), {}).get("concerns", ""),
            "claude_unknowns": decisions.get(row.get("domain", ""), {}).get("unknowns", ""),
            "claude_green_flags": decisions.get(row.get("domain", ""), {}).get("green_flags", []),
            "claude_dimensions": {
                name: decisions.get(row.get("domain", ""), {}).get(name, "")
                for name in ("thesis_fit", "enterprise_value", "product_depth", "technical_moat", "stage_fit", "evidence_confidence")
            },
            "claude_reviewed_at": decisions.get(row.get("domain", ""), {}).get("updated_at", ""),
            "claude_queue_status": queue_statuses.get(row.get("domain", ""), ""),
            "form_d_match": row.get("domain", "") in form_d,
            "form_d_filing_date": form_d.get(row.get("domain", ""), {}).get("filing_date", ""),
            "form_d_filing_url": form_d.get(row.get("domain", ""), {}).get("filing_url", ""),
            "form_d_offering_amount": form_d.get(row.get("domain", ""), {}).get("offering_amount", 0),
            "form_d_amount_sold": form_d.get(row.get("domain", ""), {}).get("amount_sold", 0),
        }
        for row in rows
    ]


@app.get("/api/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/api/auth/status")
def auth_status(request: Request):
    return {"admin": _is_admin(request)}


@app.post("/api/auth/login")
def admin_login(update: LoginRequest, request: Request, response: Response):
    client = request.client.host if request.client else "unknown"
    now = time.monotonic()
    failures = [
        attempted
        for attempted in _login_failures.get(client, [])
        if now - attempted < LOGIN_WINDOW_SECONDS
    ]
    if len(failures) >= LOGIN_MAX_FAILURES:
        raise HTTPException(429, "Too many login attempts; try again shortly")
    if not hmac.compare_digest(update.password, _admin_password()):
        failures.append(now)
        _login_failures[client] = failures
        raise HTTPException(401, "Incorrect admin password")
    _login_failures.pop(client, None)
    expires = int(time.time()) + ADMIN_SESSION_SECONDS
    response.set_cookie(
        ADMIN_COOKIE,
        _session_token(expires),
        max_age=ADMIN_SESSION_SECONDS,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path="/",
    )
    return {"admin": True}


@app.post("/api/auth/logout")
def admin_logout(response: Response):
    response.delete_cookie(ADMIN_COOKIE, path="/", samesite="strict")
    return {"admin": False}


@app.get("/api/overview")
def overview():
    # The homepage needs classification totals, not remote backup validation.
    # Full dependency and snapshot checks belong to /api/health; running them
    # here used to hold the entire homepage for roughly ten seconds.
    buckets = _counts()
    return {
        "overall": "Healthy",
        "counts": {"classifications": buckets["all"]},
        "buckets": buckets,
    }


@app.get("/api/pipeline/filters")
def pipeline_filters():
    config = read_pipeline_config() if shared_pipeline_backend_available() else {}
    if not config and RUN_CONFIG.exists():
        try:
            config = json.loads(RUN_CONFIG.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            config = {}
    return {
        "selected_extensions": config.get("selected_extensions", [".ai", ".app", ".dev"]),
        "include_keywords": config.get("include_keywords", []),
        "exclude_keywords": config.get("exclude_keywords", []),
        "minimum_score": int(config.get("minimum_score", 55)),
        "claude_daily_limit": int(config.get("claude_daily_limit", 400)),
    }


@app.post("/api/pipeline/filters", dependencies=[Depends(_require_admin)])
def save_pipeline_filters(update: PipelineFilters):
    if shared_pipeline_backend_available():
        certstream_enabled = bool(
            read_control()["desired"].get("pipeline", False)
        )
    else:
        certstream_enabled = bool(read_local_pipeline_status().get("running"))
    if certstream_enabled:
        raise HTTPException(409, "Pause CertStream before changing run filters")
    allowed = {".ai", ".app", ".dev", ".io", ".co", ".xyz", ".tech", ".com"}
    extensions = list(dict.fromkeys(value.lower() for value in update.selected_extensions if value.lower() in allowed))
    if not extensions:
        raise HTTPException(400, "Select at least one extension")
    current = read_pipeline_config() if shared_pipeline_backend_available() else {}
    payload = {
        **current,
        **update.model_dump(),
        "selected_extensions": extensions,
        "include_keywords": [value.strip() for value in update.include_keywords if value.strip()],
        "exclude_keywords": [value.strip() for value in update.exclude_keywords if value.strip()],
    }
    atomic_write_json(RUN_CONFIG, payload)
    publish_pipeline_config(payload, requested_by="react_web")
    record_admin_audit("filters_updated", detail=json.dumps(update.model_dump(), default=str))
    return payload


@app.get("/api/search")
def global_search(q: str = Query(min_length=1, max_length=200)):
    company = [
        _clean_search_row(row, classification_source_label(row.get("content_source", "")))
        for row in search_classifications(q, limit=25)
    ]
    ai = [_clean_search_row(row, ".AI crawler") for row in search_companies(AI_SEARCH_DB, q, limit=25)]
    zone = [_clean_search_row(row, f".{row.get('tld', '')} crawler") for row in search_pages(q, limit=25, database=ZONE_UNIVERSE_DB)]
    registered = []
    try:
        with ai_connect(AI_SEARCH_DB, read_only=True) as conn:
            pending_rows = conn.execute(
                "SELECT domain,latest_crawl_status,last_seen_at,last_crawled_at "
                "FROM domains WHERE lower(domain) LIKE ? "
                "ORDER BY last_seen_at DESC LIMIT 25",
                (f"%{q.strip().lower()}%",),
            ).fetchall()
        registered = [
            {
                "domain": str(row["domain"]),
                "url": f"https://{row['domain']}",
                "company": str(row["domain"]),
                "description": (
                    "Waiting for website scraping and scoring."
                    if str(row["latest_crawl_status"] or "pending") != "success"
                    else "Website is registered in Domain Intake."
                ),
                "score": "Scoring",
                "source": f"Domain Intake · {str(row['latest_crawl_status'] or 'pending').replace('_', ' ')}",
                "updated": str(row["last_crawled_at"] or row["last_seen_at"] or ""),
            }
            for row in pending_rows
        ]
    except Exception:
        registered = []
    # A company can exist in all three stores. Prefer its canonical
    # classification record and merge corpus hits instead of showing the same
    # domain several times in the unified search view.
    merged: dict[str, dict] = {}
    for row in company + ai + zone + registered:
        domain = stable_domain(row.get("domain", ""))
        key = domain or clean(row.get("url", "")).lower()
        if not key:
            continue
        if key not in merged:
            merged[key] = row
            continue
        existing = merged[key]
        sources = list(dict.fromkeys(
            part.strip()
            for part in f"{existing.get('source', '')} · {row.get('source', '')}".split("·")
            if part.strip()
        ))
        existing["source"] = " · ".join(sources)
        if not existing.get("description") and row.get("description"):
            existing["description"] = row["description"]
        if not existing.get("score") and row.get("score"):
            existing["score"] = row["score"]
    return {"rows": list(merged.values()), "total": len(merged)}


@app.get("/api/classifications")
def classifications(
    bucket: str = "all",
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=25, le=250),
    sort: str = "Recently classified",
    scope: Literal["current", "history"] = "history",
):
    if bucket not in BUCKETS:
        raise HTTPException(400, "Unknown bucket")
    run_id = _current_run_id() if scope == "current" else ""
    counts = _bucket_counts_for_run(run_id) if scope == "current" else _counts()
    rows = query_classifications(
        BUCKETS[bucket],
        limit=page_size,
        offset=(page - 1) * page_size,
        sort=sort,
        run_id=run_id,
    )
    hidden = hidden_domain_set([row["domain"] for row in rows])
    tracked = tracked_domain_set([row["domain"] for row in rows])
    return {
        "rows": _with_claude([
            {
                **row,
                "hidden": row["domain"] in hidden,
                "tracked": row["domain"] in tracked,
            }
            for row in rows
        ]),
        "total": counts[bucket],
        "page": page,
        "page_size": page_size,
    }


@app.get("/api/classifications/{domain}")
def classification(domain: str):
    rows = query_classifications_by_domains([domain])
    if not rows:
        raise HTTPException(404, "Company not found")
    return rows[0]


@app.get("/api/labels")
def labels(
    label: Literal["yes", "no"], q: str = "",
    scope: Literal["current", "history"] = "history",
    page: int = Query(1, ge=1), page_size: int = Query(100, ge=25, le=500),
):
    rows, total = query_training_labels(
        label, search=q,
        # YES/NO are durable human training baskets, not pipeline-run output.
        # Keep accepting the legacy scope parameter for old clients, but it
        # must never change which basket entries are returned.
        run_id="",
        limit=page_size, offset=(page - 1) * page_size
    )
    hidden = hidden_domain_set([row["domain"] for row in rows])
    return {
        "rows": [
            {**row, "url": f"https://{row['domain']}"}
            for row in rows if row["domain"] not in hidden
        ],
        "total": total, "page": page, "page_size": page_size,
    }


@app.post("/api/labels", dependencies=[Depends(_require_admin)])
def set_label(update: LabelUpdate):
    save_basket_entry(update.domain, update.label, update.rating, source="human", reason=update.reason)
    record_manual_move(
        [update.domain],
        target_label=update.label,
        target_bucket="priority" if update.label == "yes" else "reject",
        scores={stable_domain(update.domain): update.rating},
        note=update.reason,
        source="human",
    )
    record_admin_audit("label_set", update.domain, f"{update.label} score {update.rating}")
    return {"ok": True, "domain": stable_domain(update.domain), "label": update.label}


@app.delete("/api/labels/{domain}", dependencies=[Depends(_require_admin)])
def delete_label(domain: str):
    remove_basket_entry(domain)
    record_admin_audit("label_removed", domain)
    return {"ok": True}


@app.post("/api/hidden", dependencies=[Depends(_require_admin)])
def set_hidden(update: HiddenUpdate):
    changed = hide_domains(update.domains) if update.hidden else unhide_domains(update.domains)
    record_admin_audit("domains_hidden" if update.hidden else "domains_unhidden", ",".join(update.domains), f"changed={changed}")
    return {"ok": True, "changed": changed}


@app.get("/api/tracking")
def tracking(limit: int = Query(100, ge=1, le=500)):
    tracked_rows = list_tracked_domains(limit=limit)
    tracking_meta = {row["domain"]: row for row in tracked_rows}
    records = query_classifications_by_domains(list(tracking_meta))
    by_domain = {row["domain"]: row for row in records}
    ordered = [
        {
            **by_domain[domain],
            "tracked": True,
            "tracked_at": meta["tracked_at"],
            "tracking_status": meta.get("tracking_status", "tracking"),
            "reached_out_at": meta.get("reached_out_at", ""),
        }
        for domain, meta in tracking_meta.items()
        if domain in by_domain
    ]
    return {"rows": _with_claude(ordered), "total": len(ordered)}


@app.post("/api/tracking", dependencies=[Depends(_require_admin)])
def set_tracking(update: TrackingUpdate):
    domain = stable_domain(update.domain)
    if not domain:
        raise HTTPException(400, "Invalid domain")
    if not classification_record_for_domain(domain):
        raise HTTPException(404, "Company not found")
    track_domains([domain])
    record_admin_audit("company_tracked", domain)
    return {"ok": True, "domain": domain, "tracked": True}


@app.delete("/api/tracking/{domain}", dependencies=[Depends(_require_admin)])
def delete_tracking(domain: str):
    target = stable_domain(domain)
    if not target:
        raise HTTPException(400, "Invalid domain")
    untrack_domains([target])
    record_admin_audit("company_untracked", target)
    return {"ok": True, "domain": target, "tracked": False}


@app.patch("/api/tracking/{domain}", dependencies=[Depends(_require_admin)])
def update_tracking(domain: str, update: TrackingStatusUpdate):
    target = stable_domain(domain)
    if not target:
        raise HTTPException(400, "Invalid domain")
    if not set_tracking_status(target, update.status):
        raise HTTPException(404, "Tracked company not found")
    record_admin_audit("tracking_status_updated", target, update.status)
    return {"ok": True, "domain": target, "status": update.status}


@app.post("/api/tracking/{domain}/kill", dependencies=[Depends(_require_admin)])
def kill_tracked_company(domain: str):
    """Close a tracked card while retaining domain + score in the YES basket."""
    target = stable_domain(domain)
    if not target:
        raise HTTPException(400, "Invalid domain")
    record = classification_record_for_domain(target)
    if not record:
        raise HTTPException(404, "Company not found")
    rating = max(0, min(100, int(record.get("fit_score", 0) or 0)))
    save_basket_entry(
        target,
        "yes",
        rating,
        source="human",
        reason="tracking_killed_to_yes",
    )
    record_manual_move(
        [target],
        target_label="yes",
        target_bucket="priority",
        scores={target: rating},
        note="tracking_killed_to_yes",
        source="human",
    )
    hide_domains([target])
    untrack_domains([target])
    record_admin_audit("tracked_company_killed_to_yes", target, f"score={rating}")
    return {
        "ok": True,
        "domain": target,
        "rating": rating,
        "label": "yes",
        "hidden": True,
        "tracked": False,
    }


@app.get("/api/certstream/status")
def certstream_status():
    if shared_pipeline_backend_available():
        status = read_pipeline_status() or {
            "running": False,
            "stage": "waiting for worker status",
            "queue": {},
        }
    else:
        status = read_local_pipeline_status()
    run_id = str(status.get("run_id", "") or "")
    current_run_queue = {
        "total": 0,
        "awaiting_first_attempt": 0,
        "processing": 0,
        "retrying": 0,
        "complete": 0,
        "failed": 0,
        "attempts": 0,
    }
    if run_id:
        conn = None
        try:
            conn = connect_pipeline_db()
            row = conn.execute(
                """
                SELECT
                    count(*) AS total,
                    sum(CASE WHEN status='pending' AND attempts=0 THEN 1 ELSE 0 END)
                        AS awaiting_first_attempt,
                    sum(CASE WHEN status='processing' THEN 1 ELSE 0 END)
                        AS processing,
                    sum(CASE WHEN status='pending' AND attempts>0 THEN 1 ELSE 0 END)
                        AS retrying,
                    sum(CASE WHEN status='complete' THEN 1 ELSE 0 END)
                        AS complete,
                    sum(CASE WHEN status='failed' THEN 1 ELSE 0 END)
                        AS failed,
                    sum(attempts) AS attempts
                FROM domains
                WHERE first_seen_run_id=?
                """,
                (run_id,),
            ).fetchone()
            if row:
                current_run_queue = {
                    key: int(row[key] or 0)
                    for key in current_run_queue
                }
        except Exception:
            # Worker status remains available during a transient queue read.
            pass
        finally:
            if conn is not None:
                conn.close()
    retained_in_buckets = (
        int(_bucket_counts_for_run(run_id).get("all", 0))
        if run_id else 0
    )
    # A completed domain is not necessarily a retained company. Most certificate
    # domains are screened out and intentionally discarded; expose that side of
    # the funnel explicitly so the UI never implies that every completed screen
    # should appear in a classification bucket.
    screening_complete = int(current_run_queue.get("complete", 0) or 0)
    discarded_after_screening = max(screening_complete - retained_in_buckets, 0)
    desired = bool(read_control()["desired"].get("pipeline", False))
    return {
        **status,
        "current_run_queue": current_run_queue,
        "retained_in_buckets": retained_in_buckets,
        "discarded_after_screening": discarded_after_screening,
        "desired_running": desired,
        "pause_requested": not desired,
    }


@app.get("/api/claude/status")
def claude_status():
    with connect_store(read_only=True) as conn:
        rows = conn.execute(
            "SELECT decision,count(*) AS total,max(updated_at) AS latest "
            "FROM claude_diligence WHERE diligence_version=? GROUP BY decision",
            (DILIGENCE_VERSION,),
        ).fetchall()
        legacy = int(conn.execute(
            "SELECT count(*) AS total FROM claude_diligence "
            "WHERE diligence_version<>? OR diligence_version IS NULL",
            (DILIGENCE_VERSION,),
        ).fetchone()["total"])
    counts = {str(row["decision"]): int(row["total"]) for row in rows}
    latest = max((str(row["latest"] or "") for row in rows), default="")
    status = read_pipeline_status() if shared_pipeline_backend_available() else read_local_pipeline_status()
    return {
        "reviewed": counts.get("KEEP", 0) + counts.get("REJECT", 0),
        "kept": counts.get("KEEP", 0),
        "rejected": counts.get("REJECT", 0),
        "latest": latest,
        "rubric_version": DILIGENCE_VERSION,
        "awaiting_rescore": legacy,
        "calls_this_run": int(status.get("claude_calls", 0) or 0),
        "warning": str(status.get("claude_warning", "") or ""),
    }


@app.get("/api/claude/top")
def claude_top_companies(limit: int = Query(6, ge=1, le=20)):
    rows = top_claude_keeps(
        min(limit * 3, 20), diligence_version=DILIGENCE_VERSION
    )
    hidden = hidden_domain_set([row["domain"] for row in rows])
    visible = [row for row in rows if row["domain"] not in hidden][:limit]
    tracked = tracked_domain_set([row["domain"] for row in visible])
    return {
        "rows": [
            {
                **row,
                "tracked": row["domain"] in tracked,
                "claude_decision": "KEEP",
                "claude_green_flags": row.get("green_flags", []),
            }
            for row in visible
        ]
    }


@app.post("/api/claude/review/{domain}", dependencies=[Depends(_require_admin)])
def manual_claude_review(domain: str, request: ClaudeReviewRequest = ClaudeReviewRequest()):
    target = stable_domain(domain)
    if not target:
        raise HTTPException(400, "Invalid domain")
    existing = claude_decisions_for_domains([target]).get(target)
    if existing and existing.get("diligence_version") not in {None, "", DILIGENCE_VERSION}:
        existing = None
    if existing and str(existing.get("decision", "")).upper() in {"KEEP", "REJECT"}:
        return {"status": "already_reviewed", **existing}
    if not classification_record_for_domain(target):
        raise HTTPException(404, "Company not found")
    if not request.override_daily_limit:
        pipeline = connect_pipeline_db()
        try:
            if claude_allowance(pipeline) <= 0:
                raise HTTPException(
                    429,
                    {"code": "CLAUDE_DAILY_LIMIT", "message": "Hit daily limit"},
                )
        finally:
            pipeline.close()
    queued = enqueue_claude_review(
        target, override_budget=request.override_daily_limit
    )
    record_admin_audit(
        "claude_limit_overridden" if request.override_daily_limit else "claude_queued",
        target,
    )
    return queued


@app.get("/api/claude/review/{domain}")
def manual_claude_review_status(domain: str):
    queued = claude_review_status(domain)
    decision = claude_decisions_for_domains([domain]).get(stable_domain(domain))
    if not queued and not decision:
        raise HTTPException(404, "Claude review not found")
    return {**(queued or {}), "result": decision}


@app.get("/api/claude/queue")
def claude_queue(limit: int = Query(200, ge=1, le=500)):
    rows = claude_review_queue_rows(limit)
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
    return {"rows": rows, "counts": counts}


def _csv_response(rows: list[dict], filename: str):
    output = io.StringIO()
    columns = list(rows[0]) if rows else ["domain"]
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/classifications/{bucket}")
def export_classifications(
    bucket: str, scope: Literal["current", "history"] = "history"
):
    if bucket not in BUCKETS:
        raise HTTPException(400, "Unknown bucket")
    rows: list[dict] = []
    while True:
        batch = query_classifications(
            BUCKETS[bucket], limit=250, offset=len(rows),
            sort="Recently classified",
            run_id=_current_run_id() if scope == "current" else "",
        )
        rows.extend(batch)
        if len(batch) < 250:
            break
    return _csv_response(_with_claude(rows), f"glasswing-{bucket}.csv")


@app.get("/api/export/labels/{label}")
def export_labels(
    label: Literal["yes", "no"],
    scope: Literal["current", "history"] = "history",
):
    rows: list[dict] = []
    total = 1
    while len(rows) < total:
        batch, total = query_training_labels(
            label,
            run_id=_current_run_id() if scope == "current" else "",
            limit=500, offset=len(rows),
        )
        rows.extend(batch)
    return _csv_response(rows, f"glasswing-{label}-basket.csv")


def _execute_queued_claude(target: str, *, override_budget: bool = False) -> bool:
    if not os.getenv("ANTHROPIC_API_KEY", "").strip():
        finish_claude_review(target, error="Anthropic API key is not configured")
        return True
    pipeline = connect_pipeline_db()
    try:
        if not override_budget and claude_allowance(pipeline) <= 0:
            defer_claude_review(target, "Waiting for Claude daily/hourly budget")
            return False
    finally:
        pipeline.close()
    record = classification_record_for_domain(target)
    if not record:
        finish_claude_review(target, error="Company not found")
        return True
    import pandas as pd

    with tempfile.TemporaryDirectory(prefix="glasswing-claude-") as folder:
        input_path = Path(folder) / "company.csv"
        report_path = Path(folder) / "attempt.json"
        pd.DataFrame([{**record, "qualified_for_llm": "true"}]).to_csv(input_path, index=False)
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "claude_classifier.py"),
                    str(input_path),
                    "--reclassify", "--limit", "1", "--minimum-score", "0",
                    "--percentage-cap", "1", "--max-calls", "1",
                    "--exceptional-score", "0", "--workers", "1",
                    "--attempt-report", str(report_path),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=240,
            )
        except subprocess.TimeoutExpired as exc:
            finish_claude_review(target, error="Claude review timed out")
            return True
        attempted = report_path.exists()
        attempt_data = {}
        if attempted:
            try:
                attempt_data = json.loads(report_path.read_text(encoding="utf-8"))
            except (OSError, ValueError, TypeError):
                attempt_data = {}
    if attempted:
        pipeline = connect_pipeline_db()
        try:
            pipeline.execute(
                "INSERT INTO claude_calls(called_at,domain) VALUES (?,?)",
                (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), target),
            )
            pipeline.commit()
        finally:
            pipeline.close()
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "Claude review failed")[-600:]
        finish_claude_review(target, error=detail)
        return True
    result = claude_decisions_for_domains([target]).get(target)
    if not result:
        failure = str(
            (attempt_data.get("failure_details") or {}).get(target, "")
        ).strip()
        detail = failure or (completed.stdout or completed.stderr or "").strip()[-600:]
        finish_claude_review(
            target,
            error=detail or "Claude returned no valid diligence decision",
        )
        return True
    finish_claude_review(target)
    record_admin_audit("claude_completed", target, f"{result.get('decision')} score {result.get('fit_score')}")
    return True


async def _claude_queue_worker() -> None:
    recover_claude_review_queue()
    while True:
        queued = await asyncio.to_thread(claim_claude_review)
        if queued:
            completed = await asyncio.to_thread(
                _execute_queued_claude,
                str(queued["domain"]),
                override_budget=bool(queued.get("override_budget")),
            )
            if not completed:
                await asyncio.sleep(60)
        else:
            await asyncio.sleep(3)


async def _landscape_worker() -> None:
    while True:
        try:
            queued = await asyncio.to_thread(claim_landscape_job)
        except Exception as exc:
            print(f"Competitive landscape worker waiting for storage: {exc}", file=sys.stderr)
            await asyncio.sleep(10)
            continue
        if queued:
            await asyncio.to_thread(execute_landscape_job, queued)
        else:
            await asyncio.sleep(3)


async def _form_d_worker() -> None:
    from form_d import desired, ingest_once

    while True:
        try:
            if await asyncio.to_thread(desired):
                await asyncio.to_thread(ingest_once)
                await asyncio.sleep(600)
            else:
                await asyncio.sleep(5)
        except Exception as exc:
            print(f"Form D worker retrying: {exc}", file=sys.stderr)
            await asyncio.sleep(60)


@app.get("/api/form-d")
def form_d_filings(bucket: str = "all", limit: int = Query(100, ge=1, le=250)):
    from form_d import dashboard, desired

    result = dashboard(bucket=bucket, limit=limit)
    result["desired"] = desired()
    return result


@app.post("/api/form-d/control", dependencies=[Depends(_require_admin)])
def form_d_control(update: WorkerUpdate):
    from form_d import set_desired

    set_desired(update.desired)
    record_admin_audit(
        "form_d_resumed" if update.desired else "form_d_paused",
        "form_d",
    )
    return {"ok": True, "desired": update.desired}


@app.post("/api/form-d/refresh", dependencies=[Depends(_require_admin)])
async def form_d_refresh():
    from form_d import ingest_once

    result = await asyncio.to_thread(ingest_once)
    return result


@app.get("/api/competitive-landscapes")
def competitive_landscapes():
    return {"jobs": list_landscape_jobs()}


@app.post("/api/competitive-landscapes", dependencies=[Depends(_require_admin)])
async def submit_competitive_landscape(
    website: str = Form(...),
    requested_count: int = Form(25),
    notes: str = Form(""),
    template: UploadFile | None = File(None),
    materials: list[UploadFile | str] = File(default=[]),
):
    website = website.strip()
    if not website or "." not in website:
        raise HTTPException(400, "Enter a valid company website")
    if not 5 <= requested_count <= 75:
        raise HTTPException(400, "Competitor count must be between 5 and 75")
    if template and template.filename and not template.filename.lower().endswith(".xlsx"):
        raise HTTPException(400, "The template must be an .xlsx workbook")
    template_bytes = await template.read() if template and template.filename else b""
    if len(template_bytes) > 15_000_000:
        raise HTTPException(413, "Template exceeds the 15 MB limit")
    safe_materials: list[tuple[str, bytes]] = []
    allowed = {".pdf", ".docx", ".xlsx", ".csv", ".txt", ".md"}
    total = len(template_bytes)
    for number, upload in enumerate(materials):
        if isinstance(upload, str):
            if not upload.strip():
                continue
            raise HTTPException(400, "Supporting materials must be uploaded files")
        filename = (upload.filename or "").strip()
        # Browsers may serialize an untouched multi-file input as a blank
        # multipart file part. It is absence, not an unsupported attachment.
        if not filename:
            continue
        suffix = Path(filename).suffix.lower()
        if suffix not in allowed:
            raise HTTPException(400, f"Unsupported supporting file: {filename}")
        content = await upload.read()
        total += len(content)
        if total > 40_000_000:
            raise HTTPException(413, "Combined uploads exceed the 40 MB limit")
        safe_materials.append((f"material-{number + 1}{suffix}", content))
    job_id = uuid.uuid4().hex
    folder = LANDSCAPE_ROOT / job_id
    folder.mkdir(parents=True, exist_ok=True)
    if template_bytes:
        (folder / "template.xlsx").write_bytes(template_bytes)
    elif DEFAULT_LANDSCAPE_TEMPLATE.is_file():
        (folder / "template.xlsx").write_bytes(DEFAULT_LANDSCAPE_TEMPLATE.read_bytes())
    else:
        create_default_template(folder / "template.xlsx")
    for name, content in safe_materials:
        (folder / name).write_bytes(content)
    create_landscape_job(website, "template.xlsx", [name for name, _ in safe_materials], notes[:5000], requested_count, job_id)
    record_admin_audit("competitive_landscape_queued", website, f"job={job_id} competitors={requested_count}")
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/competitive-landscapes/{job_id}")
def competitive_landscape_status(job_id: str):
    result = landscape_job(job_id)
    if not result:
        raise HTTPException(404, "Competitive landscape job not found")
    result.pop("result_json", None)
    result.pop("output_path", None)
    return result


@app.get("/api/competitive-landscapes/{job_id}/download")
def download_competitive_landscape(job_id: str):
    result = landscape_job(job_id)
    if not result or result.get("status") != "completed":
        raise HTTPException(404, "Completed workbook not found")
    path = Path(str(result.get("output_path") or ""))
    expected = (LANDSCAPE_ROOT / job_id).resolve()
    if not path.is_file() or path.resolve().parent != expected:
        raise HTTPException(404, "Workbook file is unavailable")
    name = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(result.get("company_name") or "competitive" )).strip("-").lower()
    return FileResponse(path, filename=f"{name or 'competitive'}-landscape.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.post("/api/competitive-landscapes/{job_id}/retry", dependencies=[Depends(_require_admin)])
def retry_competitive_landscape(job_id: str):
    if not retry_landscape_job(job_id):
        raise HTTPException(409, "Only failed landscape jobs can be retried")
    record_admin_audit("competitive_landscape_retried", job_id, "export/research retry")
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/runtime")
def workers():
    snapshot = runtime_status()
    services = snapshot.get("services", {})
    pipeline = (
        read_pipeline_status()
        if shared_pipeline_backend_available()
        else read_local_pipeline_status()
    )
    if "pipeline" in services:
        services["pipeline"]["actual_status"] = str(
            pipeline.get("stage", "unknown")
        )
    try:
        with ai_connect(AI_SEARCH_DB, read_only=True) as conn:
            ai_state = ai_worker_state(conn)
        if "ai_crawler" in services:
            services["ai_crawler"]["actual_status"] = ai_state.get(
                "crawler_status", "unknown"
            )
    except Exception:
        if "ai_crawler" in services:
            services["ai_crawler"]["actual_status"] = "unknown"
    try:
        with zone_connect(ZONE_UNIVERSE_DB, read_only=True) as conn:
            zone_state = zone_worker_state(conn)
        if "zone_crawler" in services:
            services["zone_crawler"]["actual_status"] = zone_state.get(
                "crawler_status", zone_state.get("crawler_phase", "unknown")
            )
    except Exception:
        if "zone_crawler" in services:
            services["zone_crawler"]["actual_status"] = "unknown"
    return snapshot


@app.post("/api/runtime/{service}", dependencies=[Depends(_require_admin)])
def worker_control(service: str, update: WorkerUpdate):
    if service not in SERVICES and service != "all":
        raise HTTPException(404, "Unknown worker")
    targets = SERVICES if service == "all" else (service,)
    request_services({name: update.desired for name in targets}, requested_by="react_web")
    record_admin_audit("workers_resumed" if update.desired else "workers_paused", service)
    return {"ok": True, "services": list(targets), "desired": update.desired}


@app.get("/api/admin/audit", dependencies=[Depends(_require_admin)])
def admin_activity(limit: int = Query(100, ge=1, le=500)):
    return {"rows": admin_audit_rows(limit)}


@app.get("/api/health")
def system_health(full: bool = False, refresh: int = 0):
    global _health_cache
    # Reopening System Health should not repeat several full-table counts.
    # Explicit refreshes and deep checks always bypass this short cache.
    if not full and not refresh:
        now = time.monotonic()
        if _health_cache and now - _health_cache[0] < HEALTH_CACHE_SECONDS:
            return _health_cache[1]
        with _health_cache_lock:
            now = time.monotonic()
            if _health_cache and now - _health_cache[0] < HEALTH_CACHE_SECONDS:
                return _health_cache[1]
            report = health_report(include_integrity=False)
            _health_cache = (now, report)
            return report
    report = health_report(include_integrity=full)
    if not full:
        _health_cache = (time.monotonic(), report)
    return report


@app.get("/api/ai/search")
def ai_search(q: str, page: int = 1, page_size: int = 100, minimum_words: int = 25):
    rows = search_companies(
        AI_SEARCH_DB, q, limit=min(page_size, 200),
        offset=max(0, page - 1) * min(page_size, 200), minimum_words=minimum_words,
    )
    return {"rows": _safe_corpus_rows(rows), "page": page, "stats": search_stats(AI_SEARCH_DB)}


@app.get("/api/corpus/{kind}/stats")
def corpus_stats(kind: Literal["ai", "zone"]):
    if kind == "ai":
        with ai_connect(AI_SEARCH_DB, read_only=True) as conn:
            details = ai_diagnostics(conn)
            worker = ai_worker_state(conn)
            heartbeat = conn.execute(
                "SELECT max(updated_at) AS updated_at FROM worker_state"
            ).fetchone()["updated_at"]
        searchable = search_stats(AI_SEARCH_DB)
        return {
            "available": details.get("domains", 0),
            "pending": details.get("pending", 0),
            "processed": details.get("crawled", 0),
            "crawled": details.get("successful", 0),
            "failed": details.get("failed", 0),
            "searchable": searchable.get("indexed_pages", 0),
            "crawler_status": worker.get("crawler_status", "unknown"),
            "worker_status": worker.get("worker_status", "unknown"),
            "last_error": worker.get("last_crawl_error", ""),
            "updated_at": heartbeat or "",
        }
    with zone_connect(ZONE_UNIVERSE_DB, read_only=True) as conn:
        details = zone_diagnostics(conn)
        worker = zone_worker_state(conn)
        try:
            heartbeat_row = conn.execute(
                "SELECT max(updated_at) AS updated_at FROM worker_state"
            ).fetchone()
            heartbeat = str(heartbeat_row["updated_at"] or "")
            latest_deal_row = conn.execute(
                "SELECT max(crawled_at) AS crawled_at FROM pages"
            ).fetchone()
            latest_deal_at = str(latest_deal_row["crawled_at"] or "")
        except (AttributeError, KeyError, TypeError):
            heartbeat = ""
            latest_deal_at = ""
    retained = int(details.get("success", 0) or 0)
    zone_bucket_counts = source_bucket_counts(CORPUS_SOURCES["zone"])
    investable = int(zone_bucket_counts.get("priority", 0) or 0)
    needs_review = int(zone_bucket_counts.get("review", 0) or 0)
    discarded = max(
        0,
        int(details.get("low_priority_quarantined", 0) or 0)
        + int(details.get("preflight_rejected", 0) or 0)
        + int(details.get("network_failed", 0) or 0)
        + int(details.get("network_success", 0) or 0)
        - retained,
    )
    processed = (
        int(details.get("attempted", 0) or 0)
        + int(details.get("low_priority_quarantined", 0) or 0)
    )
    attempted = int(details.get("attempted", 0) or 0)
    promising = int(details.get("preflight_promising", 0) or 0)
    preflight_rejected = int(details.get("preflight_rejected", 0) or 0)
    # A homepage that produced either a promising or explicit rejection
    # decision was actually reached and parsed. Full-crawl success is later
    # in the funnel and is not the right denominator for preflight relevance.
    live = promising + preflight_rejected
    try:
        recent_batch = ast.literal_eval(worker.get("last_batch", "{}"))
        if not isinstance(recent_batch, dict):
            recent_batch = {}
    except (SyntaxError, ValueError, TypeError):
        recent_batch = {}
    try:
        batch_rate = round(
            float(worker.get("rolling_domains_per_second", worker.get("last_batch_domains_per_second", 0)) or 0), 2
        )
        instantaneous_batch_rate = round(
            float(worker.get("last_batch_domains_per_second", 0) or 0), 2
        )
    except (TypeError, ValueError):
        batch_rate = 0
        instantaneous_batch_rate = 0
    return {
        "available": details.get("queued_domains", 0),
        "pending": max(
            0,
            int(details.get("queued_domains", 0) or 0)
            - int(details.get("attempted", 0) or 0)
            - int(details.get("low_priority_quarantined", 0) or 0),
        ),
        "processed": processed,
        "attempted": attempted,
        "live": live,
        "crawled": retained,
        "retained": retained,
        "investable": investable,
        "needs_review": needs_review,
        "discarded": discarded,
        "failed": details.get("network_failed", 0),
        "preflight_rejected": details.get("preflight_rejected", 0),
        "promising": promising,
        "searchable": details.get("searchable_pages", 0),
        "live_rate": round((live / attempted * 100) if attempted else 0, 2),
        "relevance_rate": round((promising / live * 100) if live else 0, 2),
        "candidate_yield": round((retained / processed * 100) if processed else 0, 4),
        "investable_yield": round((investable / processed * 100) if processed else 0, 4),
        "batch_rate": batch_rate,
        "instantaneous_batch_rate": instantaneous_batch_rate,
        "recent_batch": recent_batch,
        "updated_at": heartbeat,
        "last_deal_at": latest_deal_at,
        "crawler_status": worker.get("crawler_status", "unknown"),
        "crawler_phase": worker.get("crawler_phase", "unknown"),
    }


@app.get("/api/zone/search")
def zone_search(q: str, tld: str = "", page: int = 1, page_size: int = 100, minimum_words: int = 25):
    rows = search_pages(
        q, tld=tld, limit=min(page_size, 200),
        offset=max(0, page - 1) * min(page_size, 200),
        minimum_words=minimum_words, database=ZONE_UNIVERSE_DB,
    )
    return {"rows": _safe_corpus_rows(rows), "page": page}


@app.get("/api/corpus/{kind}")
def corpus_companies(
    kind: Literal["ai", "zone"],
    bucket: str = "all",
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=25, le=250),
    sort: str = "Recently classified",
):
    if bucket not in BUCKETS:
        raise HTTPException(400, "Unknown bucket")
    raw = source_bucket_counts(CORPUS_SOURCES[kind])
    total = sum(int(raw.get(name, 0)) for name in BUCKETS[bucket])
    rows = query_source_classifications(
        CORPUS_SOURCES[kind],
        BUCKETS[bucket],
        limit=page_size,
        offset=(page - 1) * page_size,
        sort=sort,
    )
    return {"rows": _with_claude(rows), "total": total, "page": page, "page_size": page_size}


@app.get("/api/corpus/zone/candidates")
def zone_candidates(limit: int = Query(25, ge=1, le=50)):
    """Return the small retained deal queue: approved plus high-score review."""
    raw = source_bucket_counts(CORPUS_SOURCES["zone"])
    rows = query_source_classifications(
        CORPUS_SOURCES["zone"],
        ("priority", "review"),
        limit=limit,
        offset=0,
        sort="Recently classified",
    )
    hidden = hidden_domain_set([row["domain"] for row in rows])
    tracked = tracked_domain_set([row["domain"] for row in rows])
    return {
        "rows": _with_claude([
            {
                **row,
                "hidden": row["domain"] in hidden,
                "tracked": row["domain"] in tracked,
            }
            for row in rows
        ]),
        "total": int(raw.get("priority", 0) or 0)
        + int(raw.get("review", 0) or 0),
        "investable": int(raw.get("priority", 0) or 0),
        "needs_review": int(raw.get("review", 0) or 0),
    }


def _queue_ai_domains(values: list[str], source: str) -> dict[str, object]:
    from ai_search.database import add_domain
    from ai_search.database import connect as ai_connect

    normalized = [normalize_any_domain(value) for value in values]
    invalid = [str(value).strip() for value, domain in zip(values, normalized) if not domain]
    domains = list(dict.fromkeys(domain for domain in normalized if domain))
    if not domains:
        raise HTTPException(
            400,
            "Enter a complete domain such as company.com or https://company.ai",
        )
    added = 0
    requeued = 0
    searchable = 0
    states: dict[str, str] = {}
    with ai_connect(AI_SEARCH_DB) as conn:
        for domain in domains:
            existing = conn.execute(
                "SELECT latest_crawl_status,last_document_count FROM domains WHERE domain=?",
                (domain,),
            ).fetchone()
            if existing is None:
                if not add_domain(
                    conn, domain, source=source, allow_any_tld=True
                ):
                    raise HTTPException(400, f"Invalid domain: {domain}")
                added += 1
                states[domain] = "queued"
            elif (
                str(existing["latest_crawl_status"] or "") == "success"
                and int(existing["last_document_count"] or 0) > 0
            ):
                add_domain(
                    conn, domain, source=source, allow_any_tld=True
                )
                searchable += 1
                states[domain] = "searchable"
            else:
                conn.execute(
                    "UPDATE domains SET active=1,latest_crawl_status='pending',"
                    "last_crawled_at=NULL,next_crawl_at=NULL,crawl_failures=0,"
                    "last_seen_at=?,discovery_source=? WHERE domain=?",
                    (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), source, domain),
                )
                requeued += 1
                states[domain] = "requeued"
        conn.commit()
    classified = {
        str(row.get("domain", ""))
        for row in query_classifications_by_domains(domains)
    }
    needs_scoring = [domain for domain in domains if domain not in classified]
    scoring_result = (
        enqueue_for_screening(
            QUEUE_DB,
            needs_scoring,
            source_run_id="ai_search_cache_reuse_v1",
            requeue_existing=True,
        )
        if needs_scoring
        else {"queued": 0}
    )
    return {
        "received": len(domains),
        "invalid": len(invalid),
        "invalid_examples": invalid[:5],
        "added": added,
        "duplicates": max(0, len(domains) - added),
        "domain": domains[0] if len(domains) == 1 else "",
        "requeued": requeued,
        "searchable": searchable,
        "state": states.get(domains[0], "") if len(domains) == 1 else "",
        "scoring_queued": int(scoring_result.get("queued", 0) or 0),
    }


@app.post("/api/ai/domain", dependencies=[Depends(_require_admin)])
async def intake_domain(payload: DomainIntakeRequest):
    domain = normalize_any_domain(payload.domain)
    if not domain:
        raise HTTPException(
            400,
            "Enter a complete domain such as company.com or https://company.ai",
        )

    # Manual intake is an interactive action, not bulk ingestion. Screen the
    # website in this request and return its real result instead of merely
    # dropping it behind the crawler backlog. The corpus copy is still queued
    # separately so successful content becomes searchable across the app.
    try:
        queue_result = await asyncio.to_thread(
            _queue_ai_domains, [domain], "manual_domain_intake"
        )
    except Exception as exc:
        # A temporary corpus-pool issue must not prevent real-time scoring.
        queue_result = {
            "received": 1, "added": 0, "duplicates": 0,
            "domain": domain, "state": "scoring", "scoring_queued": 0,
            "corpus_warning": str(exc)[:200],
        }

    from filter import screen_records

    try:
        output, stats = await asyncio.wait_for(
            screen_records(
                [{
                    "domain": domain,
                    "registrable": domain,
                    "certificate_name": domain,
                    "source": "manual_domain_intake",
                }],
                workers=1,
                timeout=12,
                homepage_only=False,
                retry_timeouts=False,
                persist=True,
            ),
            timeout=30,
        )
    except asyncio.TimeoutError:
        output = None
        stats = {"timed_out_domains": [domain]}

    row = output.iloc[0].to_dict() if output is not None and not output.empty else {}
    def clean(value):
        if value is None:
            return ""
        try:
            if value != value:  # NaN
                return ""
        except Exception:
            pass
        if hasattr(value, "item"):
            value = value.item()
        return value

    row = {str(key): clean(value) for key, value in row.items()}
    score = int(float(row.get("glasswing_rating") or 0))
    routing_bucket = str(row.get("routing_bucket") or "review")
    bucket = {
        "priority": "Investable",
        "reject": "Hard Denied",
    }.get(routing_bucket, "Needs Review")
    reason = str(
        row.get("routing_reason")
        or row.get("fetch_error")
        or ("Website did not respond within 30 seconds" if not row else "Screening completed")
    )
    record_admin_audit(
        "domain_screened_realtime", domain,
        f"score={score} bucket={routing_bucket}",
    )
    return {
        **queue_result,
        "domain": domain,
        "status": "completed",
        "message": "Screening complete",
        "score": score,
        "bucket": bucket,
        "routing_bucket": routing_bucket,
        "reason": reason.replace("_", " "),
        "company_name": str(row.get("company_name") or row.get("page_title") or domain),
        "description": str(
            row.get("meta_description")
            or row.get("homepage_excerpt")
            or row.get("description")
            or "No website description was available."
        ),
        "category": str(row.get("thesis_pillar") or row.get("category") or ""),
        "stats": stats,
    }


@app.post("/api/ai/upload", dependencies=[Depends(_require_admin)])
async def upload_domains(file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8", errors="ignore")
    lines = content.splitlines()
    if file.filename and file.filename.lower().endswith(".csv") and lines:
        lines = lines[1:]
    result = _queue_ai_domains(
        [line.split(",")[0] for line in lines], "csv_upload"
    )
    record_admin_audit(
        "domains_uploaded", file.filename or "upload",
        f"received={result['received']} added={result['added']}",
    )
    return {
        **result,
        "message": "New domains are queued automatically and become searchable after the crawler finishes.",
    }


DIST = ROOT / "webapp" / "dist"
if DIST.exists():
    @app.get("/api/version")
    def frontend_version():
        index = (DIST / "index.html").read_text(encoding="utf-8")
        match = re.search(r'<script[^>]+src="([^"]+index-[^"]+\.js)"', index)
        return {"bundle": match.group(1) if match else ""}

    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        requested = DIST / path
        if path and requested.is_file():
            return FileResponse(requested)
        return FileResponse(DIST / "index.html")
