"""FastAPI web surface for the React Glasswing sourcing application."""

from __future__ import annotations

import json
import hashlib
import hmac
import os
import re
import sys
import time
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
AI_SRC = ROOT / "ai-domain-search-engine" / "src"
ZONE_SRC = ROOT / "zone-universe" / "src"
for source in (ROOT, AI_SRC, ZONE_SRC):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from ai_search.og_bridge import search_companies, search_stats
from ai_search.database import connect as ai_connect, diagnostics as ai_diagnostics
from classification_store import (
    classification_bucket_counts,
    classification_source_label,
    hidden_domain_set,
    hide_domains,
    query_classifications,
    query_classifications_by_domains,
    query_source_classifications,
    search_classifications,
    source_bucket_counts,
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
)
from continuous_pipeline import read_status as read_local_pipeline_status
from health_monitor import health_report
from runtime_control import (
    SERVICES,
    read_pipeline_status,
    request_services,
    runtime_status,
    shared_pipeline_backend_available,
)
from zone_universe.search import search_pages
from zone_universe.database import connect as zone_connect, diagnostics as zone_diagnostics

app = FastAPI(title="Glasswing Sourcing API", version="1.0.0")
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


class HiddenUpdate(BaseModel):
    domains: list[str]
    hidden: bool = True


class LoginRequest(BaseModel):
    password: str = Field(min_length=1, max_length=200)


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
    report = health_report()
    return {
        "overall": report["overall"],
        "counts": report["counts"],
        "runtime": report["runtime"],
        "buckets": _counts(),
    }


@app.get("/api/search")
def global_search(q: str = Query(min_length=1, max_length=200)):
    company = [
        _clean_search_row(row, classification_source_label(row.get("content_source", "")))
        for row in search_classifications(q, limit=25)
    ]
    ai = [_clean_search_row(row, ".AI crawler") for row in search_companies(AI_SEARCH_DB, q, limit=25)]
    zone = [_clean_search_row(row, f".{row.get('tld', '')} crawler") for row in search_pages(q, limit=25, database=ZONE_UNIVERSE_DB)]
    return {"rows": company + ai + zone}


@app.get("/api/classifications")
def classifications(
    bucket: str = "all",
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=25, le=250),
    sort: str = "Recently classified",
):
    if bucket not in BUCKETS:
        raise HTTPException(400, "Unknown bucket")
    counts = _counts()
    rows = query_classifications(
        BUCKETS[bucket],
        limit=page_size,
        offset=(page - 1) * page_size,
        sort=sort,
    )
    hidden = hidden_domain_set([row["domain"] for row in rows])
    return {
        "rows": [{**row, "hidden": row["domain"] in hidden} for row in rows],
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
def labels(label: Literal["yes", "no"]):
    return {"rows": _basket_payload(YES_BASKET_DATA if label == "yes" else NO_BASKET_DATA)}


@app.post("/api/labels", dependencies=[Depends(_require_admin)])
def set_label(update: LabelUpdate):
    save_basket_entry(update.domain, update.label, update.rating, source="human", reason=update.reason)
    return {"ok": True, "domain": stable_domain(update.domain), "label": update.label}


@app.delete("/api/labels/{domain}", dependencies=[Depends(_require_admin)])
def delete_label(domain: str):
    remove_basket_entry(domain)
    return {"ok": True}


@app.post("/api/hidden", dependencies=[Depends(_require_admin)])
def set_hidden(update: HiddenUpdate):
    changed = hide_domains(update.domains) if update.hidden else unhide_domains(update.domains)
    return {"ok": True, "changed": changed}


@app.get("/api/certstream/status")
def certstream_status():
    if shared_pipeline_backend_available():
        return read_pipeline_status() or {
            "running": False,
            "stage": "waiting for worker status",
            "queue": {},
        }
    return read_local_pipeline_status()


@app.get("/api/runtime")
def workers():
    return runtime_status()


@app.post("/api/runtime/{service}", dependencies=[Depends(_require_admin)])
def worker_control(service: str, update: WorkerUpdate):
    if service not in SERVICES and service != "all":
        raise HTTPException(404, "Unknown worker")
    targets = SERVICES if service == "all" else (service,)
    request_services({name: update.desired for name in targets}, requested_by="react_web")
    return {"ok": True, "services": list(targets), "desired": update.desired}


@app.get("/api/health")
def system_health(full: bool = False):
    return health_report(include_integrity=full)


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
            failed = int(conn.execute(
                "SELECT count(*) AS count FROM domains "
                "WHERE last_crawled_at IS NULL AND latest_crawl_status NOT IN ('pending','')"
            ).fetchone()["count"])
        searchable = search_stats(AI_SEARCH_DB)
        return {
            "available": details.get("domains", 0),
            "pending": details.get("pending", 0),
            "crawled": details.get("crawled", 0),
            "failed": failed,
            "searchable": searchable.get("indexed_pages", 0),
        }
    with zone_connect(ZONE_UNIVERSE_DB, read_only=True) as conn:
        details = zone_diagnostics(conn)
    return {
        "available": details.get("queued_domains", 0),
        "pending": max(0, details.get("queued_domains", 0) - details.get("attempted", 0)),
        "crawled": details.get("success", 0),
        "failed": max(0, details.get("attempted", 0) - details.get("success", 0)),
        "searchable": details.get("searchable_pages", 0),
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
    )
    return {"rows": rows, "total": total, "page": page, "page_size": page_size}


@app.post("/api/ai/upload", dependencies=[Depends(_require_admin)])
async def upload_domains(file: UploadFile = File(...)):
    from ai_search.database import add_domain
    from ai_search.database import connect as ai_connect

    content = (await file.read()).decode("utf-8", errors="ignore")
    lines = content.splitlines()
    if file.filename and file.filename.lower().endswith(".csv") and lines:
        lines = lines[1:]
    domains = list(dict.fromkeys(stable_domain(line.split(",")[0]) for line in lines))
    domains = [domain for domain in domains if domain]
    added = 0
    with ai_connect(AI_SEARCH_DB) as conn:
        before = int(conn.execute("SELECT count(*) AS count FROM domains").fetchone()["count"])
        for domain in domains:
            add_domain(conn, domain)
        conn.commit()
        after = int(conn.execute("SELECT count(*) AS count FROM domains").fetchone()["count"])
        added = max(0, after - before)
    return {
        "received": len(domains),
        "added": added,
        "duplicates": max(0, len(domains) - added),
        "message": "New domains are queued automatically and become searchable after the crawler finishes.",
    }


DIST = ROOT / "webapp" / "dist"
if DIST.exists():
    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        requested = DIST / path
        if path and requested.is_file():
            return FileResponse(requested)
        return FileResponse(DIST / "index.html")
