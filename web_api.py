"""FastAPI web surface for the React Glasswing sourcing application."""

from __future__ import annotations

import asyncio
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
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, Response, UploadFile
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

from ai_search.og_bridge import search_companies, search_stats
from ai_search.database import connect as ai_connect, diagnostics as ai_diagnostics
from classification_store import (
    admin_audit_rows,
    claim_claude_review,
    claude_decisions_for_domains,
    claude_review_status,
    claude_review_queue_rows,
    connect_store,
    enqueue_claude_review,
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
    recover_claude_review_queue,
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
    RUN_CONFIG,
    atomic_write_json,
)
from continuous_pipeline import (
    claude_allowance,
    connect_db as connect_pipeline_db,
    read_status as read_local_pipeline_status,
)
from health_monitor import health_report
from runtime_control import (
    SERVICES,
    read_pipeline_status,
    read_pipeline_config,
    publish_pipeline_config,
    request_services,
    runtime_status,
    shared_pipeline_backend_available,
)
from zone_universe.search import search_pages
from zone_universe.database import connect as zone_connect, diagnostics as zone_diagnostics

@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(_claude_queue_worker())
    try:
        yield
    finally:
        task.cancel()
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


class PipelineFilters(BaseModel):
    selected_extensions: list[str]
    include_keywords: list[str] = []
    exclude_keywords: list[str] = []
    minimum_score: int = Field(55, ge=0, le=100)
    claude_daily_limit: int = Field(400, ge=1, le=5000)


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


def _with_claude(rows: list[dict]) -> list[dict]:
    decisions: dict[str, dict] = {}
    domains = [row.get("domain", "") for row in rows]
    for start in range(0, len(domains), 500):
        decisions.update(claude_decisions_for_domains(domains[start:start + 500]))
    return [
        {
            **row,
            "claude_decision": decisions.get(row.get("domain", ""), {}).get("decision", ""),
            "claude_score": decisions.get(row.get("domain", ""), {}).get("fit_score", ""),
            "claude_reasoning": decisions.get(row.get("domain", ""), {}).get("reasoning", ""),
            "claude_reviewed_at": decisions.get(row.get("domain", ""), {}).get("updated_at", ""),
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
    report = health_report()
    return {
        "overall": report["overall"],
        "counts": report["counts"],
        "runtime": report["runtime"],
        "buckets": _counts(),
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
    status = (
        read_pipeline_status()
        if shared_pipeline_backend_available()
        else read_local_pipeline_status()
    )
    if bool(status.get("running")):
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
        "rows": _with_claude([{**row, "hidden": row["domain"] in hidden} for row in rows]),
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
    page: int = Query(1, ge=1), page_size: int = Query(100, ge=25, le=500),
):
    rows, total = query_training_labels(
        label, search=q, limit=page_size, offset=(page - 1) * page_size
    )
    return {
        "rows": [{**row, "url": f"https://{row['domain']}"} for row in rows],
        "total": total, "page": page, "page_size": page_size,
    }


@app.post("/api/labels", dependencies=[Depends(_require_admin)])
def set_label(update: LabelUpdate):
    save_basket_entry(update.domain, update.label, update.rating, source="human", reason=update.reason)
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


@app.get("/api/certstream/status")
def certstream_status():
    if shared_pipeline_backend_available():
        return read_pipeline_status() or {
            "running": False,
            "stage": "waiting for worker status",
            "queue": {},
        }
    return read_local_pipeline_status()


@app.get("/api/claude/status")
def claude_status():
    with connect_store(read_only=True) as conn:
        rows = conn.execute(
            "SELECT decision,count(*) AS total,max(updated_at) AS latest "
            "FROM claude_diligence GROUP BY decision"
        ).fetchall()
    counts = {str(row["decision"]): int(row["total"]) for row in rows}
    latest = max((str(row["latest"] or "") for row in rows), default="")
    status = read_pipeline_status() if shared_pipeline_backend_available() else read_local_pipeline_status()
    return {
        "reviewed": counts.get("KEEP", 0) + counts.get("REJECT", 0),
        "kept": counts.get("KEEP", 0),
        "rejected": counts.get("REJECT", 0),
        "latest": latest,
        "calls_this_run": int(status.get("claude_calls", 0) or 0),
        "warning": str(status.get("claude_warning", "") or ""),
    }


@app.post("/api/claude/review/{domain}", dependencies=[Depends(_require_admin)])
def manual_claude_review(domain: str):
    target = stable_domain(domain)
    if not target:
        raise HTTPException(400, "Invalid domain")
    existing = claude_decisions_for_domains([target]).get(target)
    if existing and str(existing.get("decision", "")).upper() in {"KEEP", "REJECT"}:
        return {"status": "already_reviewed", **existing}
    if not classification_record_for_domain(target):
        raise HTTPException(404, "Company not found")
    queued = enqueue_claude_review(target)
    record_admin_audit("claude_queued", target)
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
def export_classifications(bucket: str):
    if bucket not in BUCKETS:
        raise HTTPException(400, "Unknown bucket")
    rows: list[dict] = []
    while True:
        batch = query_classifications(BUCKETS[bucket], limit=250, offset=len(rows), sort="Recently classified")
        rows.extend(batch)
        if len(batch) < 250:
            break
    return _csv_response(_with_claude(rows), f"glasswing-{bucket}.csv")


@app.get("/api/export/labels/{label}")
def export_labels(label: Literal["yes", "no"]):
    rows: list[dict] = []
    total = 1
    while len(rows) < total:
        batch, total = query_training_labels(label, limit=500, offset=len(rows))
        rows.extend(batch)
    return _csv_response(rows, f"glasswing-{label}-basket.csv")


def _execute_queued_claude(target: str) -> None:
    if not os.getenv("ANTHROPIC_API_KEY", "").strip():
        finish_claude_review(target, error="Anthropic API key is not configured")
        return
    pipeline = connect_pipeline_db()
    try:
        if claude_allowance(pipeline) <= 0:
            finish_claude_review(target, error="Claude daily/hourly budget is currently exhausted")
            return
    finally:
        pipeline.close()
    record = classification_record_for_domain(target)
    if not record:
        finish_claude_review(target, error="Company not found")
        return
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
            return
        attempted = report_path.exists()
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
        return
    result = claude_decisions_for_domains([target]).get(target)
    if not result:
        finish_claude_review(target, error="Claude completed without saving a decision")
        return
    finish_claude_review(target)
    record_admin_audit("claude_completed", target, f"{result.get('decision')} score {result.get('fit_score')}")


async def _claude_queue_worker() -> None:
    recover_claude_review_queue()
    while True:
        queued = await asyncio.to_thread(claim_claude_review)
        if queued:
            await asyncio.to_thread(_execute_queued_claude, str(queued["domain"]))
        else:
            await asyncio.sleep(3)


@app.get("/api/runtime")
def workers():
    return runtime_status()


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
    record_admin_audit("domains_uploaded", file.filename or "upload", f"received={len(domains)} added={added}")
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
