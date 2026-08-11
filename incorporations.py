"""Licensed Delaware incorporation intake; never scrapes Delaware eCorp."""
from __future__ import annotations

import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

from classification_store import connect_store
from common import QUEUE_DB
from form_d import _now, resolve_official_domains

API = "https://api.opencorporates.com/v0.4/companies/search"


def _schema(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS incorporation_signals(
      company_number TEXT PRIMARY KEY, company_name TEXT NOT NULL DEFAULT '',
      incorporation_date TEXT NOT NULL DEFAULT '', company_type TEXT NOT NULL DEFAULT '',
      registry_url TEXT NOT NULL DEFAULT '', score INTEGER NOT NULL DEFAULT 0,
      bucket TEXT NOT NULL DEFAULT 'review', reason TEXT NOT NULL DEFAULT '',
      domain TEXT NOT NULL DEFAULT '', domain_status TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS incorporation_state(
      key TEXT PRIMARY KEY,value TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL)""")
    conn.commit()


def _state(conn, key: str, value: Any) -> None:
    conn.execute("INSERT INTO incorporation_state(key,value,updated_at) VALUES (?,?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                 (key, json.dumps(value) if not isinstance(value, str) else value, _now()))
    conn.commit()


def score_company(row: dict[str, Any]) -> tuple[int, str, str]:
    name = str(row.get("company_name", "")).lower()
    kind = str(row.get("company_type", "")).lower()
    if any(term in name for term in (" fund", "spv", "holdings", " holding ", "realty", "property", "properties", "capital partners", "investments")):
        return 0, "denied", "fund_spv_or_holding_company"
    if any(term in kind for term in ("limited liability", "partnership", "trust", "nonprofit")):
        return 0, "denied", "non_target_entity_type"
    if "corporation" not in kind and "public benefit" not in kind and "corp" not in kind:
        return 0, "denied", "non_target_entity_type"
    return 70, "review", "new_delaware_corporation"


def _fetch(date: str, page: int) -> dict:
    token = os.getenv("OPENCORPORATES_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("OpenCorporates API token is not configured")
    query = urllib.parse.urlencode({"jurisdiction_code": "us_de", "incorporation_date": date,
                                    "exclude_inactive": "true", "exclude_branches": "true",
                                    "order": "incorporation_date", "per_page": 100, "page": page,
                                    "api_token": token})
    request = urllib.request.Request(f"{API}?{query}", headers={"User-Agent": "Glasswing Ventures sourcing@glasswing.vc"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def ingest_once() -> dict[str, int]:
    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=offset)).isoformat() for offset in range(0, 4)]
    received = added = queued = 0
    with connect_store() as conn:
        _schema(conn)
        for date in dates:
            page = 1
            while page <= 5:
                payload = _fetch(date, page)
                results = payload.get("results", {})
                companies = results.get("companies", [])
                received += len(companies)
                for wrapped in companies:
                    company = wrapped.get("company", {})
                    number = str(company.get("company_number", "")).strip()
                    if not number or conn.execute("SELECT 1 FROM incorporation_signals WHERE company_number=?", (number,)).fetchone():
                        continue
                    row = {"company_name": company.get("name", ""), "company_type": company.get("company_type", "")}
                    score, bucket, reason = score_company(row)
                    now = _now()
                    conn.execute("INSERT INTO incorporation_signals(company_number,company_name,incorporation_date,company_type,registry_url,score,bucket,reason,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                 (number, row["company_name"], company.get("incorporation_date", date), row["company_type"], company.get("opencorporates_url", ""), score, bucket, reason, now, now))
                    added += 1
                conn.commit()
                total_pages = int(results.get("total_pages", 1) or 1)
                if page >= total_pages or not companies:
                    break
                page += 1
        candidates = [dict(row) for row in conn.execute("SELECT company_number AS accession,company_name FROM incorporation_signals WHERE bucket='review' AND domain_status='' ORDER BY incorporation_date DESC LIMIT 10").fetchall()]
        if candidates:
            try:
                resolved = resolve_official_domains(candidates, context="new Delaware corporation")
                for candidate in candidates:
                    domain = resolved.get(candidate["accession"], "")
                    conn.execute("UPDATE incorporation_signals SET domain=?,domain_status=?,updated_at=? WHERE company_number=?",
                                 (domain, "queued" if domain else "unresolved", _now(), candidate["accession"]))
                conn.commit()
                if resolved:
                    from ai_search.og_bridge import enqueue_for_screening
                    queued = int(enqueue_for_screening(QUEUE_DB, list(resolved.values()), source_run_id="delaware_incorporation").get("queued", 0))
            except Exception:
                pass
        result = {"received": received, "added": added, "domains_queued": queued}
        _state(conn, "last_run", result)
        _state(conn, "status", "running")
    return result


def dashboard(bucket: str = "review", limit: int = 100) -> dict:
    with connect_store() as conn:
        _schema(conn)
        counts = {str(row["bucket"]): int(row["total"]) for row in conn.execute("SELECT bucket,count(*) total FROM incorporation_signals GROUP BY bucket")}
        where, params = ("WHERE bucket=?", [bucket]) if bucket in {"review", "denied"} else ("", [])
        params.append(limit)
        rows = [dict(row) for row in conn.execute(f"SELECT * FROM incorporation_signals {where} ORDER BY incorporation_date DESC,score DESC LIMIT ?", params).fetchall()]
        state = {str(row["key"]): str(row["value"]) for row in conn.execute("SELECT key,value FROM incorporation_state")}
        domains = int(conn.execute("SELECT count(*) total FROM incorporation_signals WHERE domain<>''").fetchone()["total"])
    return {"rows": rows, "counts": {"all": sum(counts.values()), "domains": domains, **counts}, "state": state, "configured": bool(os.getenv("OPENCORPORATES_API_TOKEN", "").strip())}


def desired() -> bool:
    with connect_store() as conn:
        _schema(conn)
        row = conn.execute("SELECT value FROM incorporation_state WHERE key='desired'").fetchone()
        return True if row is None else str(row["value"]).lower() == "true"


def set_desired(value: bool) -> None:
    with connect_store() as conn:
        _schema(conn)
        _state(conn, "desired", "true" if value else "false")
        _state(conn, "status", "starting" if value else "paused")
