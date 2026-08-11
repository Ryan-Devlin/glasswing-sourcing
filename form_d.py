"""SEC EDGAR Form D intake and deterministic venture screening."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from classification_store import connect_store
from common import QUEUE_DB, get_model, stable_domain

FEED_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=D&company=&dateb=&owner=include&count=100&output=atom"
USER_AGENT = os.getenv(
    "SEC_EDGAR_USER_AGENT",
    "Glasswing Ventures sourcing@glasswing.vc",
).strip()
MAX_TARGET_RAISE = int(os.getenv("FORM_D_MAX_TARGET_RAISE", "15000000"))
_SEC_LOCK = threading.Lock()
_LAST_REQUEST = 0.0
_DOMAIN_RESOLUTION_LIMIT = 10


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS form_d_filings(
            accession TEXT PRIMARY KEY, cik TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '', filing_date TEXT NOT NULL DEFAULT '',
            filing_url TEXT NOT NULL DEFAULT '', industry TEXT NOT NULL DEFAULT '',
            entity_type TEXT NOT NULL DEFAULT '', issuer_year TEXT NOT NULL DEFAULT '',
            offering_amount INTEGER NOT NULL DEFAULT 0, amount_sold INTEGER NOT NULL DEFAULT 0,
            investors INTEGER NOT NULL DEFAULT 0, first_sale_date TEXT NOT NULL DEFAULT '',
            exemption TEXT NOT NULL DEFAULT '', score INTEGER NOT NULL DEFAULT 0,
            bucket TEXT NOT NULL DEFAULT 'review', reason TEXT NOT NULL DEFAULT '',
            domain TEXT NOT NULL DEFAULT '', domain_status TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    try:
        conn.execute("SELECT domain,domain_status FROM form_d_filings LIMIT 0")
    except Exception:
        conn.rollback()
        conn.execute("ALTER TABLE form_d_filings ADD COLUMN domain TEXT NOT NULL DEFAULT ''")
        conn.execute("ALTER TABLE form_d_filings ADD COLUMN domain_status TEXT NOT NULL DEFAULT ''")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS form_d_state(
            key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def _state(conn, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO form_d_state(key,value,updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
        (key, json.dumps(value) if not isinstance(value, str) else value, _now()),
    )
    conn.commit()


def _get(url: str) -> bytes:
    global _LAST_REQUEST
    with _SEC_LOCK:
        delay = 0.12 - (time.monotonic() - _LAST_REQUEST)
        if delay > 0:
            time.sleep(delay)
        request = urllib.request.Request(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            content = response.read()
        _LAST_REQUEST = time.monotonic()
        return content


def _text(root: ET.Element, name: str) -> str:
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] == name and element.text:
            return " ".join(element.text.split())
    return ""


def _integer(value: str) -> int:
    try:
        return int(float(re.sub(r"[^0-9.]", "", value or "") or 0))
    except ValueError:
        return 0


def score_filing(row: dict[str, Any]) -> tuple[int, str, str]:
    """Return a decisive bucket while treating missing facts as uncertainty."""
    industry = str(row.get("industry", "")).lower()
    entity = str(row.get("entity_type", "")).lower()
    name = str(row.get("company_name", "")).lower()
    if not name.strip():
        return 0, "denied", "missing_issuer_identity"
    pooled = str(row.get("pooled_fund", "")).lower() in {"true", "1", "yes"}
    fund_named = bool(re.search(r"\bfunds?\b", name))
    if pooled or "investment fund" in industry or fund_named or any(
        term in f"{entity} {name}" for term in (
            "hedge fund", "private equity fund", "venture fund", "fund lp",
            "fund i ", "fund ii ", "fund iii ", "master fund", "feeder fund",
        )
    ):
        return 0, "denied", "pooled_or_investment_fund"
    if any(term in industry for term in (
        "real estate", "residential", "commercial", "investing",
    )):
        return 0, "denied", "real_estate_or_investment_vehicle"
    amount = int(row.get("offering_amount", 0) or 0)
    sold = int(row.get("amount_sold", 0) or 0)
    if amount > MAX_TARGET_RAISE or sold > MAX_TARGET_RAISE:
        return 0, "denied", "raise_above_target_stage"
    # A genuine new issuer filing is reviewable by default. Form D omits the
    # product/website evidence needed for a factual hard denial.
    score = 42
    reasons = ["new_form_d_issuer"]
    frontier = (
        "technology", "computers", "biotechnology", "health care",
        "pharmaceuticals", "energy", "other technology",
    )
    if any(term in industry for term in frontier):
        score += 28
        reasons.append("venture_relevant_industry")
    if 250_000 <= amount <= 10_000_000:
        score += 18
        reasons.append("venture_scale_raise")
    elif 10_000_000 < amount <= MAX_TARGET_RAISE:
        reasons.append("upper_end_of_target_raise")
    if sold > 0:
        score += 8
        reasons.append("capital_already_sold")
    if int(row.get("investors", 0) or 0) <= 50:
        score += 5
    score = max(0, min(100, score))
    if score >= 72:
        return score, "investable", "+".join(reasons)
    if score >= 42:
        return score, "review", "+".join(reasons)
    return score, "denied", "+".join(reasons)


def _filing_xml(index_url: str) -> tuple[str, bytes]:
    base = index_url.rsplit("/", 1)[0]
    listing = json.loads(_get(f"{base}/index.json"))
    items = listing.get("directory", {}).get("item", [])
    names = [str(item.get("name", "")) for item in items]
    selected = next(
        (name for name in names if name.lower().endswith(".xml") and "primary" in name.lower()),
        next((name for name in names if name.lower().endswith(".xml")), ""),
    )
    if not selected:
        raise ValueError("Form D XML document not found")
    url = f"{base}/{selected}"
    time.sleep(0.11)
    return url, _get(url)


def _json_object(text: str) -> dict[str, Any]:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I)
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("Domain resolution did not return JSON")
    return json.loads(text[start:end + 1])


def resolve_official_domains(rows: list[dict[str, Any]], context: str = "SEC Form D issuer") -> dict[str, str]:
    """Resolve only verified official sites; an uncertain result stays blank."""
    if not rows or not os.getenv("ANTHROPIC_API_KEY", "").strip():
        return {}
    from anthropic import Anthropic

    issuers = [
        {"accession": str(row["accession"]), "legal_name": str(row["company_name"])}
        for row in rows[:_DOMAIN_RESOLUTION_LIMIT]
    ]
    prompt = f"""Find the official company website for each {context} below.
Use web search and verify that the website itself identifies the same operating company. Do not use SEC, LinkedIn, Crunchbase, PitchBook, news, directories, social profiles, investor pages, or guessed domains. If the exact official website cannot be verified, return an empty domain. Return only JSON shaped as {{"matches":[{{"accession":"...","domain":"example.com","verified":true}}]}}.

Issuers:
{json.dumps(issuers)}"""
    response = Anthropic().messages.create(
        model=get_model(),
        max_tokens=2500,
        temperature=0,
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 12}],
        messages=[{"role": "user", "content": prompt}],
    )
    content = "\n".join(
        block.text for block in response.content
        if getattr(block, "type", "") == "text"
    )
    matches = _json_object(content).get("matches", [])
    blocked = {"sec.gov", "linkedin.com", "crunchbase.com", "pitchbook.com", "facebook.com", "x.com"}
    valid_accessions = {item["accession"] for item in issuers}
    resolved: dict[str, str] = {}
    for match in matches if isinstance(matches, list) else []:
        accession = str(match.get("accession", ""))
        domain = stable_domain(match.get("domain", ""))
        if (
            accession in valid_accessions
            and match.get("verified") is True
            and "." in domain
            and domain not in blocked
            and not any(domain.endswith(f".{host}") for host in blocked)
        ):
            resolved[accession] = domain
    return resolved


def _resolve_and_queue_domains(conn) -> int:
    candidates = [dict(row) for row in conn.execute(
        "SELECT accession,company_name FROM form_d_filings "
        "WHERE bucket IN ('investable','review') AND domain='' AND domain_status='' "
        "ORDER BY filing_date DESC,score DESC LIMIT ?",
        (_DOMAIN_RESOLUTION_LIMIT,),
    ).fetchall()]
    if not candidates:
        return 0
    try:
        resolved = resolve_official_domains(candidates)
    except Exception:
        return 0
    if not resolved:
        for candidate in candidates:
            conn.execute(
                "UPDATE form_d_filings SET domain_status='unresolved',updated_at=? WHERE accession=?",
                (_now(), candidate["accession"]),
            )
        conn.commit()
        return 0
    resolved_accessions = set(resolved)
    for candidate in candidates:
        if candidate["accession"] not in resolved_accessions:
            conn.execute(
                "UPDATE form_d_filings SET domain_status='unresolved',updated_at=? WHERE accession=?",
                (_now(), candidate["accession"]),
            )
    for accession, domain in resolved.items():
        conn.execute(
            "UPDATE form_d_filings SET domain=?,domain_status='queued',updated_at=? WHERE accession=?",
            (domain, _now(), accession),
        )
    conn.commit()
    try:
        from ai_search.og_bridge import enqueue_for_screening
        result = enqueue_for_screening(
            QUEUE_DB,
            list(resolved.values()),
            source_run_id="form_d",
        )
        return int(result.get("queued", 0) or 0)
    except Exception:
        return 0


def ingest_once() -> dict[str, int]:
    if "@" not in USER_AGENT:
        raise RuntimeError("SEC_EDGAR_USER_AGENT must include a contact email")
    root = ET.fromstring(_get(FEED_URL))
    entries = [node for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "entry"]
    added = screened = failed = 0
    with connect_store() as conn:
        _schema(conn)
        for entry in entries:
            title = _text(entry, "title")
            if " D" not in f" {title.upper()}" or "D/A" in title.upper():
                continue
            link = next((node.attrib.get("href", "") for node in entry if node.tag.rsplit("}", 1)[-1] == "link"), "")
            accession_match = re.search(r"(\d{10}-\d{2}-\d{6})", link)
            if not link or not accession_match:
                continue
            accession = accession_match.group(1)
            if conn.execute("SELECT 1 FROM form_d_filings WHERE accession=?", (accession,)).fetchone():
                continue
            try:
                xml_url, content = _filing_xml(link)
                form = ET.fromstring(content)
                row = {
                    "accession": accession,
                    "cik": _text(form, "cik"),
                    "company_name": _text(form, "entityName"),
                    "filing_date": _text(entry, "updated")[:10],
                    "filing_url": link,
                    "industry": _text(form, "industryGroupType"),
                    "entity_type": _text(form, "entityType"),
                    "pooled_fund": _text(form, "isPooledInvestmentFund"),
                    "issuer_year": _text(form, "yearOfInc"),
                    "offering_amount": _integer(_text(form, "totalOfferingAmount")),
                    "amount_sold": _integer(_text(form, "totalAmountSold")),
                    "investors": _integer(_text(form, "totalNumberAlreadyInvested")),
                    "first_sale_date": _text(form, "dateOfFirstSale"),
                    "exemption": ", ".join(
                        node.text.strip() for node in form.iter()
                        if node.tag.rsplit("}", 1)[-1] == "item" and node.text
                    )[:500],
                    "xml_url": xml_url,
                }
                score, bucket, reason = score_filing(row)
                now = _now()
                conn.execute(
                    "INSERT INTO form_d_filings(accession,cik,company_name,filing_date,filing_url,industry,entity_type,issuer_year,offering_amount,amount_sold,investors,first_sale_date,exemption,score,bucket,reason,raw_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (accession, row["cik"], row["company_name"], row["filing_date"], link, row["industry"], row["entity_type"], row["issuer_year"], row["offering_amount"], row["amount_sold"], row["investors"], row["first_sale_date"], row["exemption"], score, bucket, reason, json.dumps(row), now, now),
                )
                conn.commit()
                added += 1
                screened += 1
            except Exception:
                failed += 1
            time.sleep(0.11)
        # Rules improve as production edge cases surface. Re-score the small,
        # structured corpus on every pass so old decisions never go stale.
        for existing in conn.execute(
            "SELECT accession,raw_json FROM form_d_filings"
        ).fetchall():
            try:
                facts = json.loads(str(existing["raw_json"] or "{}"))
                score, bucket, reason = score_filing(facts)
                conn.execute(
                    "UPDATE form_d_filings SET score=?,bucket=?,reason=?,updated_at=? WHERE accession=?",
                    (score, bucket, reason, _now(), existing["accession"]),
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        conn.commit()
        queued = _resolve_and_queue_domains(conn)
        result = {"received": len(entries), "added": added, "screened": screened, "failed": failed}
        result["domains_queued"] = queued
        _state(conn, "last_run", result)
        _state(conn, "status", "running")
        return result


def dashboard(bucket: str = "all", limit: int = 100) -> dict[str, Any]:
    with connect_store() as conn:
        _schema(conn)
        counts = {str(row["bucket"]): int(row["total"]) for row in conn.execute("SELECT bucket,count(*) AS total FROM form_d_filings GROUP BY bucket")}
        params: list[Any] = []
        where = ""
        if bucket in {"investable", "review", "denied"}:
            where = "WHERE bucket=?"
            params.append(bucket)
        params.append(limit)
        rows = [dict(row) for row in conn.execute(f"SELECT accession,company_name,filing_date,filing_url,industry,entity_type,offering_amount,amount_sold,investors,score,bucket,reason,domain,domain_status FROM form_d_filings {where} ORDER BY filing_date DESC,score DESC LIMIT ?", params).fetchall()]
        domain_count = int(conn.execute("SELECT count(*) AS total FROM form_d_filings WHERE domain<>''").fetchone()["total"])
        state = {str(row["key"]): str(row["value"]) for row in conn.execute("SELECT key,value FROM form_d_state")}
    return {"rows": rows, "counts": {"all": sum(counts.values()), "domains": domain_count, **counts}, "state": state}


def desired() -> bool:
    with connect_store() as conn:
        _schema(conn)
        row = conn.execute("SELECT value FROM form_d_state WHERE key='desired'").fetchone()
        return True if row is None else str(row["value"]).lower() == "true"


def set_desired(value: bool) -> None:
    with connect_store() as conn:
        _schema(conn)
        _state(conn, "desired", "true" if value else "false")
        _state(conn, "status", "starting" if value else "paused")
