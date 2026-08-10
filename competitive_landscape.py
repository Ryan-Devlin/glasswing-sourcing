"""Durable, template-driven competitive-landscape research jobs."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from copy import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from anthropic import Anthropic
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from classification_store import connect_store
from common import DATA, get_model


ROOT = Path(__file__).resolve().parent
JOB_ROOT = Path(os.getenv("GLASSWING_LANDSCAPE_DIR", str(DATA / "competitive_landscapes"))).expanduser().resolve()
MODEL = os.getenv("ANTHROPIC_DILIGENCE_MODEL", get_model())
SHIP_ANGEL_TEMPLATE = ROOT / "templates" / "Competitive Analysis - Ship Angel.xlsx"


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_job(website: str, template_name: str, materials: list[str], notes: str, count: int, job_id: str = "") -> str:
    job_id = job_id or uuid.uuid4().hex
    with connect_store() as conn:
        conn.execute(
            "INSERT INTO competitive_landscape_jobs(job_id,status,website,template_name,materials_json,notes,requested_count,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (job_id, "queued", website, template_name, json.dumps(materials), notes, count, now()),
        )
        conn.commit()
    return job_id


def list_jobs(limit: int = 30) -> list[dict]:
    with connect_store(read_only=True) as conn:
        rows = conn.execute(
            "SELECT job_id,status,website,template_name,requested_count,company_name,competitor_count,error,created_at,started_at,completed_at FROM competitive_landscape_jobs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def job(job_id: str) -> dict | None:
    with connect_store(read_only=True) as conn:
        row = conn.execute("SELECT * FROM competitive_landscape_jobs WHERE job_id=?", (job_id,)).fetchone()
    return dict(row) if row else None


def retry_job(job_id: str) -> bool:
    with connect_store() as conn:
        cursor = conn.execute(
            "UPDATE competitive_landscape_jobs SET status='queued',error='',completed_at='' WHERE job_id=? AND status='failed'",
            (job_id,),
        )
        conn.commit()
    return int(cursor.rowcount or 0) > 0


def claim_job() -> dict | None:
    with connect_store() as conn:
        row = conn.execute(
            "UPDATE competitive_landscape_jobs SET status='researching',started_at=?,error='' "
            "WHERE job_id=(SELECT job_id FROM competitive_landscape_jobs WHERE status='queued' ORDER BY created_at LIMIT 1) "
            "AND status='queued' RETURNING *",
            (now(),),
        ).fetchone()
        conn.commit()
    return dict(row) if row else None


def recover_jobs() -> int:
    stale_before = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    with connect_store() as conn:
        cursor = conn.execute(
            "UPDATE competitive_landscape_jobs SET status='queued',started_at='',error='Recovered after interrupted worker' WHERE status='researching' AND started_at < ?",
            (stale_before,),
        )
        conn.commit()
    return max(0, int(cursor.rowcount or 0))


def _site_text(url: str) -> str:
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 Glasswing Research"})
        with urlopen(req, timeout=20) as response:
            html = response.read(1_500_000).decode("utf-8", "ignore")
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        return re.sub(r"\s+", " ", soup.get_text(" ", strip=True))[:30000]
    except Exception as exc:
        return f"Website extraction unavailable ({type(exc).__name__}). Use web research."


def _material_text(path: Path) -> str:
    suffix = path.suffix.lower()
    try:
        if suffix in {".txt", ".md", ".csv"}:
            return path.read_text(encoding="utf-8", errors="ignore")[:30000]
        if suffix == ".pdf":
            from pypdf import PdfReader
            return "\n".join((page.extract_text() or "") for page in PdfReader(path).pages)[:30000]
        if suffix == ".docx":
            from docx import Document
            return "\n".join(p.text for p in Document(path).paragraphs)[:30000]
        if suffix == ".xlsx":
            book = load_workbook(path, read_only=True, data_only=True)
            lines = []
            for sheet in book.worksheets:
                lines.append(f"Sheet: {sheet.title}")
                for row in sheet.iter_rows(values_only=True):
                    lines.append(" | ".join("" if value is None else str(value) for value in row))
                    if sum(map(len, lines)) > 30000:
                        break
            return "\n".join(lines)[:30000]
    except Exception as exc:
        return f"Could not extract {path.name}: {type(exc).__name__}"
    return f"Attached file: {path.name} (format not text-extractable)"


def _json_object(text: str) -> dict:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I)
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("Research response did not contain JSON")
    return json.loads(text[start:end + 1])


def _research(record: dict, folder: Path) -> dict:
    if not os.getenv("ANTHROPIC_API_KEY", "").strip():
        raise RuntimeError("Anthropic API key is not configured")
    website = str(record["website"])
    context = _site_text(website)
    materials = json.loads(record.get("materials_json") or "[]")
    material_context = "\n\n".join(_material_text(folder / name) for name in materials)
    count = int(record.get("requested_count") or 25)
    prompt = f"""You are preparing a venture-capital competitive landscape for the target company at {website}.
Find the {count} most relevant direct and adjacent competitors. Research current, defensible facts using web search. Do not invent missing financing, pricing, customers, or capabilities; use null when not verified. Exclude the target itself and duplicate brands. Favor comprehensive global coverage and include source URLs for every company.

Target website text:
{context}

User notes:
{record.get('notes') or 'None'}

Supporting material:
{material_context or 'None'}

Return ONLY valid JSON with keys target_company, methodology, and competitors. competitors must be an array of objects with exactly these keys:
name, website, level_of_competition (H/M/L), summary (max 35 words), focus, hq, total_funding_m, last_round_size_m, date_raised, valuation_m, notable_investors, pricing, generating_revenue, partners_brands, real_time_visibility, ai_ml_utilization, predictive_analytics, quote_rate_management, digital_dashboard_order_management, automated_broker_experience, air_freight_quoting, ocean_freight_quoting, ground_freight_quoting, contract_management, spot_bidding_marketplace, freight_audit, sources.
For product capabilities use Yes, No, Partial, or Unknown. Dates use YYYY-MM-DD where known. Monetary fields are numeric USD millions or null."""
    client = Anthropic()
    response = client.messages.create(
        model=MODEL,
        max_tokens=16000,
        temperature=0,
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 20}],
        messages=[{"role": "user", "content": prompt}],
    )
    text = "\n".join(block.text for block in response.content if getattr(block, "type", "") == "text")
    return _json_object(text)


KEYS = ["name", "level_of_competition", "summary", "focus", "hq", "total_funding_m", "last_round_size_m", "date_raised", "valuation_m", "notable_investors", "pricing", "generating_revenue", "partners_brands", "real_time_visibility", "ai_ml_utilization", "predictive_analytics", "quote_rate_management", "digital_dashboard_order_management", "automated_broker_experience", "air_freight_quoting", "ocean_freight_quoting", "ground_freight_quoting", "contract_management", "spot_bidding_marketplace", "freight_audit"]

HEADERS = ["", "Level of Competition (H/M/L)", "Summary", "Focus", "HQ", "Total Funding ($M)", "Last Round Size ($M)", "Date Raised", "Valuation ($M)", "Notable Investors", "Pricing\n(* + italic indicates pulled from third-party source)", "Generating Revenue?", "Partners / Brands", "Real-Time Visibility", "AI/ML Utilization", "Predictive Analytics", "Quote/Rate Management", "Digital Dashboard/\nOrder Management", "Automated Broker Experience", "Air Freight Quoting", "Ocean Freight Quoting", "Ground Freight Quoting", "Contract Management", "Spot Bidding / Marketplace", "Freight Audit"]


def create_default_template(path: Path) -> None:
    """Create the reusable Glasswing version of the supplied Ship Angel layout."""
    book = Workbook()
    sheet = book.active
    sheet.title = "Competitive Landscape"
    groups = {1: "Competitors", 2: "General Information", 6: "Financing", 11: "Business Model & Traction", 14: "Product / Service Offerings"}
    widths = [25.16, 16.5, 58.66, 14.83, 14.83, 14.83, 14.83, 14.83, 14.83, 26.33, 23, 14.83, 23.33] + [14.83] * 12
    group_fill = PatternFill("solid", fgColor="123B38")
    header_fill = PatternFill("solid", fgColor="DCEDE7")
    thin = Side(style="thin", color="C7D8D2")
    for col in range(1, 26):
        sheet.column_dimensions[get_column_letter(col)].width = widths[col - 1]
        group = sheet.cell(1, col, groups.get(col, ""))
        group.fill = group_fill
        group.font = Font(color="FFFFFF", bold=True)
        group.alignment = Alignment(vertical="center")
        header = sheet.cell(2, col, HEADERS[col - 1])
        header.fill = header_fill
        header.font = Font(color="123B38", bold=True)
        header.alignment = Alignment(wrap_text=True, vertical="center")
        header.border = Border(bottom=thin)
        sample = sheet.cell(3, col)
        sample.alignment = Alignment(wrap_text=True, vertical="top")
        sample.border = Border(bottom=thin)
    sheet.row_dimensions[1].height = 17
    sheet.row_dimensions[2].height = 50
    sheet.row_dimensions[3].height = 55
    sheet.freeze_panes = "B3"
    sheet.auto_filter.ref = "A2:Y3"
    book.save(path)


def _excel_value(value, key: str = ""):
    """Collapse occasionally nested model JSON into a valid Excel scalar."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return ", ".join(
            str(item) for item in (_excel_value(item, key) for item in value)
            if item not in (None, "")
        )
    if isinstance(value, dict):
        preferred = {
            "name": ("name", "company_name", "title"),
            "website": ("website", "url", "domain"),
            "summary": ("summary", "description", "value"),
        }.get(key, (key, "value", "description", "summary", "name"))
        for candidate in preferred:
            if candidate and candidate in value and value[candidate] not in (None, ""):
                return _excel_value(value[candidate], candidate)
        return "; ".join(
            f"{label}: {_excel_value(child, label)}"
            for label, child in value.items() if child not in (None, "", [], {})
        )
    return str(value)


def _write_workbook(template: Path, output: Path, result: dict) -> int:
    shutil.copyfile(template, output)
    book = load_workbook(output)
    sheet = book["Competitive Landscape"] if "Competitive Landscape" in book.sheetnames else book.active
    style_row = 3
    for row in range(3, max(sheet.max_row, 3) + 1):
        for col in range(1, 26):
            sheet.cell(row, col).value = None
        sheet.row_dimensions[row].hidden = False
    competitors = result.get("competitors") or []
    for index, company in enumerate(competitors, start=3):
        if not isinstance(company, dict):
            company = {"name": _excel_value(company)}
        nested = company.get("name")
        if isinstance(nested, dict):
            company = {**nested, **{key: value for key, value in company.items() if key != "name"}}
        if index != style_row:
            for col in range(1, 26):
                source, target = sheet.cell(style_row, col), sheet.cell(index, col)
                target._style = copy(source._style)
                target.number_format = source.number_format
                target.alignment = copy(source.alignment)
                target.border = copy(source.border)
                target.fill = copy(source.fill)
                target.font = copy(source.font)
        sheet.row_dimensions[index].height = 54.75
        for col, key in enumerate(KEYS, start=1):
            sheet.cell(index, col).value = _excel_value(company.get(key), key)
        website = _excel_value(company.get("website"), "website")
        if website:
            sheet.cell(index, 1).hyperlink = str(website)
    last = max(3, len(competitors) + 2)
    sheet.auto_filter.ref = f"A2:Y{last}"
    sheet.freeze_panes = "B3"
    metadata = book.create_sheet("Research Notes")
    metadata.append([
        "Target company",
        _excel_value(result.get("target_company", ""), "name"),
    ])
    metadata.append(["Generated", now()])
    metadata.append([
        "Methodology",
        _excel_value(result.get("methodology", ""), "methodology"),
    ])
    metadata.append([])
    metadata.append(["Competitor", "Source URLs"])
    for company in competitors:
        if not isinstance(company, dict):
            company = {"name": company}
        sources = company.get("sources") or []
        metadata.append([
            _excel_value(company.get("name", ""), "name"),
            _excel_value(sources, "sources"),
        ])
    metadata.column_dimensions["A"].width = 30
    metadata.column_dimensions["B"].width = 100
    book.save(output)
    return len(competitors)


def execute_job(record: dict) -> None:
    job_id = str(record["job_id"])
    folder = JOB_ROOT / job_id
    try:
        stored = str(record.get("result_json") or "").strip()
        result = json.loads(stored) if stored and stored != "{}" else _research(record, folder)
        target_name = _excel_value(result.get("target_company", ""), "name") or ""
        with connect_store() as conn:
            conn.execute(
                "UPDATE competitive_landscape_jobs SET status='exporting',company_name=?,result_json=? WHERE job_id=?",
                (str(target_name), json.dumps(result), job_id),
            )
            conn.commit()
        output = folder / "competitive-landscape.xlsx"
        count = _write_workbook(folder / str(record["template_name"]), output, result)
        with connect_store() as conn:
            conn.execute("UPDATE competitive_landscape_jobs SET status='completed',company_name=?,competitor_count=?,output_path=?,result_json=?,completed_at=? WHERE job_id=?", (str(target_name), count, str(output), json.dumps(result), now(), job_id))
            conn.commit()
    except Exception as exc:
        with connect_store() as conn:
            conn.execute("UPDATE competitive_landscape_jobs SET status='failed',error=?,completed_at=? WHERE job_id=?", (f"{type(exc).__name__}: {str(exc)[:1000]}", now(), job_id))
            conn.commit()
