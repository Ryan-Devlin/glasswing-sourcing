from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
AI_SRC = ROOT / "ai-domain-search-engine" / "src"
ZONE_SRC = ROOT / "zone-universe" / "src"
for source in (ROOT, AI_SRC, ZONE_SRC):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from ai_search.og_bridge import search_companies
from classification_store import search_classifications
from common import AI_SEARCH_DB, ZONE_UNIVERSE_DB
from health_monitor import health_report
from runtime_control import RUNTIME_MODE, SERVICES
from ui_theme import render_card_grid
from zone_universe.search import search_pages

st.title("Glasswing Sourcing")


@st.cache_data(show_spinner=False, ttl=15)
def cached_health():
    return health_report()


report = cached_health()
overall = str(report["overall"])
runtime = report["runtime"]
counts = report["counts"]

status_cols = st.columns(4)
{
    "Healthy": status_cols[0].success,
    "Warning": status_cols[0].warning,
    "Error": status_cols[0].error,
}[overall](f"System: **{overall}**")

if RUNTIME_MODE == "local":
    running_count = sum(
        1 for service in SERVICES if runtime["services"][service]["running"]
    )
    status_cols[1].metric("Workers running", f"{running_count}/{len(SERVICES)}")
else:
    # Local PID files don't apply once each worker is its own Railway
    # service -- there's no local process on this container to check, so
    # "running" is unknown here rather than false. See runtime_status().
    status_cols[1].metric("Workers", "Cloud-managed")
status_cols[2].metric("Companies tracked", f"{int(counts['classifications']):,}")
status_cols[3].metric(
    "Pages indexed",
    f"{int(counts['ai_indexed']) + int(counts['zone_indexed']):,}",
)

service_labels = {
    "pipeline": "New-domain screening",
    "ai_crawler": ".AI discovery crawler",
    "zone_crawler": ".COM/.APP zone crawler",
}
with st.expander("Worker detail"):
    for service in SERVICES:
        row = runtime["services"][service]
        if row["running"] is None:
            status = "Cloud-managed (Railway)"
        else:
            status = "Running" if row["running"] else "Stopped"
        st.write(
            f"**{service_labels[service]}** -- {status} "
            f"(desired: {'on' if row['desired'] else 'off'})"
        )
st.page_link("pages/system_health.py", label="Full system health", icon="🩺")

st.divider()

with st.form("overview_search"):
    query_input = st.text_input(
        "Search companies and crawled pages",
        value=st.session_state.get("overview_query", ""),
        placeholder="e.g. fintech, acme.ai, payments api",
    )
    submitted = st.form_submit_button("Search", type="primary")

if submitted:
    st.session_state["overview_query"] = query_input.strip()

active_query = st.session_state.get("overview_query", "")


def _clean_snippet(value: str, limit: int = 150) -> str:
    text = (value or "").replace("<mark>", "").replace("</mark>", "")
    return text if len(text) <= limit else text[:limit].rsplit(" ", 1)[0] + "…"


def _company_blurb(row: dict) -> str | None:
    """What the company actually does, not its own domain repeated back."""
    for field in ("description", "excerpt"):
        text = (row.get(field) or "").strip()
        if text:
            return _clean_snippet(text, limit=160)
    name = (row.get("company_name") or "").strip()
    if name and name.lower() != row["domain"].lower():
        return name
    return None


if active_query:
    company_rows = search_classifications(active_query, limit=5)
    st.subheader("Companies you're tracking")
    if company_rows:
        render_card_grid([
            {
                "title": row["domain"],
                "link": row.get("final_url") or None,
                "meta": _company_blurb(row),
                "tags": (
                    [row["thesis_pillar"].replace("_", " ").title()]
                    if row.get("thesis_pillar")
                    else []
                ),
                "activity": [
                    ("Score", str(row["fit_score"])),
                    ("Last updated", (row["updated_at"] or "")[:10]),
                ],
            }
            for row in company_rows
        ])
    else:
        st.caption("No investable matches in your classification list.")

    ai_rows = search_companies(AI_SEARCH_DB, active_query, limit=5)
    st.subheader(".AI pages")
    if ai_rows:
        render_card_grid([
            {
                "title": row["domain"],
                "link": row.get("final_url") or None,
                "meta": row["title"] or "(no title)",
                "activity": [
                    ("Match", _clean_snippet(row["snippet"])),
                    ("Crawled", (row["crawled_at"] or "")[:10]),
                ],
            }
            for row in ai_rows
        ])
        if st.button("Open full results in .AI Search"):
            st.session_state["ai_page_query"] = active_query
            st.session_state["ai_page_number"] = 1
            st.switch_page("pages/ai_search.py")
    else:
        st.caption("No matches in the .AI corpus.")

    zone_rows = search_pages(active_query, limit=5, database=ZONE_UNIVERSE_DB)
    st.subheader("Zone Universe pages (.com / .app)")
    if zone_rows:
        render_card_grid([
            {
                "title": row["domain"],
                "link": row.get("final_url") or None,
                "meta": f"{row['tld']} · {row['title'] or '(no title)'}",
                "activity": [
                    ("Match", _clean_snippet(row["snippet"])),
                    ("Crawled", (row["crawled_at"] or "")[:10]),
                ],
            }
            for row in zone_rows
        ])
        if st.button("Open full results in Zone Universe"):
            st.session_state["zone_query"] = active_query
            st.session_state["zone_page"] = 1
            st.switch_page("pages/zone_universe.py")
    else:
        st.caption("No matches in the .com/.app corpus.")
