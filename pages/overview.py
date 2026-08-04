from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
AI_SRC = ROOT / "ai-domain-search-engine" / "src"
ZONE_SRC = ROOT / "zone-universe" / "src"
for source in (ROOT, AI_SRC, ZONE_SRC):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from ai_search.og_bridge import search_companies
from classification_store import (
    classification_bucket_counts,
    classification_source_label,
    query_classifications,
    query_classifications_by_domains,
    search_classifications,
)
from common import (
    AI_SEARCH_DB,
    NO_BASKET_DATA,
    YES_BASKET_DATA,
    ZONE_UNIVERSE_DB,
    read_basket_data,
    stable_domain,
)
from health_monitor import health_report
from runtime_control import SERVICES
from ui_theme import render_hideable_table
from zone_universe.search import search_pages

st.title("Glasswing Sourcing")


@st.cache_data(show_spinner=False, ttl=15)
def cached_health():
    return health_report()


report = cached_health()
overall = str(report["overall"])
runtime = report["runtime"]
counts = report["counts"]

status_cols = st.columns(2)
{
    "Healthy": status_cols[0].success,
    "Warning": status_cols[0].warning,
    "Error": status_cols[0].error,
}[overall](f"System: **{overall}**")
status_cols[1].metric("Companies tracked", f"{int(counts['classifications']):,}")

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

_LINK_COLUMN = st.column_config.LinkColumn(
    "Domain",
    display_text=r"https?://(?:www\.)?([^/]+).*",
)

st.subheader("Master company review")
st.caption(
    "Every classified company across CertStream, the .AI crawler, and the "
    ".COM/.APP crawler. Hide only changes what you see; Mark Yes and Mark No "
    "save training labels without moving or deleting the company."
)

MASTER_BUCKETS = [
    ("Investable", ("priority",)),
    ("Coming Soon", ("coming_soon",)),
    ("Needs Review", ("review",)),
    ("Potential Comp", ("comp",)),
    ("Insufficient Evidence", ("insufficient", "ghost", "screened_out")),
    ("Hard Denied", ("reject",)),
]


@st.cache_data(show_spinner=False, ttl=15)
def cached_master_bucket_counts() -> dict[str, int]:
    return classification_bucket_counts()


@st.cache_data(show_spinner=False, ttl=15, max_entries=48)
def cached_master_rows(
    buckets: tuple[str, ...],
    limit: int,
    offset: int,
    sort: str,
) -> list[dict]:
    return query_classifications(
        buckets,
        limit=limit,
        offset=offset,
        sort=sort,
    )


@st.cache_data(show_spinner=False, ttl=15, max_entries=48)
def cached_domain_rows(domains: tuple[str, ...]) -> list[dict]:
    return query_classifications_by_domains(domains)


master_counts = cached_master_bucket_counts()
bucket_totals = {
    label: sum(int(master_counts.get(bucket, 0)) for bucket in buckets)
    for label, buckets in MASTER_BUCKETS
}
yes_basket = read_basket_data(YES_BASKET_DATA)
no_basket = read_basket_data(NO_BASKET_DATA)
view_totals = {
    **bucket_totals,
    "Yes": len(yes_basket),
    "No": len(no_basket),
}
master_metric_columns = st.columns(len(view_totals))
for column, (label, total) in zip(master_metric_columns, view_totals.items()):
    column.metric(label, f"{total:,}")

selected_bucket_name = st.segmented_control(
    "Company table",
    list(view_totals),
    default="Investable",
    label_visibility="collapsed",
    width="stretch",
    key="overview_master_view",
)
selected_bucket_name = selected_bucket_name or "Investable"
master_sort_col, master_rows_col, master_status_col = st.columns([2.5, 1, 2])
if selected_bucket_name in {"Yes", "No"}:
    master_sort = master_sort_col.selectbox(
        "Sort companies",
        ["Rating: high to low", "Rating: low to high", "Domain: A to Z"],
        key=f"overview_master_sort_{selected_bucket_name}",
    )
else:
    master_sort = master_sort_col.selectbox(
        "Sort companies",
        [
            "Priority: high to low",
            "Fit: high to low",
            "Recently classified",
            "Company: A to Z",
            "Domain: A to Z",
        ],
        key=f"overview_master_sort_{selected_bucket_name}",
    )
master_page_size = int(master_rows_col.selectbox(
    "Rows",
    [25, 50, 100, 200],
    index=2,
    key=f"overview_master_rows_{selected_bucket_name}",
))
master_total = int(view_totals[selected_bucket_name])
master_page_key = f"overview_master_page_{selected_bucket_name}"
master_max_page = max(1, (master_total + master_page_size - 1) // master_page_size)
master_page = min(
    int(st.session_state.get(master_page_key, 1)),
    master_max_page,
)
master_status_col.caption(
    f"{master_total:,} companies · page {master_page:,} of {master_max_page:,}"
)
master_ratings: dict[str, int] = {}
if selected_bucket_name in {"Yes", "No"}:
    selected_basket = yes_basket if selected_bucket_name == "Yes" else no_basket
    selected_basket = selected_basket.copy()
    selected_basket["_domain"] = selected_basket["domain_url"].map(stable_domain)
    selected_basket = selected_basket[selected_basket["_domain"] != ""]
    if master_sort == "Rating: low to high":
        selected_basket = selected_basket.sort_values(
            ["rating", "_domain"], ascending=[True, True]
        )
    elif master_sort == "Domain: A to Z":
        selected_basket = selected_basket.sort_values("_domain")
    else:
        selected_basket = selected_basket.sort_values(
            ["rating", "_domain"], ascending=[False, True]
        )
    basket_page = selected_basket.iloc[
        (master_page - 1) * master_page_size:master_page * master_page_size
    ]
    page_domains = tuple(basket_page["_domain"].tolist())
    saved_rows = {
        stable_domain(row["domain"]): row
        for row in cached_domain_rows(page_domains)
    }
    master_rows = [
        {
            **saved_rows.get(domain, {}),
            "domain": domain,
            "final_url": saved_rows.get(domain, {}).get("final_url")
            or f"https://{domain}",
        }
        for domain in page_domains
    ]
    master_ratings = {
        row["_domain"]: int(row["rating"])
        for row in basket_page.to_dict(orient="records")
    }
else:
    selected_buckets = dict(MASTER_BUCKETS)[selected_bucket_name]
    master_rows = cached_master_rows(
        tuple(selected_buckets),
        master_page_size,
        (master_page - 1) * master_page_size,
        master_sort,
    )
if master_rows:
    master_frame = pd.DataFrame({
        "_domain": [row["domain"] for row in master_rows],
        "Company": [row.get("company_name", "") for row in master_rows],
        "Domain": [
            row.get("final_url") or f"https://{row['domain']}"
            for row in master_rows
        ],
        "Source": [
            classification_source_label(row.get("content_source", ""))
            for row in master_rows
        ],
        "Category": [
            str(row.get("thesis_pillar", "")).replace("_", " ").title()
            for row in master_rows
        ],
        "Score": [int(row.get("fit_score", 0) or 0) for row in master_rows],
        "Description": [row.get("description", "") for row in master_rows],
        "Reason": [
            str(row.get("routing_reason", "")).replace("_", " ").title()
            for row in master_rows
        ],
        "Updated": [str(row.get("updated_at", ""))[:10] for row in master_rows],
    })
    visible_master_columns = [
        "Company", "Domain", "Source", "Category", "Score",
        "Description", "Reason", "Updated",
    ]
    if selected_bucket_name in {"Yes", "No"}:
        master_frame.insert(
            3,
            "Training rating",
            [master_ratings.get(domain, 0) for domain in master_frame["_domain"]],
        )
        visible_master_columns.insert(3, "Training rating")
    render_hideable_table(
        master_frame,
        domain_column="_domain",
        visible_columns=visible_master_columns,
        key=f"overview_master_table_{selected_bucket_name}_{master_page}",
        height=min(760, 45 + len(master_frame) * 35),
        column_config={
            "Domain": _LINK_COLUMN,
            "Score": st.column_config.ProgressColumn(
                "Score",
                min_value=0,
                max_value=100,
                format="%d",
            ),
        },
    )
else:
    st.info(f"No companies currently appear in {selected_bucket_name}.")

master_previous, master_next, _ = st.columns([1, 1, 6])
if master_previous.button(
    "← Previous",
    disabled=master_page <= 1,
    key=f"overview_master_previous_{selected_bucket_name}",
):
    st.session_state[master_page_key] = master_page - 1
    st.rerun()
if master_next.button(
    "Next →",
    disabled=master_page >= master_max_page,
    key=f"overview_master_next_{selected_bucket_name}",
):
    st.session_state[master_page_key] = master_page + 1
    st.rerun()

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


def _results_table(rows: list[dict], columns: list[str], *, key: str) -> None:
    frame = pd.DataFrame(rows, columns=["_domain", *columns])
    render_hideable_table(
        frame,
        domain_column="_domain",
        visible_columns=columns,
        key=key,
        column_config={"Domain": _LINK_COLUMN},
    )


if active_query:
    company_rows = search_classifications(active_query, limit=25)
    st.subheader("Companies you're tracking")
    if company_rows:
        _results_table(
            [
                {
                    "_domain": row["domain"],
                    "Domain": row.get("final_url") or f"https://{row['domain']}",
                    "Source": classification_source_label(
                        row.get("content_source", "")
                    ),
                    "Description": _company_blurb(row) or "",
                    "Thesis pillar": (
                        row["thesis_pillar"].replace("_", " ").title()
                        if row.get("thesis_pillar")
                        else ""
                    ),
                    "Score": row["fit_score"],
                    "Last updated": (row["updated_at"] or "")[:10],
                }
                for row in company_rows
            ],
            columns=[
                "Domain",
                "Source",
                "Description",
                "Thesis pillar",
                "Score",
                "Last updated",
            ],
            key="overview_companies_table",
        )
    else:
        st.caption("No investable matches in your classification list.")

    ai_rows = search_companies(AI_SEARCH_DB, active_query, limit=25)
    st.subheader(".AI pages")
    if ai_rows:
        _results_table(
            [
                {
                    "_domain": row["domain"],
                    "Domain": row.get("final_url") or f"https://{row['domain']}",
                    "Title": row["title"] or "(no title)",
                    "Match": _clean_snippet(row["snippet"]),
                    "Crawled": (row["crawled_at"] or "")[:10],
                }
                for row in ai_rows
            ],
            columns=["Domain", "Title", "Match", "Crawled"],
            key="overview_ai_table",
        )
        if st.button("Open full results in .AI Search"):
            st.session_state["ai_page_query"] = active_query
            st.session_state["ai_page_number"] = 1
            st.switch_page("pages/ai_search.py")
    else:
        st.caption("No matches in the .AI corpus.")

    zone_rows = search_pages(active_query, limit=25, database=ZONE_UNIVERSE_DB)
    st.subheader("Zone Universe pages (.com / .app)")
    if zone_rows:
        _results_table(
            [
                {
                    "_domain": row["domain"],
                    "Domain": row.get("final_url") or f"https://{row['domain']}",
                    "TLD": row["tld"],
                    "Title": row["title"] or "(no title)",
                    "Match": _clean_snippet(row["snippet"]),
                    "Crawled": (row["crawled_at"] or "")[:10],
                }
                for row in zone_rows
            ],
            columns=["Domain", "TLD", "Title", "Match", "Crawled"],
            key="overview_zone_table",
        )
        if st.button("Open full results in Zone Universe"):
            st.session_state["zone_query"] = active_query
            st.session_state["zone_page"] = 1
            st.switch_page("pages/zone_universe.py")
    else:
        st.caption("No matches in the .com/.app corpus.")
