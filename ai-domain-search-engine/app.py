from __future__ import annotations

import html
import sqlite3

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from ai_search.config import load_settings
from ai_search.database import connect, diagnostics, utc_now, worker_state
from ai_search.search import SearchOptions, search

st.set_page_config(
    page_title=".AI Company Search",
    page_icon="⌕",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown("""
<style>
[data-testid="stHeader"]{height:0}
.block-container{max-width:1750px;padding:1rem 1.6rem 3rem}
h1{font-size:1.75rem!important;margin:0!important}
.subtle{color:#7b8494;font-size:.84rem}
.status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;
background:#27c281;margin-right:6px}
div[data-testid="stMetric"]{border:1px solid rgba(128,128,128,.18);
border-radius:10px;padding:.55rem .8rem}
div[data-testid="stMetric"] label{font-size:.76rem}
div[data-testid="stMetricValue"]{font-size:1.35rem}
div[data-testid="stDataFrame"]{border:1px solid rgba(128,128,128,.18);
border-radius:10px;overflow:hidden}
.match-row{border-bottom:1px solid rgba(128,128,128,.16);padding:.6rem .2rem}
.match-title{font-weight:650;font-size:.98rem}
.match-meta{color:#7b8494;font-size:.77rem}
.match-snippet{font-size:.86rem;margin-top:.2rem;line-height:1.35}
mark{background:#f6d75c;color:#111;padding:0 .12rem;border-radius:2px}
[data-testid="stSidebar"] .block-container{padding-top:1rem}
</style>
""", unsafe_allow_html=True)

settings = load_settings()


def db() -> sqlite3.Connection:
    return connect(settings.database)


def option_values(conn: sqlite3.Connection, column: str) -> list[str]:
    allowed = {"discovery_source", "latest_crawl_status"}
    if column not in allowed:
        return []
    return [
        str(row[0]) for row in conn.execute(
            f"SELECT DISTINCT {column} FROM domains "
            f"WHERE {column} != '' ORDER BY 1"
        )
    ]


def clean_snippet(value: str) -> str:
    safe = html.escape(value or "")
    return safe.replace("&lt;mark&gt;", "<mark>").replace(
        "&lt;/mark&gt;", "</mark>"
    )


conn = db()
stats = diagnostics(conn)
state = worker_state(conn)
total = max(int(stats["domains"]), 1)
crawled = int(stats["crawled"])
coverage = crawled / total

header_left, header_right = st.columns([5, 2])
with header_left:
    st.title("⌕ .AI Company Search")
    st.markdown(
        '<div class="subtle">Search and triage the actual content of '
        f'{int(stats["domains"]):,} discovered .ai domains.</div>',
        unsafe_allow_html=True,
    )
with header_right:
    worker = state.get("worker_status", "unknown")
    st.markdown(
        f'<div style="text-align:right;padding-top:.4rem">'
        f'<span class="status-dot"></span>Worker {html.escape(worker)}'
        f'<br><span class="subtle">{coverage:.1%} initial crawl complete</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

metric_cols = st.columns(7)
metrics = [
    ("Domains", f"{int(stats['domains']):,}"),
    ("Crawled", f"{crawled:,}"),
    ("Searchable pages", f"{int(stats['indexed_pages']):,}"),
    ("Pending", f"{int(stats['pending']):,}"),
    ("Coming soon", f"{int(stats['coming_soon']):,}"),
    ("Parked", f"{int(stats['parked']):,}"),
    ("Shortlisted", f"{int(stats['shortlisted']):,}"),
]
for column, (label, value) in zip(metric_cols, metrics):
    column.metric(label, value)

tabs = st.tabs(["Search companies", "Browse latest", "Shortlist", "Crawl monitor"])

with st.sidebar:
    st.markdown("### Search controls")
    with st.form("filters"):
        query = st.text_input(
            "Website content",
            value=st.session_state.get("query", ""),
            placeholder='robotics · "drug discovery" · security -consulting',
        )
        domain_contains = st.text_input(
            "Domain contains", placeholder="optional"
        )
        site_type = st.selectbox(
            "Site type",
            ["All active sites", "Established content", "Coming soon", "Parked"],
        )
        source_values = ["All sources"] + option_values(
            conn, "discovery_source"
        )
        source = st.selectbox("Discovery source", source_values)
        status_values = ["All statuses"] + option_values(
            conn, "latest_crawl_status"
        )
        crawl_status = st.selectbox("Crawl status", status_values)
        min_words = st.number_input(
            "Minimum page words", 0, 100_000, 0, 25
        )
        title_only = st.checkbox("Match titles only")
        exclude_parked = st.checkbox("Exclude parked", value=True)
        sort = st.selectbox(
            "Sort by", ["Relevance", "Recently crawled", "Newest"]
        )
        page_size = st.select_slider(
            "Rows per page", [25, 50, 100, 200], value=100
        )
        submitted = st.form_submit_button(
            "Search", type="primary", use_container_width=True
        )
    if submitted:
        st.session_state["query"] = query.strip()
        st.session_state["page"] = 1
    st.caption(
        'Syntax: words are combined with AND. Use quotes for an exact phrase '
        'and a minus sign to exclude: `security -consulting`.'
    )

active_query = st.session_state.get("query", "")

with tabs[0]:
    if not active_query:
        st.info(
            "Enter any word or phrase in the sidebar. Results search page "
            "titles, descriptions, headings, and visible website text."
        )
    else:
        page = int(st.session_state.get("page", 1))
        options = SearchOptions(
            limit=int(page_size),
            offset=(page - 1) * int(page_size),
            title_only=title_only,
            exclude_parked=exclude_parked and site_type != "Parked",
            minimum_words=int(min_words),
            source="" if source == "All sources" else source,
            sort=sort,
            crawl_status=(
                "" if crawl_status == "All statuses" else crawl_status
            ),
            site_type=site_type,
            domain_contains=domain_contains,
        )
        try:
            rows = search(conn, active_query, options)
        except ValueError as exc:
            st.warning(str(exc))
            rows = []

        top_left, top_mid, top_right = st.columns([3, 2, 2])
        top_left.markdown(
            f"**{len(rows):,} matches on page {page}** "
            f'<span class="subtle">for {html.escape(active_query)}</span>',
            unsafe_allow_html=True,
        )
        compact = top_mid.toggle("Compact scanner", value=True)
        auto_refresh = top_right.toggle("Live updates", value=True)
        if auto_refresh:
            st_autorefresh(interval=10_000, key="search-live-refresh")

        if rows:
            frame = pd.DataFrame(rows)
            frame.insert(0, "Save", False)
            display = pd.DataFrame({
                "Save": frame["Save"],
                "Company / Domain": frame["domain"],
                "Title": frame["title"],
                "Description": frame["description"],
                "Words": frame["word_count"],
                "Crawled": frame["crawled_at"].str.slice(0, 10),
                "Source": frame["discovery_source"],
                "Website": frame["final_url"],
            })
            if compact:
                edited = st.data_editor(
                    display,
                    hide_index=True,
                    use_container_width=True,
                    height=min(900, 39 + len(display) * 35),
                    disabled=[
                        "Company / Domain", "Title", "Description", "Words",
                        "Crawled", "Source", "Website",
                    ],
                    column_config={
                        "Save": st.column_config.CheckboxColumn(width="small"),
                        "Company / Domain": st.column_config.TextColumn(
                            width="medium"
                        ),
                        "Title": st.column_config.TextColumn(width="large"),
                        "Description": st.column_config.TextColumn(width="large"),
                        "Words": st.column_config.NumberColumn(width="small"),
                        "Crawled": st.column_config.TextColumn(width="small"),
                        "Source": st.column_config.TextColumn(width="medium"),
                        "Website": st.column_config.LinkColumn(
                            display_text="Open ↗", width="small"
                        ),
                    },
                    key=f"scanner-{active_query}-{page}",
                )
                selected = edited[edited["Save"]]["Company / Domain"].tolist()
                if selected and st.button(
                    f"Shortlist {len(selected)} selected",
                    type="primary",
                ):
                    for domain in selected:
                        domain_id = conn.execute(
                            "SELECT id FROM domains WHERE domain=?", (domain,)
                        ).fetchone()[0]
                        conn.execute("""
                            INSERT OR IGNORE INTO shortlists(domain_id, saved_at)
                            VALUES (?, ?)
                        """, (domain_id, utc_now()))
                    conn.commit()
                    st.success(f"Saved {len(selected)} companies.")
            else:
                for row in rows:
                    st.markdown(
                        '<div class="match-row">'
                        f'<div class="match-title">{html.escape(row["title"] or row["domain"])}</div>'
                        f'<div class="match-meta">{html.escape(row["domain"])} · '
                        f'{row["word_count"]:,} words · {html.escape(row["crawled_at"][:10])}</div>'
                        f'<div class="match-snippet">{clean_snippet(row["snippet"])}</div>'
                        '</div>',
                        unsafe_allow_html=True,
                    )
            previous, next_col, spacer = st.columns([1, 1, 8])
            if previous.button("← Previous", disabled=page <= 1):
                st.session_state["page"] = page - 1
                st.rerun()
            if next_col.button("Next →", disabled=len(rows) < page_size):
                st.session_state["page"] = page + 1
                st.rerun()
        else:
            st.warning(
                "No indexed page matched these filters. Reduce filters or "
                "wait for more domains to finish crawling."
            )

with tabs[1]:
    latest = pd.read_sql_query("""
        SELECT d.domain AS Domain, p.title AS Title,
               p.meta_description AS Description, p.word_count AS Words,
               p.crawled_at AS Crawled, d.is_coming_soon AS ComingSoon,
               p.final_url AS Website
        FROM pages p JOIN domains d ON d.id=p.domain_id
        ORDER BY p.crawled_at DESC LIMIT 500
    """, conn)
    st.caption("The 500 most recently indexed pages. Updates every 10 seconds.")
    st.dataframe(
        latest,
        hide_index=True,
        use_container_width=True,
        height=720,
        column_config={
            "Website": st.column_config.LinkColumn(display_text="Open ↗"),
            "ComingSoon": st.column_config.CheckboxColumn("Coming soon"),
        },
    )

with tabs[2]:
    shortlist = pd.read_sql_query("""
        SELECT d.domain AS Domain, COALESCE(p.title, '') AS Title,
               COALESCE(p.meta_description, '') AS Description,
               s.saved_at AS Saved, COALESCE(p.final_url, 'https://' || d.domain)
               AS Website
        FROM shortlists s JOIN domains d ON d.id=s.domain_id
        LEFT JOIN pages p ON p.id=(
            SELECT id FROM pages WHERE domain_id=d.id
            ORDER BY crawled_at DESC LIMIT 1
        )
        ORDER BY s.saved_at DESC
    """, conn)
    if shortlist.empty:
        st.info("Select companies in the compact scanner to build a shortlist.")
    else:
        st.dataframe(
            shortlist,
            hide_index=True,
            use_container_width=True,
            height=700,
            column_config={
                "Website": st.column_config.LinkColumn(display_text="Open ↗")
            },
        )

with tabs[3]:
    st.progress(min(coverage, 1.0), text=(
        f"{crawled:,} of {int(stats['domains']):,} domains processed "
        f"({coverage:.2%})"
    ))
    status_frame = pd.DataFrame(
        conn.execute("""
            SELECT latest_crawl_status AS Status, count(*) AS Domains
            FROM domains GROUP BY latest_crawl_status ORDER BY Domains DESC
        """).fetchall(),
        columns=["Status", "Domains"],
    )
    attempts = pd.read_sql_query("""
        SELECT status AS Status, count(*) AS Attempts,
               round(avg(duration_ms)) AS AvgMilliseconds
        FROM crawl_attempts GROUP BY status ORDER BY Attempts DESC
    """, conn)
    left, right = st.columns(2)
    left.markdown("#### Domain outcomes")
    left.dataframe(status_frame, hide_index=True, use_container_width=True)
    right.markdown("#### Request diagnostics")
    right.dataframe(attempts, hide_index=True, use_container_width=True)
    st.markdown("#### Worker state")
    st.json(state)

conn.close()
