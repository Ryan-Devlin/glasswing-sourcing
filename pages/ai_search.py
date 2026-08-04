from __future__ import annotations

import sqlite3
import sys
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

ROOT = Path(__file__).resolve().parents[1]
SEARCH_PROJECT = ROOT / "ai-domain-search-engine"
_search_database = Path(
    os.getenv("AI_SEARCH_DB", "data/search.sqlite3")
).expanduser()
SEARCH_DB = (
    _search_database
    if _search_database.is_absolute()
    else SEARCH_PROJECT / _search_database
).resolve()
SEARCH_SRC = SEARCH_PROJECT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SEARCH_SRC) not in sys.path:
    sys.path.insert(0, str(SEARCH_SRC))

from ai_search.og_bridge import (
    crawler_state,
    enqueue_for_screening,
    pipeline_status_for_corpus,
    search_companies,
    search_stats,
)
from classification_store import (
    bootstrap_from_csv,
    classification_map as store_classification_map,
    corpus_bucket_counts,
    count_classifications,
    query_corpus_classifications,
    recent_movements,
)
from common import (
    FILTERED,
    NO_BASKET_DATA,
    QUEUE_DB,
    YES_BASKET_DATA,
    read_basket_data,
    stable_domain,
)
from ui_theme import render_hideable_table, render_worker_controls

st.markdown("""
<style>
.block-container{max-width:1750px;padding:1rem 1.5rem 3rem}
h1{font-size:1.9rem!important;margin-bottom:.1rem!important}
div[data-testid="stDataFrame"]{border:1px solid rgba(128,128,128,.18);
border-radius:10px;overflow:hidden}
</style>
""", unsafe_allow_html=True)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def classification_map(
    domains: list[str] | pd.Series | None = None,
) -> dict[str, str]:
    result = store_classification_map(domains)
    if result:
        return result
    frame = read_csv(FILTERED / "all_screened.csv")
    if frame.empty or "domain" not in frame:
        return {}
    if domains is not None:
        wanted = {stable_domain(domain) for domain in domains}
        frame = frame[
            frame["domain"].map(stable_domain).isin(wanted)
        ].copy()
    bucket = frame.get(
        "routing_bucket",
        pd.Series("screened", index=frame.index),
    )
    return dict(zip(frame["domain"].map(stable_domain), bucket.astype(str)))


def basket_domains(path: Path) -> set[str]:
    frame = read_basket_data(path)
    if frame.empty:
        return set()
    return set(frame["domain_url"].map(stable_domain))


def apply_actions(edited: pd.DataFrame) -> dict[str, int]:
    selected = edited[
        edited["Apply"] & edited["Action"].astype(str).ne("")
    ]
    counts = {"screen": 0}
    screen_domains: list[str] = []
    for _, row in selected.iterrows():
        domain = stable_domain(row["Domain"])
        action = str(row["Action"])
        if action == "Send to Python screening":
            screen_domains.append(domain)
            counts["screen"] += 1
    if screen_domains:
        result = enqueue_for_screening(
            QUEUE_DB,
            screen_domains,
            source_run_id="ai_search_page",
        )
        counts["screen"] = result["queued"]
    return counts


st.title(".AI Company Search")

if not SEARCH_DB.exists():
    st.error(f"Search database not found: {SEARCH_DB}")
    st.stop()

if count_classifications() == 0:
    with st.spinner("Preparing the indexed classification store…"):
        bootstrap_from_csv()

stats = search_stats(SEARCH_DB)
metrics = st.columns([1, 1, 1, 1, 1, 1])
for column, (label, value) in zip(metrics, [
    ("Discovered", stats["domains"]),
    ("Crawled", stats["crawled"]),
    ("Searchable pages", stats["indexed_pages"]),
    ("Coming soon", stats["coming_soon"]),
    ("Parked", stats["parked"]),
    ("Coverage", f"{stats['crawled'] / max(stats['domains'], 1):.1%}"),
]):
    column.metric(label, f"{value:,}" if isinstance(value, int) else value)

enrichment_metrics = st.columns([1, 1, 4])
enrichment_metrics[0].metric(
    "Homepage sufficient",
    f"{stats['homepage_only']:,}",
    help=(
        "Sites retained and classified after one homepage request because "
        "more evidence was unnecessary or unavailable."
    ),
)
enrichment_metrics[1].metric(
    "Evidence enriched",
    f"{stats['evidence_enriched']:,}",
    help=(
        "Sites with additional Product, Technology, Security, Research, "
        "About, or Team pages retained."
    ),
)

ai_worker_state = crawler_state(SEARCH_DB)
render_worker_controls(
    "ai_crawler",
    label=".AI crawler",
    actual_status=str(
        ai_worker_state.get(
            "crawler_status",
            ai_worker_state.get("worker_status", "unknown"),
        )
    ),
    key="ai_crawler",
)
st.caption(
    "Pause stops new crawl batches after the current safe operation. "
    "Resume continues from the saved queue and checkpoints."
)

refresh_col, refresh_setting_col = st.columns([1, 4])
refresh_col.button("Refresh data now", width="stretch")
auto_refresh_enabled = refresh_setting_col.toggle(
    "Auto-refresh every 5 minutes",
    value=False,
    key="ai_page_auto_refresh_enabled",
    help=(
        "Off by default so checkbox selections, table sorting, and your "
        "current review position are not interrupted."
    ),
)

tabs = st.tabs([
    "Search results",
    "Processing & buckets",
    "Automation",
    "Current decisions",
])

with tabs[0]:
    st.subheader("Search crawled website content")
    with st.form("ai_page_filters"):
        search_col, type_col, words_col, rows_col = st.columns(
            [5, 2, 1.5, 1.5]
        )
        query = search_col.text_input(
            "Website content",
            value=st.session_state.get("ai_page_query", ""),
            placeholder='robotics · "drug discovery" · security -consulting',
        )
        site_type = type_col.selectbox(
            "Site type",
            ["All active sites", "Established content", "Coming soon", "Parked"],
        )
        minimum_words = words_col.number_input(
            "Minimum words", 0, 10000, 25, 25
        )
        rows_per_page = rows_col.selectbox(
            "Rows per page", [25, 50, 100, 200], index=2
        )
        submitted = st.form_submit_button(
            "Search indexed sites", type="primary", width="stretch"
        )
    st.caption(
        'Multi-word searches use exact phrase matching automatically. '
        'Use `-word` to exclude a term.'
    )
    if submitted:
        st.session_state["ai_page_query"] = query.strip()
        st.session_state["ai_page_number"] = 1

    active_query = st.session_state.get("ai_page_query", "")
    if not active_query:
        st.info("Enter any technology, market, product, or customer term.")
    else:
        page = int(st.session_state.get("ai_page_number", 1))
        try:
            rows = search_companies(
                SEARCH_DB,
                active_query,
                limit=int(rows_per_page),
                offset=(page - 1) * int(rows_per_page),
                minimum_words=int(minimum_words),
                site_type=site_type,
                exclude_parked=site_type != "Parked",
            )
        except (ValueError, sqlite3.Error) as exc:
            st.error(str(exc))
            rows = []

        yes_domains = basket_domains(YES_BASKET_DATA)
        no_domains = basket_domains(NO_BASKET_DATA)
        if rows:
            source = pd.DataFrame(rows).drop_duplicates("domain", keep="first")
            domains = source["domain"].map(stable_domain)
            buckets = classification_map(domains)
            current = []
            for domain in domains:
                if domain in yes_domains:
                    current.append("Yes Basket")
                elif domain in no_domains:
                    current.append("No Basket")
                else:
                    current.append(buckets.get(domain, "Not screened"))
            editor = pd.DataFrame({
                "Apply": False,
                "Action": "",
                "Domain": domains,
                "Current List": current,
                "Title": source["title"],
                "Description": source["description"],
                "Match": (
                    source["snippet"]
                    .str.replace("<mark>", "", regex=False)
                    .str.replace("</mark>", "", regex=False)
                ),
                "Words": source["word_count"],
                "Crawled": source["crawled_at"].str.slice(0, 10),
                "Website": source["final_url"],
            })
            edited = render_hideable_table(
                editor,
                domain_column="Domain",
                editable_columns=["Apply", "Action"],
                height=min(900, 40 + len(editor) * 35),
                column_config={
                    "Apply": st.column_config.CheckboxColumn(width="small"),
                    "Action": st.column_config.SelectboxColumn(
                        options=[
                            "",
                            "Send to Python screening",
                        ],
                        width="medium",
                    ),
                    "Current List": st.column_config.TextColumn(width="medium"),
                    "Title": st.column_config.TextColumn(width="large"),
                    "Description": st.column_config.TextColumn(width="large"),
                    "Match": st.column_config.TextColumn(width="large"),
                    "Website": st.column_config.LinkColumn(
                        display_text="Open ↗", width="small"
                    ),
                },
                key=f"ai_actions_{active_query}_{page}",
            )
            selected_count = int(
                (edited["Apply"] & edited["Action"].astype(str).ne("")).sum()
            )
            if st.button(
                f"Apply {selected_count} selected actions",
                type="primary",
                disabled=selected_count == 0,
            ):
                result = apply_actions(edited)
                st.success(
                    f"Queued {result['screen']} for screening."
                )
                st.rerun()
            previous, next_col, _ = st.columns([1, 1, 8])
            if previous.button("← Previous", disabled=page <= 1):
                st.session_state["ai_page_number"] = page - 1
                st.rerun()
            if next_col.button(
                "Next →", disabled=len(rows) < int(rows_per_page)
            ):
                st.session_state["ai_page_number"] = page + 1
                st.rerun()
        else:
            st.warning("No currently indexed website matches these filters.")

with tabs[1]:
    st.subheader("Crawled .AI processing")
    pipeline = pipeline_status_for_corpus(
        SEARCH_DB, QUEUE_DB, crawled_only=True
    )
    raw_bucket_counts = corpus_bucket_counts(SEARCH_DB)
    bucket_counts = {
        "Investable": raw_bucket_counts.get("priority", 0),
        "Coming Soon": raw_bucket_counts.get("coming_soon", 0),
        "Needs Review": raw_bucket_counts.get("review", 0),
        "Potential Comp": raw_bucket_counts.get("comp", 0),
        "Insufficient": sum(
            raw_bucket_counts.get(bucket, 0)
            for bucket in ["insufficient", "ghost", "screened_out"]
        ),
        "Hard Denied": raw_bucket_counts.get("reject", 0),
    }
    bucketed_records = sum(raw_bucket_counts.values())
    processing_metrics = st.columns([1, 1, 1, 1, 1, 1])
    for column, (label, value) in zip(processing_metrics, [
        ("In screening registry", pipeline.get("in_screening_registry", 0)),
        ("Pending", pipeline.get("pending", 0)),
        ("Processing now", pipeline.get("processing", 0)),
        ("Screened complete", pipeline.get("complete", 0)),
        ("Not queued", pipeline.get("not_queued", 0)),
        ("Bucketed records", bucketed_records),
    ]):
        column.metric(label, f"{int(value):,}")

    bucket_metrics = st.columns(len(bucket_counts))
    for column, (label, value) in zip(
        bucket_metrics, bucket_counts.items()
    ):
        column.metric(label, f"{value:,}")

    st.caption(
        "Every count and table on this tab includes only domains that the "
        ".AI crawler has finished crawling. Uncrawled corpus domains and "
        "unrelated CertStream domains are excluded."
    )

    bucket_specs = [
        ("Investable", ["priority"]),
        ("Coming Soon", ["coming_soon"]),
        ("Needs Review", ["review"]),
        ("Potential Comp", ["comp"]),
        ("Insufficient Evidence", ["insufficient", "ghost", "screened_out"]),
        ("Hard Denied", ["reject"]),
    ]
    bucket_tabs = st.tabs([name for name, _ in bucket_specs])
    for bucket_tab, (bucket_name, routing_buckets) in zip(
        bucket_tabs,
        bucket_specs,
    ):
        with bucket_tab:
            total_rows = sum(
                raw_bucket_counts.get(bucket, 0)
                for bucket in routing_buckets
            )
            if total_rows == 0:
                st.info("No companies currently appear in this bucket.")
            else:
                sort_col, rows_col, page_status_col = st.columns(
                    [2.5, 1, 2]
                )
                with sort_col:
                    sort_choice = st.selectbox(
                        "Sort companies",
                        [
                            "Priority: high to low",
                            "Fit: high to low",
                            "Confidence: high to low",
                            "Recently classified",
                            "Company: A to Z",
                            "Domain: A to Z",
                        ],
                        key=f"crawler_bucket_sort_{bucket_name}",
                    )
                with rows_col:
                    bucket_page_size = st.selectbox(
                        "Rows",
                        [25, 50, 100, 200],
                        index=1,
                        key=f"crawler_bucket_rows_{bucket_name}",
                    )
                page_key = f"crawler_bucket_page_{bucket_name}"
                current_page = int(st.session_state.get(page_key, 1))
                maximum_page = max(
                    1,
                    (total_rows + int(bucket_page_size) - 1)
                    // int(bucket_page_size),
                )
                current_page = min(current_page, maximum_page)
                st.session_state[page_key] = current_page
                page_status_col.caption(
                    f"{total_rows:,} companies · page "
                    f"{current_page:,} of {maximum_page:,}"
                )

                selected_rows = query_corpus_classifications(
                    SEARCH_DB,
                    routing_buckets,
                    limit=int(bucket_page_size),
                    offset=(current_page - 1) * int(bucket_page_size),
                    sort=sort_choice,
                )
                selected_bucket = pd.DataFrame(selected_rows)
                if (
                    bucket_name == "Insufficient Evidence"
                    and raw_bucket_counts.get("ghost", 0)
                ):
                    st.caption(
                        f"{raw_bucket_counts.get('ghost', 0):,} records were "
                        "originally marked Ghost. Availability is now shown "
                        "separately; cached crawler content can still be "
                        "reviewed and reclassified."
                    )

                bucket_table = pd.DataFrame({
                    "Company": selected_bucket["company_name"],
                    "Domain": selected_bucket["domain"],
                    "Availability": selected_bucket["availability_status"],
                    "Fit": selected_bucket["fit_score"],
                    "Venture": pd.to_numeric(
                        selected_bucket["venture_quality_score"],
                        errors="coerce",
                    ).fillna(0).astype(int),
                    "Wedge": pd.to_numeric(
                        selected_bucket["venture_wedge_score"],
                        errors="coerce",
                    ).fillna(0).astype(int),
                    "Moat": pd.to_numeric(
                        selected_bucket["venture_moat_score"],
                        errors="coerce",
                    ).fillna(0).astype(int),
                    "Founders": pd.to_numeric(
                        selected_bucket["venture_founder_score"],
                        errors="coerce",
                    ).fillna(0).astype(int),
                    "Confidence": selected_bucket["confidence_score"],
                    "Priority": selected_bucket["priority_score"],
                    "Description": selected_bucket["description"],
                    "Reason": selected_bucket["routing_reason"],
                    "Website": selected_bucket["final_url"],
                })
                render_hideable_table(
                    bucket_table,
                    domain_column="Domain",
                    height=min(760, 45 + len(bucket_table) * 35),
                    column_config={
                        "Fit": st.column_config.ProgressColumn(
                            "Fit",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Venture": st.column_config.ProgressColumn(
                            "Venture quality",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Wedge": st.column_config.ProgressColumn(
                            "Wedge",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Moat": st.column_config.ProgressColumn(
                            "Moat",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Founders": st.column_config.ProgressColumn(
                            "Founder evidence",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Confidence": st.column_config.ProgressColumn(
                            "Confidence",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Priority": st.column_config.ProgressColumn(
                            "Priority",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "Website": st.column_config.LinkColumn(
                            "Website",
                            display_text="Open website",
                        ),
                    },
                    key=f"crawler_bucket_editor_{bucket_name}",
                )

                previous_col, next_col, _ = st.columns([1, 1, 6])
                if previous_col.button(
                    "← Previous",
                    disabled=current_page <= 1,
                    key=f"crawler_bucket_previous_{bucket_name}",
                ):
                    st.session_state[page_key] = current_page - 1
                    st.rerun()
                if next_col.button(
                    "Next →",
                    disabled=current_page >= maximum_page,
                    key=f"crawler_bucket_next_{bucket_name}",
                ):
                    st.session_state[page_key] = current_page + 1
                    st.rerun()

                message_key = f"crawler_bucket_message_{bucket_name}"
                if st.session_state.get(message_key):
                    st.success(
                        st.session_state.pop(message_key)
                    )

with tabs[2]:
    st.subheader("Automatic crawler → Python screening")
    current_pipeline = pipeline_status_for_corpus(SEARCH_DB, QUEUE_DB)
    if current_pipeline.get("not_queued", 0) == 0:
        st.info(
            f"All {stats['domains']:,} discovered domains are already in the "
            "original screening registry. The pipeline is working through "
            f"{current_pipeline.get('pending', 0):,} pending domains now."
        )
    else:
        st.info(
            f"{current_pipeline.get('not_queued', 0):,} discovered domains "
            "are not yet in the original screening registry."
        )
    st.markdown("#### Dedicated crawled-corpus datasets")
    dataset_rows = []
    for filename in [
        "crawled_all_screened.csv",
        "crawled_investable.csv",
        "crawled_coming_soon.csv",
        "crawled_needs_review.csv",
        "crawled_insufficient_evidence.csv",
        "crawled_hard_denied.csv",
    ]:
        path = FILTERED / filename
        frame = read_csv(path)
        dataset_rows.append({
            "Dataset": filename,
            "Rows": len(frame),
            "Updated": (
                pd.Timestamp(path.stat().st_mtime, unit="s").isoformat()
                if path.exists() else "Waiting for next screened batch"
            ),
            "Path": str(path),
        })
    st.dataframe(
        pd.DataFrame(dataset_rows),
        hide_index=True,
        width="stretch",
    )
    st.caption(
        "These are compatibility exports for external use. The live website "
        "reads classifications from the indexed SQLite store."
    )

with tabs[3]:
    movements = pd.DataFrame(recent_movements(limit=100))
    if not movements.empty:
        st.subheader("Recent bucket movements")
        movements = movements.rename(columns={
            "domain": "Domain",
            "from_bucket": "From",
            "to_bucket": "To",
            "objective_score": "Score",
            "note": "Note",
            "source": "Source",
            "moved_at": "Moved",
        })
        st.dataframe(
            movements,
            hide_index=True,
            width="stretch",
            height=min(420, 45 + len(movements) * 35),
        )
    yes = read_basket_data(YES_BASKET_DATA)
    no = read_basket_data(NO_BASKET_DATA)
    left, right = st.columns(2)
    left.subheader(f"Yes Basket · {len(yes):,}")
    left.dataframe(yes, hide_index=True, width="stretch", height=650)
    right.subheader(f"No Basket · {len(no):,}")
    right.dataframe(no, hide_index=True, width="stretch", height=650)

if auto_refresh_enabled:
    st_autorefresh(
        interval=300_000,
        key="ai_page_live_refresh_5m",
    )
