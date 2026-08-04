from __future__ import annotations

import os
import signal
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "zone-universe"
ZONE_SRC = PROJECT / "src"
AI_SRC = ROOT / "ai-domain-search-engine" / "src"
PAUSED_IMPORT_PID = PROJECT / "logs" / "paused_import.pid"
for source in (ROOT, ZONE_SRC, AI_SRC):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from ai_search.og_bridge import enqueue_for_screening
from ai_search.og_bridge import pipeline_status_for_corpus
from zone_universe.database import ZONE_UNIVERSE_DATABASE_ENV_VAR
from classification_store import (
    corpus_bucket_counts,
    query_corpus_classifications,
    record_manual_move,
)
from common import (
    MANUAL,
    QUEUE_DB,
    atomic_write_csv,
    remove_basket_entry,
    save_basket_entries,
    stable_domain,
)
from runtime_control import RUNTIME_MODE
from zone_universe.config import (
    DATABASE,
    PID_FILE,
    WORKER_LOG,
    ZONEFILES,
    load_settings,
)
from zone_universe.database import connect, diagnostics, get_state
from zone_universe.rdap import cached_registrations, lookup_and_save
from zone_universe.search import raw_html, search_pages
from zone_universe.zone_parser import discover_zonefiles


st.markdown(
    """
    <style>
    .block-container{max-width:1750px;padding:1rem 1.5rem 3rem}
    h1{font-size:1.9rem!important;margin-bottom:.1rem!important}
    div[data-testid="stDataFrame"]{border:1px solid rgba(128,128,128,.18);
    border-radius:10px;overflow:hidden}
    </style>
    """,
    unsafe_allow_html=True,
)


def process_alive(pid_path: Path) -> bool:
    if not pid_path.exists():
        return False
    try:
        pid = int(pid_path.read_text().strip())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def active_worker_pid() -> int | None:
    for path in (PID_FILE, PAUSED_IMPORT_PID):
        if not process_alive(path):
            continue
        try:
            return int(path.read_text().strip())
        except (OSError, ValueError):
            continue
    return None


def stage_copy(state: dict[str, str]) -> tuple[str, str]:
    stage = str(state.get("crawler_status", "stopped")).lower()
    if stage.startswith("importing_"):
        tld = stage.removeprefix("importing_").upper()
        return (
            f"Building the .{tld.lower()} domain list",
            f"The .{tld.lower()} zone download is complete. The system is "
            "removing obvious junk and building prioritized crawl queues. "
            "Website crawling waits until this one-time import finishes; "
            f"final .{tld.lower()} counts appear when it completes.",
        )
    if stage.startswith("crawling_"):
        tld = stage.removeprefix("crawling_")
        return (
            f"Crawling .{tld} websites",
            "Domains are being checked in parallel. Successful websites are "
            "saved, made searchable, and sent through company screening.",
        )
    if stage == "paused_low_disk":
        return (
            "Paused to protect disk space",
            "The crawler stopped before the configured free-space reserve was "
            "crossed. Move the HTML archive to a larger disk before resuming.",
        )
    if stage == "idle":
        return (
            "Caught up for now",
            "There is no immediately available crawl work. Scheduled retries "
            "and new zone files will be picked up automatically.",
        )
    return (
        "System stopped",
        "Your saved domains, HTML, decisions, and checkpoints are intact. "
        "Use System status to start processing again.",
    )


def launch_import(
    zone_path: Path,
    tld: str,
    *,
    limit: int,
) -> int:
    if RUNTIME_MODE != "local":
        raise RuntimeError(
            "Process controls are delegated to the cloud runtime. Use the "
            "cloud platform's worker controls instead."
        )
    log_path = PROJECT / "logs" / f"import-{tld}.log"
    pid_path = PROJECT / "logs" / f"import-{tld}.pid"
    if process_alive(pid_path):
        raise RuntimeError(f"An import for .{tld} is already running.")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = (
        f"{ZONE_SRC}:{AI_SRC}:"
        + environment.get("PYTHONPATH", "")
    )
    command = [
        str(ROOT / ".venv" / "bin" / "python"),
        "-m",
        "zone_universe.cli",
        "import-zone",
        str(zone_path),
        "--tld",
        tld,
        "--shards",
        "64",
    ]
    if limit:
        command.extend(["--limit", str(limit)])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        process = subprocess.Popen(
            command,
            cwd=PROJECT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.write_text(str(process.pid), encoding="utf-8")
    return process.pid


def worker_action(script_name: str) -> str:
    result = subprocess.run(
        [str(PROJECT / script_name)],
        cwd=PROJECT,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    return (result.stdout or result.stderr).strip()


def file_size(path: Path) -> str:
    if not path.exists():
        return "0 B"
    size = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def human_bytes(size: int | float) -> str:
    value = float(size or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def human_duration(seconds: int | float) -> str:
    remaining = max(0, int(float(seconds or 0)))
    days, remaining = divmod(remaining, 86_400)
    hours, remaining = divmod(remaining, 3_600)
    minutes, _ = divmod(remaining, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def registration_columns(domains: list[str]) -> tuple[list[str], list[str]]:
    cached = cached_registrations(domains)
    registered: list[str] = []
    registrars: list[str] = []
    for domain in domains:
        record = cached.get(stable_domain(domain), {})
        registered.append(str(record.get("registration_at", ""))[:10])
        registrars.append(str(record.get("registrar", "")))
    return registered, registrars


def enrich_registration_dates(domains: list[str]) -> tuple[int, list[str]]:
    unique = list(
        dict.fromkeys(
            stable_domain(domain)
            for domain in domains
            if stable_domain(domain)
        )
    )
    if len(unique) > 10:
        raise ValueError(
            "Select at most 10 domains per lookup. Registry services prohibit "
            "high-volume automated RDAP queries."
        )
    found = 0
    errors: list[str] = []
    for domain in unique:
        record = lookup_and_save(domain)
        if record.registration_at:
            found += 1
        elif record.lookup_status != "ok":
            errors.append(
                f"{domain}: {record.lookup_status}"
                + (
                    f" ({record.lookup_error})"
                    if record.lookup_error
                    else ""
                )
            )
        else:
            errors.append(f"{domain}: registration date was not returned")
    return found, errors


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def move_zone_domains(
    selected: pd.DataFrame,
    destination: str,
) -> int:
    if selected.empty:
        return 0
    target_map = {
        "Investable Candidate": ("Investable Candidate", "priority"),
        "Coming Soon": ("Coming Soon", "coming_soon"),
        "Needs Review": ("Review Candidate", "review"),
        "Potential Comp": ("Potential Comp", "comp"),
        "Insufficient Evidence": (
            "Insufficient Evidence",
            "insufficient",
        ),
        "Hard Denied": ("Hard Denied", "reject"),
    }
    target_label, routing_bucket = target_map[destination]
    domains = (
        selected["Domain"]
        .map(stable_domain)
        .loc[lambda values: values.ne("")]
        .drop_duplicates()
    )
    scores = pd.to_numeric(
        selected.get("Fit", 0),
        errors="coerce",
    ).fillna(0).clip(0, 100).astype(int)
    score_map = dict(
        zip(selected["Domain"].map(stable_domain), scores)
    )

    overrides_path = MANUAL / "domain_overrides.csv"
    overrides = read_csv(overrides_path)
    additions = pd.DataFrame({
        "domain": domains,
        "override_target": target_label,
        "override_note": "manual_move_from_zone_universe",
        "updated_at": pd.Timestamp.utcnow().isoformat(),
    })
    overrides = pd.concat(
        [overrides, additions],
        ignore_index=True,
        sort=False,
    )
    overrides["domain"] = overrides["domain"].map(stable_domain)
    overrides = overrides.drop_duplicates("domain", keep="last")
    atomic_write_csv(overrides_path, overrides)

    record_manual_move(
        domains,
        target_label=target_label,
        target_bucket=routing_bucket,
        scores=score_map,
        note="manual_move_from_zone_universe",
        source="zone_universe_page",
    )
    if destination == "Investable Candidate":
        save_basket_entries(
            [
                (domain, max(70, score_map.get(domain, 90)))
                for domain in domains
            ],
            "yes",
            source="human",
            reason="manual_zone_move_to_investable",
        )
    elif destination == "Hard Denied":
        save_basket_entries(
            [(domain, score_map.get(domain, 0)) for domain in domains],
            "no",
            source="human",
            reason="manual_zone_move_to_hard_denied",
        )
    else:
        for domain in domains:
            remove_basket_entry(domain)
    return len(domains)


st.title(".COM + .APP Company Search")

conn = connect(DATABASE)
stats = {
    key: 0
    for key in (
        "zones",
        "queued_domains",
        "attempted",
        "success",
        "searchable_pages",
        "documents",
        "homepage_only",
        "evidence_enriched",
        "low_priority_quarantined",
        "raw_html_bytes",
        "compressed_html_bytes",
        "gibberish_rejected",
        "retries_pending",
        "registration_dates",
        "registration_queued",
    )
}
stats.update(diagnostics(conn))
state = get_state(conn)
zone_rows = [
    dict(row)
    for row in conn.execute(
        "SELECT * FROM zones ORDER BY tld"
    )
]
progress_rows = [
    dict(row)
    for row in conn.execute(
        """
        SELECT z.tld,z.import_status,z.discovered_domains,
               coalesce(sum(s.attempted),0) AS attempted,
               coalesce(sum(s.success),0) AS successful,
               coalesce(sum(s.dns_error),0) AS dns_errors,
               coalesce(sum(s.timeout),0) AS timeouts,
               coalesce(sum(s.blocked),0) AS blocked,
               coalesce(sum(s.http_error),0) AS http_errors,
               coalesce(sum(s.empty),0) AS empty_pages,
               coalesce(sum(s.other_error),0) AS other_errors,
               coalesce(sum(s.skipped_low_priority),0)
                   AS low_priority_quarantined,
               z.rejected_gibberish
        FROM zones z
        LEFT JOIN shards s
          ON s.tld=z.tld
         AND s.queue_version=z.active_queue_version
        GROUP BY z.tld,z.import_status,z.discovered_domains,
                 z.rejected_gibberish
        ORDER BY z.tld
        """
    )
]
saved_by_tld = {
    str(row["tld"]): int(row["saved"])
    for row in conn.execute(
        "SELECT tld,count(*) AS saved FROM pages GROUP BY tld"
    )
}
conn.close()
settings = load_settings()
if RUNTIME_MODE == "local":
    worker_pid = active_worker_pid()
    worker_running: bool | None = worker_pid is not None
else:
    # active_worker_pid() reads a local PID file that only exists on the
    # zone-worker container -- this page renders on the web container, so
    # there's no local process to check. Trust the shared crawler_status
    # below instead of reporting a false "stopped".
    worker_pid = None
    worker_running = None
stage_title, stage_description = stage_copy(state)
active_stage = str(state.get("crawler_status", "")).lower()
zone_by_tld = {str(row.get("tld", "")): row for row in zone_rows}

st.markdown("### What is happening now")
if active_stage.startswith(("importing_", "crawling_")) and worker_running is not False:
    st.info(f"**{stage_title}**\n\n{stage_description}")
elif active_stage == "paused_low_disk":
    st.error(f"**{stage_title}**\n\n{stage_description}")
else:
    st.warning(f"**{stage_title}**\n\n{stage_description}")

st.markdown("### Domain lists")
domain_columns = st.columns(2)
for domain_column, tld in zip(domain_columns, ("app", "com")):
    row = zone_by_tld.get(tld, {})
    status = str(row.get("import_status", "not_imported"))
    with domain_column.container(border=True):
        st.markdown(f"#### .{tld.upper()}")
        if status == "ready":
            kept = int(row.get("discovered_domains", 0) or 0)
            skipped = int(row.get("rejected_gibberish", 0) or 0)
            total = kept + skipped
            st.success("Domain list ready")
            summary = st.columns(2)
            summary[0].metric("Names processed", f"{total:,}")
            summary[1].metric(
                "Websites saved so far",
                f"{saved_by_tld.get(tld, 0):,}",
            )
            split = st.columns(2)
            split[0].metric("Kept for crawling", f"{kept:,}")
            split[1].metric("Skipped as likely junk", f"{skipped:,}")
            st.caption(
                f"{total:,} processed = {kept:,} kept + "
                f"{skipped:,} skipped."
            )
        elif status == "importing":
            st.warning("Filtering and building the domain list now")
            st.metric(
                "Downloaded file",
                file_size(Path(str(row.get("zone_path", "")))),
            )
            pending = st.columns(2)
            pending[0].metric("Kept for crawling", "Pending")
            pending[1].metric("Skipped as likely junk", "Pending")
            st.caption(
                "The importer is still counting. Final totals appear together "
                "when the new queue is saved; zero is not being reported."
            )
        elif status == "error":
            st.error(str(row.get("import_error", "Import failed.")))
        else:
            st.info("No completed domain-list import yet.")

database_metrics = st.columns(5)
database_metrics[0].metric(
    "Searchable websites",
    f"{stats['searchable_pages']:,}",
)
database_metrics[1].metric(
    "Homepage sufficient",
    f"{stats['homepage_only']:,}",
    help=(
        "One homepage was enough to retain and classify the site without "
        "spending more requests."
    ),
)
database_metrics[2].metric(
    "Evidence enriched",
    f"{stats['evidence_enriched']:,}",
    help=(
        "Additional Product, Technology, Security, Research, About, or Team "
        "pages were retained."
    ),
)
database_metrics[3].metric(
    "HTML documents retained",
    f"{stats['documents']:,}",
)
database_metrics[4].metric(
    "Compressed HTML storage",
    human_bytes(stats["compressed_html_bytes"]),
)

st.button("Refresh status")
available_gb = shutil.disk_usage(PROJECT).free / (1024 ** 3)
bytes_per_fetched_site = (
    stats["compressed_html_bytes"] / max(stats["success"], 1)
)
safe_archive_gb = max(0.0, available_gb - settings.minimum_free_gb)
local_capacity = int(
    safe_archive_gb * (1024 ** 3) / max(bytes_per_fetched_site, 1)
)
remaining_total = max(
    0,
    int(stats["queued_domains"])
    - int(stats["attempted"])
    - int(stats["low_priority_quarantined"]),
)
projected_remaining_gb = (
    remaining_total * bytes_per_fetched_site / (1024 ** 3)
)
tabs = st.tabs([
    "Search",
    "Companies",
    "Progress",
    "Settings",
])

with tabs[0]:
    st.subheader("Search saved website content")
    st.caption(
        "Search only the websites that have already been successfully fetched. "
        "The complete retained HTML—not merely the domain name—is searched."
    )
    with st.expander("Registration date lookup"):
        with st.form("direct_registration_lookup"):
            direct_domain = st.text_input(
                "Domain",
                placeholder="example.com",
                key="zone_direct_rdap_domain",
            )
            direct_lookup = st.form_submit_button(
                "Look up authoritative registration",
                type="primary",
            )
        if direct_lookup:
            try:
                with st.spinner("Querying the registry RDAP service…"):
                    direct_record = lookup_and_save(direct_domain)
                st.session_state["zone_direct_rdap_result"] = (
                    direct_record.__dict__
                )
            except ValueError as exc:
                st.error(str(exc))
        direct_result = st.session_state.get("zone_direct_rdap_result")
        if direct_result:
            direct_columns = st.columns(4)
            direct_columns[0].metric(
                "Registered",
                str(direct_result.get("registration_at", ""))[:10]
                or "Not returned",
            )
            direct_columns[1].metric(
                "Expires",
                str(direct_result.get("expiration_at", ""))[:10]
                or "Not returned",
            )
            direct_columns[2].metric(
                "Registrar",
                str(direct_result.get("registrar", "")) or "Not returned",
            )
            direct_columns[3].metric(
                "Lookup",
                str(direct_result.get("lookup_status", "")).replace(
                    "_",
                    " ",
                ).title(),
            )
    with st.form("zone_search_form"):
        search_col, tld_col, words_col, rows_col = st.columns(
            [5, 1.2, 1.2, 1.2]
        )
        query = search_col.text_input(
            "Search website HTML",
            value=st.session_state.get("zone_query", ""),
            placeholder="quantum data",
        )
        tld_filter = tld_col.selectbox("TLD", ["All", ".app", ".com"])
        minimum_words = words_col.number_input(
            "Minimum words",
            min_value=0,
            max_value=10000,
            value=25,
            step=25,
        )
        rows_per_page = rows_col.selectbox(
            "Rows",
            [25, 50, 100, 200],
            index=1,
        )
        submitted = st.form_submit_button(
            "Search indexed websites",
            type="primary",
            width="stretch",
        )
    st.caption(
        "Multi-word searches are exact phrases automatically. Raw HTML is "
        "retained compressed; searchable content includes page text, metadata, "
        "headings, image alt text, and JSON-LD."
    )
    if submitted:
        st.session_state["zone_query"] = query.strip()
        st.session_state["zone_page"] = 1
    active_query = st.session_state.get("zone_query", "")
    if not active_query:
        st.info("Enter a company, market, product, or technology phrase.")
    else:
        page = int(st.session_state.get("zone_page", 1))
        try:
            results = search_pages(
                active_query,
                tld="" if tld_filter == "All" else tld_filter.lstrip("."),
                limit=int(rows_per_page),
                offset=(page - 1) * int(rows_per_page),
                minimum_words=int(minimum_words),
            )
        except (ValueError, sqlite3.Error) as exc:
            st.error(str(exc))
            results = []
        if results:
            source = pd.DataFrame(results)
            table = pd.DataFrame({
                "Select": False,
                "Domain": source["domain"],
                "TLD": source["tld"],
                "Title": source["title"],
                "Description": source["description"],
                "Registered": source["registration_at"].str.slice(0, 10),
                "Registrar": source["registrar"],
                "Match": (
                    source["snippet"]
                    .str.replace("<mark>", "", regex=False)
                    .str.replace("</mark>", "", regex=False)
                ),
                "Words": source["word_count"],
                "Website": source["final_url"],
            })
            edited = st.data_editor(
                table,
                hide_index=True,
                width="stretch",
                height=min(760, 45 + len(table) * 35),
                disabled=[
                    "Domain",
                    "TLD",
                    "Title",
                    "Description",
                    "Registered",
                    "Registrar",
                    "Match",
                    "Words",
                    "Website",
                ],
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select"),
                    "Website": st.column_config.LinkColumn(
                        "Website",
                        display_text="Open website",
                    ),
                },
                key=f"zone_results_{active_query}_{page}",
            )
            chosen = edited[edited["Select"]]
            action_col, rdap_col, html_col, _ = st.columns(
                [1.6, 1.6, 1.6, 3]
            )
            if action_col.button(
                f"Screen {len(chosen)} selected",
                disabled=chosen.empty,
                type="primary",
            ):
                result = enqueue_for_screening(
                    QUEUE_DB,
                    chosen["Domain"].tolist(),
                    source_run_id="zone_universe_search",
                )
                st.success(
                    f"Queued {result['queued']:,}; "
                    f"{result['already_known']:,} were already in the "
                    "screening system."
                )
            if rdap_col.button(
                "Look up registration dates",
                disabled=chosen.empty or len(chosen) > 10,
                help=(
                    "Looks up and caches authoritative registry dates for up "
                    "to 10 selected domains."
                ),
            ):
                with st.spinner("Querying registry RDAP services…"):
                    found, errors = enrich_registration_dates(
                        chosen["Domain"].tolist()
                    )
                if found:
                    st.success(f"Saved {found} registration date(s).")
                if errors:
                    st.warning("\n".join(errors))
                if found:
                    st.rerun()
            if html_col.button(
                "Prepare selected raw HTML",
                disabled=len(chosen) != 1,
            ):
                selected_domain = str(chosen.iloc[0]["Domain"])
                html = raw_html(selected_domain)
                if html:
                    st.session_state["zone_html_download"] = (
                        selected_domain,
                        html,
                    )
            saved_download = st.session_state.get("zone_html_download")
            if saved_download:
                selected_domain, html = saved_download
                st.download_button(
                    f"Download {selected_domain} HTML",
                    data=html.encode("utf-8"),
                    file_name=f"{selected_domain}.html",
                    mime="text/html",
                )
            previous, next_col, _ = st.columns([1, 1, 8])
            if previous.button("← Previous", disabled=page <= 1):
                st.session_state["zone_page"] = page - 1
                st.rerun()
            if next_col.button(
                "Next →",
                disabled=len(results) < int(rows_per_page),
            ):
                st.session_state["zone_page"] = page + 1
                st.rerun()
        else:
            st.warning("No crawled website matches this exact phrase.")

with tabs[1]:
    st.subheader("Review and organize crawled companies")
    st.caption(
        "This area contains only .com and .app websites successfully fetched "
        "by this crawler. Select rows to move companies between buckets; your "
        "manual decisions also update the main Glasswing Yes/No datasets."
    )
    pipeline = pipeline_status_for_corpus(
        DATABASE,
        QUEUE_DB,
        crawled_only=True,
        search_db_env_var=ZONE_UNIVERSE_DATABASE_ENV_VAR,
    )
    raw_bucket_counts = corpus_bucket_counts(DATABASE)
    bucket_counts = {
        "Investable": raw_bucket_counts.get("priority", 0),
        "Coming Soon": raw_bucket_counts.get("coming_soon", 0),
        "Needs Review": raw_bucket_counts.get("review", 0),
        "Potential Comp": raw_bucket_counts.get("comp", 0),
        "Insufficient": sum(
            raw_bucket_counts.get(bucket, 0)
            for bucket in ("insufficient", "ghost", "screened_out")
        ),
        "Hard Denied": raw_bucket_counts.get("reject", 0),
    }
    processing_metrics = st.columns(3)
    for column, (label, value) in zip(
        processing_metrics,
        [
            ("Crawled websites sent to screening", pipeline.get(
                "in_screening_registry",
                0,
            )),
            ("Waiting for a decision", pipeline.get("pending", 0)),
            ("Classified into a bucket", sum(raw_bucket_counts.values())),
        ],
    ):
        column.metric(label, f"{int(value):,}")
    bucket_metrics = st.columns(len(bucket_counts))
    for column, (label, value) in zip(
        bucket_metrics,
        bucket_counts.items(),
    ):
        column.metric(label, f"{int(value):,}")

    st.info(
        "Screening happens automatically after a website is saved. It uses the "
        "locally retained HTML, so the same website is not downloaded twice. "
        "Registration dates are then added in the background for Investable, "
        "Coming Soon, Needs Review, and Potential Comp companies."
    )
    st.caption(
        f"Registration enrichment: {stats['registration_dates']:,} dates "
        f"saved · {stats['registration_queued']:,} candidates waiting. "
        "The lookup runs separately at a conservative rate, so it does not "
        "slow website crawling."
    )
    with st.expander("What do these buckets mean?"):
        st.markdown(
            """
- **Investable:** thesis-aligned real product with a clear wedge and a plausible moat, technical edge, or exceptional frontier team; also enters the main Yes list.
- **Coming Soon:** credible company or product that has not fully launched.
- **Needs Review:** thesis-aligned product with a credible wedge or moat signal, but one missing conviction dimension.
- **Potential Comp:** relevant but already mature, scaled, acquired, or overfunded.
- **Insufficient Evidence:** a real or reachable site without enough evidence.
- **Hard Denied:** explicit disqualifier, parked/template site, or clear mismatch; also enters the main No list.
            """
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
                st.info("No crawled companies currently appear in this bucket.")
                continue
            sort_col, rows_col, status_col = st.columns([2.5, 1, 2])
            sort_choice = sort_col.selectbox(
                "Sort companies",
                [
                    "Priority: high to low",
                    "Fit: high to low",
                    "Confidence: high to low",
                    "Recently classified",
                    "Company: A to Z",
                    "Domain: A to Z",
                ],
                key=f"zone_bucket_sort_{bucket_name}",
            )
            page_size = rows_col.selectbox(
                "Rows",
                [25, 50, 100, 200],
                index=1,
                key=f"zone_bucket_rows_{bucket_name}",
            )
            page_key = f"zone_bucket_page_{bucket_name}"
            current_page = int(st.session_state.get(page_key, 1))
            maximum_page = max(
                1,
                (total_rows + int(page_size) - 1) // int(page_size),
            )
            current_page = min(current_page, maximum_page)
            st.session_state[page_key] = current_page
            status_col.caption(
                f"{total_rows:,} companies · page {current_page:,} "
                f"of {maximum_page:,}"
            )
            rows = query_corpus_classifications(
                DATABASE,
                routing_buckets,
                limit=int(page_size),
                offset=(current_page - 1) * int(page_size),
                sort=sort_choice,
            )
            selected_bucket = pd.DataFrame(rows)
            if selected_bucket.empty:
                st.info("No companies appear on this page.")
                continue
            registered, registrars = registration_columns(
                selected_bucket["domain"].tolist()
            )
            bucket_table = pd.DataFrame({
                "Select": False,
                "Company": selected_bucket["company_name"],
                "Domain": selected_bucket["domain"],
                "Registered": registered,
                "Registrar": registrars,
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
            edited_bucket = st.data_editor(
                bucket_table,
                hide_index=True,
                width="stretch",
                height=min(760, 45 + len(bucket_table) * 35),
                disabled=[
                    "Company",
                    "Domain",
                    "Registered",
                    "Registrar",
                    "Availability",
                    "Fit",
                    "Venture",
                    "Wedge",
                    "Moat",
                    "Founders",
                    "Confidence",
                    "Priority",
                    "Description",
                    "Reason",
                    "Website",
                ],
                column_config={
                    "Select": st.column_config.CheckboxColumn("Move"),
                    "Fit": st.column_config.ProgressColumn(
                        "Glasswing fit",
                        min_value=0,
                        max_value=100,
                        format="%d",
                        help="Rule-based match to the Glasswing investment thesis.",
                    ),
                    "Venture": st.column_config.ProgressColumn(
                        "Venture quality",
                        min_value=0,
                        max_value=100,
                        format="%d",
                        help="Balanced thesis, product, wedge, moat, and team score.",
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
                        "Evidence confidence",
                        min_value=0,
                        max_value=100,
                        format="%d",
                        help="Strength and completeness of website evidence.",
                    ),
                    "Priority": st.column_config.ProgressColumn(
                        "Review priority",
                        min_value=0,
                        max_value=100,
                        format="%d",
                        help="Combined score used to order human review.",
                    ),
                    "Website": st.column_config.LinkColumn(
                        "Website",
                        display_text="Open website",
                    ),
                },
                key=f"zone_bucket_editor_{bucket_name}",
            )
            action_col, rdap_col, button_col = st.columns([2.5, 1.4, 1])
            destination = action_col.selectbox(
                "Move selected to",
                [
                    "Needs Review",
                    "Investable Candidate",
                    "Potential Comp",
                    "Insufficient Evidence",
                    "Hard Denied",
                    "Coming Soon",
                ],
                key=f"zone_bucket_destination_{bucket_name}",
            )
            rdap_col.write("")
            lookup_clicked = rdap_col.button(
                "Get registration dates",
                width="stretch",
                key=f"zone_bucket_rdap_{bucket_name}",
                help=(
                    "Select up to 10 companies. Dates are cached after the "
                    "authoritative registry lookup."
                ),
            )
            button_col.write("")
            move_clicked = button_col.button(
                "Move selected",
                type="primary",
                width="stretch",
                key=f"zone_bucket_move_{bucket_name}",
            )
            if lookup_clicked:
                chosen = edited_bucket[edited_bucket["Select"]].copy()
                if chosen.empty:
                    st.warning("Select at least one company first.")
                elif len(chosen) > 10:
                    st.warning("Select at most 10 companies per lookup.")
                else:
                    with st.spinner("Querying registry RDAP services…"):
                        found, errors = enrich_registration_dates(
                            chosen["Domain"].tolist()
                        )
                    if found:
                        st.success(f"Saved {found} registration date(s).")
                    if errors:
                        st.warning("\n".join(errors))
                    if found:
                        st.rerun()
            if move_clicked:
                chosen = edited_bucket[edited_bucket["Select"]].copy()
                if chosen.empty:
                    st.warning("Select at least one company first.")
                else:
                    moved = move_zone_domains(chosen, destination)
                    st.success(
                        f"Moved {moved} compan"
                        f"{'y' if moved == 1 else 'ies'} to {destination}."
                    )
                    st.rerun()
            previous, next_col, _ = st.columns([1, 1, 6])
            if previous.button(
                "← Previous",
                disabled=current_page <= 1,
                key=f"zone_bucket_previous_{bucket_name}",
            ):
                st.session_state[page_key] = current_page - 1
                st.rerun()
            if next_col.button(
                "Next →",
                disabled=current_page >= maximum_page,
                key=f"zone_bucket_next_{bucket_name}",
            ):
                st.session_state[page_key] = current_page + 1
                st.rerun()

with tabs[2]:
    st.subheader("System status")
    st.caption(
        "Follow the pipeline from downloaded domain list → filtered queue → "
        "website crawl → searchable HTML → company bucket."
    )
    st.markdown("#### Domain-list and crawl progress")
    if progress_rows:
        progress_display = []
        for row in progress_rows:
            importing = str(row["import_status"]) == "importing"
            discovered = int(row["discovered_domains"] or 0)
            attempted = int(row["attempted"] or 0)
            successful = int(row["successful"] or 0)
            skipped = int(row["rejected_gibberish"] or 0)
            quarantined = int(
                row["low_priority_quarantined"] or 0
            )
            processed = attempted + quarantined
            progress_display.append({
                "Domain list": f".{row['tld']}",
                "Current phase": (
                    "Filtering names and building queues"
                    if importing
                    else "Website crawling"
                    if row["import_status"] == "ready"
                    else str(row["import_status"]).replace("_", " ").title()
                ),
                "Kept domains": (
                    "Counting…" if importing else f"{discovered:,}"
                ),
                "Skipped names": (
                    "Counting…" if importing else f"{skipped:,}"
                ),
                "Checked in current queue": (
                    "Waiting for import"
                    if importing
                    else f"{attempted:,}"
                ),
                "Cold names bypassed": (
                    "Waiting for import"
                    if importing
                    else f"{quarantined:,}"
                ),
                "Saved from current queue": (
                    "Waiting for import"
                    if importing
                    else f"{successful:,}"
                ),
                "Current queue progress": (
                    "Not started"
                    if importing
                    else f"{processed / max(discovered, 1):.2%}"
                ),
            })
        st.dataframe(
            pd.DataFrame(progress_display),
            hide_index=True,
            width="stretch",
        )
    else:
        st.info("No domain list has been imported yet.")

    recent_rate = float(
        state.get("last_batch_domains_per_second", "0") or 0
    )
    remaining_domains = max(
        0,
        int(stats["queued_domains"])
        - int(stats["attempted"])
        - int(stats["low_priority_quarantined"]),
    )
    eta_seconds = (
        remaining_domains / recent_rate if recent_rate > 0 else 0
    )
    free_gb = shutil.disk_usage(PROJECT).free / (1024 ** 3)
    st.markdown("#### Speed and storage")
    performance_metrics = st.columns(6)
    for column, (label, value) in zip(
        performance_metrics,
        [
            ("Recent crawl speed", f"{recent_rate:.1f} domains/sec"),
            (
                "Ready-queue ETA",
                human_duration(eta_seconds) if eta_seconds else "Waiting",
            ),
            ("HTML documents saved", stats["documents"]),
            ("Temporary failures to retry", stats["retries_pending"]),
            ("Free disk", f"{free_gb:.1f} GB"),
            ("Stored HTML", human_bytes(stats["compressed_html_bytes"])),
        ],
    ):
        column.metric(
            label,
            f"{value:,}" if isinstance(value, int) else value,
        )
    if active_stage.startswith("importing_com"):
        st.warning(
            f"The storage forecast currently covers .app only. The .com "
            f"forecast will be calculated after its domain-list import "
            f"finishes. Free disk: {available_gb:.1f} GB."
        )
    elif projected_remaining_gb > safe_archive_gb:
        st.error(
            f"The ready crawl queues project approximately "
            f"{projected_remaining_gb:.1f} GB of additional compressed HTML, "
            f"but only {safe_archive_gb:.1f} GB is available above the safety "
            "reserve. Move the HTML archive to a larger disk."
        )
    else:
        st.success(
            "The currently ready queues fit the estimated safe storage budget."
        )

    st.markdown("#### Processing controls")
    st.caption(
        "Start resumes from saved checkpoints. Stop finishes the current safe "
        "operation before exiting, so no completed work is discarded."
    )
    start_col, stop_col, state_col = st.columns([1, 1, 4])
    if start_col.button(
        "Start / resume",
        type="primary",
        disabled=(RUNTIME_MODE != "local") or bool(worker_running),
        width="stretch",
    ):
        if RUNTIME_MODE != "local":
            st.error(
                "Process controls are delegated to the cloud runtime. Use "
                "the cloud platform's worker controls instead."
            )
        else:
            st.success(worker_action("start_worker.sh"))
        st.rerun()
    if stop_col.button(
        "Stop safely",
        disabled=(RUNTIME_MODE != "local") or not worker_running,
        width="stretch",
    ):
        if RUNTIME_MODE != "local":
            st.error(
                "Process controls are delegated to the cloud runtime. Use "
                "the cloud platform's worker controls instead."
            )
        elif process_alive(PID_FILE):
            st.info(worker_action("stop_worker.sh"))
        elif worker_pid is not None:
            os.kill(worker_pid, signal.SIGTERM)
            st.info(
                "Stop requested. The active domain-list import will finish "
                "its atomic save before the process exits."
            )
        st.rerun()
    if RUNTIME_MODE != "local":
        state_col.info(
            f"**{stage_title}**\n\n"
            "Cloud-managed (Railway) -- liveness isn't visible from here; "
            "the stage above reflects the worker's own last report."
        )
    else:
        state_col.info(
            f"**{stage_title}**\n\n"
            + (
                f"Process {worker_pid} is active."
                if worker_running
                else "No processing process is running."
            )
        )

    zone_files = discover_zonefiles()
    with st.expander("Advanced: manually import a downloaded domain list"):
        st.warning(
            "Normally you do not need this. The worker automatically imports "
            "completed CZDS downloads. Use this only to rebuild a queue or run "
            "a smaller pilot."
        )
        if not zone_files:
            st.info(f"No zone files were found under `{ZONEFILES}`.")
        else:
            with st.form("zone_import_form"):
                selected_path = st.selectbox(
                    "Downloaded file",
                    zone_files,
                    format_func=lambda path: (
                        f"{path.name} · {file_size(path)}"
                    ),
                )
                inferred_tld = selected_path.name.split(".", 1)[0].lower()
                tld = st.text_input("Domain ending", value=inferred_tld)
                import_mode = st.radio(
                    "How much to import",
                    ["100,000-domain test", "Complete domain list"],
                    horizontal=True,
                )
                import_clicked = st.form_submit_button(
                    "Build a new crawl queue",
                    type="primary",
                    disabled=active_stage.startswith("importing_"),
                )
            if import_clicked:
                try:
                    pid = launch_import(
                        selected_path,
                        tld.strip().lower().lstrip("."),
                        limit=(
                            100_000
                            if import_mode == "100,000-domain test"
                            else 0
                        ),
                    )
                    st.success(
                        f"Queue build started as process {pid}. Refresh this "
                        "page to monitor it."
                    )
                except Exception as exc:
                    st.error(str(exc))

    with st.expander("Technical details"):
        st.code(
            "\n".join(
                f"{key}: {value}"
                for key, value in sorted(state.items())
            )
            or "Worker has not run yet.",
            language="text",
        )
        if WORKER_LOG.exists():
            st.caption("Recent worker log")
            st.code(
                "\n".join(
                    WORKER_LOG.read_text(
                        encoding="utf-8",
                        errors="replace",
                    ).splitlines()[-80:]
                ),
                language="text",
            )

with tabs[2]:
    st.divider()
    st.subheader("Domains skipped as obvious junk")
    st.write(
        "Before crawling, the importer skips names with several strong signs "
        "of machine-generated noise—for example long random character strings "
        "or numeric spam. It deliberately keeps unusual short brands, acronyms, "
        "numbered companies, hyphenated names, and internationalized domains."
    )
    st.info(
        "Nothing is deleted from the original ICANN files. Skipped names are "
        "left out of crawl queues and written to an audit file so a false "
        "positive can be recovered."
    )
    completed_names = (
        int(stats["queued_domains"]) + int(stats["gibberish_rejected"])
    )
    hygiene_metrics = st.columns(4)
    hygiene_metrics[0].metric(
        "Names processed",
        f"{completed_names:,}",
    )
    hygiene_metrics[1].metric(
        "Kept for crawling",
        f"{stats['queued_domains']:,}",
    )
    hygiene_metrics[2].metric(
        "Skipped as likely junk",
        f"{stats['gibberish_rejected']:,}",
    )
    hygiene_metrics[3].metric(
        "Cold names bypassed",
        f"{stats['low_priority_quarantined']:,}",
        help=(
            "Plausible but low-value names retained in the zone queue and "
            "skipped before DNS/HTTP. Newly registered names bypass this gate."
        ),
    )
    st.caption(
        f"{completed_names:,} processed = {stats['queued_domains']:,} kept + "
        f"{stats['gibberish_rejected']:,} skipped. Filter setting: conservative."
    )
    st.caption(
        "Active crawl policy: priority "
        f"0–{settings.max_crawl_priority}. Lower-value priority 3 names stay "
        "recoverable in the source queue but do not consume DNS or HTTP."
    )
    if active_stage.startswith("importing_com"):
        st.caption(
            "The numbers above currently cover completed imports only. The "
            "live .com exclusions will be added when that import finishes."
        )
    audit_root = PROJECT / "data" / "rejected"
    audit_files = (
        sorted(
            list(audit_root.glob("*.tsv"))
            + list(audit_root.glob("*.tsv.gz")),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if audit_root.exists()
        else []
    )
    if not audit_files:
        st.info(
            "No skipped-domain audit exists yet."
        )
    else:
        selected_audit = st.selectbox(
            "Skipped-domain audit file",
            audit_files,
            format_func=lambda path: f"{path.name} · {file_size(path)}",
        )
        try:
            audit_preview = pd.read_csv(
                selected_audit,
                sep="\t",
                dtype=str,
                keep_default_na=False,
                nrows=500,
            )
        except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError):
            audit_preview = pd.DataFrame()
        st.caption(
            f"Showing the first {len(audit_preview):,} skipped names. Full file: "
            f"`{selected_audit}`"
        )
        if not audit_preview.empty:
            st.dataframe(
                audit_preview,
                hide_index=True,
                width="stretch",
                height=600,
            )

with tabs[3]:
    st.subheader("Setup and advanced controls")
    st.caption(
        "You only need this section when renewing access, downloading a fresh "
        "daily zone file, or setting up the project on another computer."
    )
    czds_client = PROJECT / "czds.py"
    if czds_client.exists():
        st.success(f"CZDS client found: {czds_client}")
    else:
        st.error(
            "The attachment included only the private repository README. "
            f"Copy the repository's actual `czds.py` into `{PROJECT}`."
        )
    st.markdown(
        """
1. Create an account in the ICANN CZDS portal.
2. Accept its Terms and Conditions.
3. Request access to `.app` and `.com` in the portal. Approval is per registry.
4. Copy the private repository's `czds.py` into the Zone Universe folder.
5. In Terminal, run:
        """
    )
    st.code(
        f"""cd "{PROJECT}"
export CZDS_USERNAME='your-email@example.com'
chmod +x czds.py
./czds.py store-credentials
./czds.py auth
./czds.py list
./czds.py status app com
./czds.py download app com -o data/zonefiles

export PYTHONPATH="$PWD/src:../ai-domain-search-engine/src"
../.venv/bin/python -m zone_universe.cli init
../.venv/bin/python -m zone_universe.cli import-zone \\
  data/zonefiles/app.txt.gz --tld app
../.venv/bin/python -m zone_universe.cli import-zone \\
  data/zonefiles/com.txt.gz --tld com --limit 100000
./start_worker.sh""",
        language="zsh",
    )
    st.caption(
        "CZDS access is governed by ICANN and each registry’s agreement. "
        "Do not redistribute zone files or use them outside the approved "
        "purpose. ICANN states approved zone data can generally be downloaded "
        "at most once per 24 hours."
    )

with tabs[3]:
    st.divider()
    st.subheader("Downloaded files and storage")
    inventory = pd.DataFrame(zone_rows)
    if not inventory.empty:
        inventory["Domain ending"] = "." + inventory["tld"].astype(str)
        inventory["Status"] = inventory["import_status"].map({
            "ready": "Ready",
            "importing": "Importing now",
            "error": "Error",
            "not_imported": "Not imported",
        }).fillna(inventory["import_status"])
        inventory = inventory.rename(columns={
            "discovered_domains": "Domains ready",
            "added_domains": "New since prior file",
            "removed_domains": "Removed since prior file",
            "imported_at": "Last completed import",
        })
        st.dataframe(
            inventory[
                [
                    "Domain ending",
                    "Status",
                    "Domains ready",
                    "New since prior file",
                    "Removed since prior file",
                    "Last completed import",
                ]
            ],
            hide_index=True,
            width="stretch",
        )
        st.caption(
            "On every zone refresh, newly delegated domains enter priority "
            "zero and crawl before the older inventory. Added/removed gzip "
            "files are retained for audit."
        )
    files = pd.DataFrame([
        {
            "File": path.name,
            "Size": file_size(path),
            "Modified": pd.Timestamp(
                path.stat().st_mtime,
                unit="s",
            ).isoformat(),
            "Path": str(path),
        }
        for path in discover_zonefiles()
    ])
    if not files.empty:
        st.dataframe(files, hide_index=True, width="stretch")
    st.markdown("#### HTML storage")
    st.write(
        f"Saved website HTML currently uses "
        f"**{human_bytes(stats['compressed_html_bytes'])}**. The archive is "
        "compressed and exact duplicates share storage. Processing pauses "
        f"before free space falls below {settings.minimum_free_gb:g} GB."
    )
    st.code(str(settings.html_archive), language="text")
    with st.expander("Technical inventory and delta-file paths"):
        technical_columns = [
            column
            for column in (
                "tld",
                "snapshot_path",
                "delta_added_path",
                "delta_removed_path",
                "source_fingerprint",
            )
            if column in inventory.columns
        ]
        if technical_columns:
            st.dataframe(
                inventory[technical_columns],
                hide_index=True,
                width="stretch",
            )
