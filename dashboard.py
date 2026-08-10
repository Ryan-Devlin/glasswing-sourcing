#!/usr/bin/env python3
from __future__ import annotations
import os
import re

import json
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from classification_store import (
    classification_source_label,
    frame_from_store,
)
from common import (
    FILTERED, LLM, MANUAL, NO_BASKET_DATA, YES_BASKET_DATA,
    ROOT, RUN_LOCK, RUN_LOG, RUN_STATUS,
    QUEUE_DB, RUN_CONFIG, STREAM_PID, STREAM_STATUS, STREAM_STOP,
    atomic_write_json, read_basket_data, remove_basket_entry,
    save_basket_entries, save_basket_entry, seed_yes_basket,
    stable_domain,
)
from filter import (
    enterprise_rescue_mask,
    investment_eligibility_adjusted_score,
    score_text,
)
from glasswing_portfolio import GLASSWING_PORTFOLIO_DOMAINS
from runtime_control import (
    RUNTIME_MODE,
    control_backend_available,
    publish_pipeline_config,
    publish_pipeline_status,
    read_pipeline_config,
    read_pipeline_status,
    request_services,
    runtime_status,
    shared_pipeline_backend_available,
)
from ui_theme import render_card_grid, render_hideable_table
from training_data import (
    backfill_label_metadata, balanced_instruction_frame,
    build_active_review_queue, build_training_dataset, write_training_exports,
)

load_dotenv(ROOT / ".env")

# Seed verified Glasswing investments as ground-truth positives. Existing basket
# labels and human-edited ratings always win, so reruns remain safe and editable.
seed_yes_basket(GLASSWING_PORTFOLIO_DOMAINS, rating=100)
backfill_label_metadata()

st.markdown(
    """
    <style>
        .stDeployButton {
            display: none !important;
        }
        .stApp {
            margin-top: 0 !important;
        }
        .block-container {
            padding-top: .65rem !important;
            padding-bottom: 2rem;
            max-width: 1400px;
        }
        .glasswing-header {
            margin-bottom: .75rem;
        }
        .glasswing-title {
            font-size: 2.15rem;
            line-height: 1.1;
            font-weight: 760;
            letter-spacing: -.04em;
            margin: 0;
        }
        .glasswing-subtitle {
            margin: .4rem 0 0 0;
            opacity: .68;
            font-size: 1rem;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(128,128,128,.16);
            border-radius: 12px;
            overflow: hidden;
        }
        div[data-baseweb="select"] > div {
            min-height: 42px;
            border-radius: 10px;
        }
        .status-card {
            border: 1px solid rgba(128,128,128,.18);
            border-radius: 12px;
            padding: .8rem 1rem;
            min-height: 44px;
        }
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)


api_key_present = bool(os.getenv("ANTHROPIC_API_KEY", "").strip())
with st.expander("API key status", expanded=not api_key_present):
    if api_key_present:
        st.success("Anthropic API key is configured.")
    else:
        st.error(
            "No Anthropic API key is configured. Add ANTHROPIC_API_KEY to the .env file, "
            "then restart the dashboard."
        )

@st.cache_data(show_spinner=False, max_entries=32)
def _read_csv_version(
    path_string: str,
    modified_ns: int,
    size: int,
) -> pd.DataFrame:
    """Cache one immutable on-disk CSV version."""
    del modified_ns, size
    return pd.read_csv(path_string, dtype=str).fillna("")


def read_csv_safely(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    snapshot_key = f"csv_snapshot::{path.resolve()}"
    try:
        stat = path.stat()
        frame = _read_csv_version(
            str(path.resolve()),
            stat.st_mtime_ns,
            stat.st_size,
        )
        st.session_state[snapshot_key] = frame
        return frame.copy()
    except (
        OSError,
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
    ):
        previous = st.session_state.get(snapshot_key)
        if isinstance(previous, pd.DataFrame):
            return previous.copy()
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=15, max_entries=2)
def read_screened_classifications() -> pd.DataFrame:
    """Read the canonical cross-service store, not a container-local CSV."""
    return frame_from_store()


def read_status() -> dict:
    if not RUN_STATUS.exists():
        return {"running": False, "stage": "idle"}
    try:
        return json.loads(RUN_STATUS.read_text(encoding="utf-8"))
    except Exception:
        return {"running": RUN_LOCK.exists(), "stage": "unknown"}

INDUSTRY_OPTIONS = {
    "Vertical AI": "vertical_ai",
    "Physical AI": "physical_ai",
    "AI Infrastructure": "ai_adaptive_infrastructure",
    "Cybersecurity": "intelligent_enterprise_defense",
    "Collaborative Intelligence": "collaborative_intelligence",
    "Next-Generation Compute": "next_gen_compute",
}

EXTENSION_OPTIONS = {
    ".ai": "AI-native companies",
    ".io": "Technology startups",
    ".app": "Application products",
    ".dev": "Developer tools",
    ".co": "General startups",
    ".xyz": "Emerging technology",
    ".tech": "Technology companies",
    ".com": "Maximum coverage",
}

EXTENSION_PRESETS = {
    "Focused AI & Developer": [
        ".ai",
        ".io",
        ".app",
        ".dev",
    ],
    "Startup Broad": [
        ".ai",
        ".io",
        ".app",
        ".dev",
        ".co",
        ".xyz",
        ".tech",
    ],
    "Maximum Coverage": [
        ".ai",
        ".io",
        ".app",
        ".dev",
        ".co",
        ".xyz",
        ".tech",
        ".com",
    ],
    "Custom": [],
}


DEFAULT_RUN_CONFIG = {
    "selected_extensions": [".ai", ".app", ".dev"],
    "selected_industries": list(INDUSTRY_OPTIONS.values()),
    "include_keywords": [],
    "exclude_keywords": [],
    "keyword_mode": "any",
    "minimum_score": 55,
    "minimum_signal_dimensions": 2,
    "screening_batch_size": 75,
    "scraper_workers": 28,
    "scrape_timeout": 6,
    "require_us_evidence": False,
    "exceptional_score": 84,
}


def load_dashboard_run_config() -> dict:
    loaded: dict = {}
    if shared_pipeline_backend_available():
        try:
            loaded = read_pipeline_config()
        except Exception:
            loaded = {}
    if not loaded and RUN_CONFIG.exists():
        try:
            loaded = json.loads(RUN_CONFIG.read_text(encoding="utf-8"))
        except Exception:
            loaded = {}
    result = dict(DEFAULT_RUN_CONFIG)
    result.update(loaded)
    return result


def save_dashboard_run_config(config: dict) -> None:
    atomic_write_json(RUN_CONFIG, config)
    publish_pipeline_config(config, requested_by="dashboard")


def parse_keyword_text(value: str) -> list[str]:
    return [
        item.strip()
        for item in re.split(r"[,;\n]+", value or "")
        if item.strip()
    ]


def process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, ValueError):
        return False


def read_stream_status() -> dict:
    if shared_pipeline_backend_available():
        try:
            shared = read_pipeline_status()
        except Exception as exc:
            return {
                "running": False,
                "stage": "status unavailable",
                "queue": {},
                "shared_status_error": f"{type(exc).__name__}: {exc}",
            }
        if shared:
            return shared
        return {
            "running": False,
            "stage": "waiting for worker status",
            "queue": {},
            "shared_status_waiting": True,
        }
    if not STREAM_STATUS.exists():
        return {"running": False, "stage": "stopped", "queue": {}}
    try:
        status = json.loads(
            STREAM_STATUS.read_text(encoding="utf-8")
        )
    except Exception:
        status = {
            "running": STREAM_PID.exists(),
            "stage": "unknown",
            "queue": {},
        }

    pid = int(status.get("pid", 0) or 0)
    if status.get("running") and pid > 0 and not process_is_alive(pid):
        status["running"] = False
        status["stage"] = "crashed"
        status["last_error"] = (
            status.get("last_error")
            or "Background process exited unexpectedly. Open Run Details or run ./diagnose.sh."
        )
        try:
            atomic_write_json(STREAM_STATUS, status)
        except Exception:
            pass
    return status

def start_continuous_pipeline(config: dict) -> None:
    if RUNTIME_MODE != "local":
        if not control_backend_available():
            st.error(
                "No shared control store is configured for cloud mode -- "
                "set RUNTIME_CONTROL_DATABASE_URL to enable this."
            )
            return
        # The worker container is always up. Persist both the settings and
        # desired state in Postgres so the separate service sees this exact
        # dashboard request.
        save_dashboard_run_config(config)
        request_services({"pipeline": True}, requested_by="dashboard_start")
        st.success(
            "Resume requested. The pipeline-worker service will pick this "
            "up within a few seconds."
        )
        st.rerun()
        return
    status = read_stream_status()
    stale_pid = 0
    if STREAM_PID.exists():
        try:
            stale_pid = int(
                STREAM_PID.read_text(encoding="utf-8").strip()
            )
        except Exception:
            stale_pid = 0

    if status.get("running") or process_is_alive(stale_pid):
        st.warning("The continuous pipeline is already running.")
        return

    STREAM_PID.unlink(missing_ok=True)
    STREAM_STOP.unlink(missing_ok=True)
    save_dashboard_run_config(config)

    log_path = ROOT / "logs" / "continuous_pipeline.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("a", encoding="utf-8")
    try:
        process = subprocess.Popen(
            [sys.executable, str(ROOT / "continuous_pipeline.py")],
            cwd=ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as exc:
        log_handle.close()
        st.error(f"Could not start pipeline: {type(exc).__name__}: {exc}")
        return
    finally:
        try:
            log_handle.close()
        except Exception:
            pass

    # Write an optimistic startup state immediately. The old build reran the
    # dashboard before the child process wrote status, making a healthy start
    # look like an instant stop.
    startup_status = {
        "running": True,
        "pid": process.pid,
        "stage": "starting",
        "started_at": pd.Timestamp.utcnow().isoformat(),
        "updated_at": pd.Timestamp.utcnow().isoformat(),
        "selected_extensions": config.get(
            "selected_extensions",
            [".ai"],
        ),
        "new_domains_received": 0,
        "domains_received": 0,
        "screened": 0,
        "repeat_events_ignored": 0,
        "failed_this_run": 0,
        "claude_calls": 0,
        "last_error": "",
        "queue": {},
    }
    atomic_write_json(STREAM_STATUS, startup_status)
    st.success("Continuous pipeline is starting.")
    st.rerun()

def stop_continuous_pipeline() -> None:
    if RUNTIME_MODE != "local":
        if not control_backend_available():
            st.error(
                "No shared control store is configured for cloud mode -- "
                "set RUNTIME_CONTROL_DATABASE_URL to enable this."
            )
            return
        request_services({"pipeline": False}, requested_by="dashboard_stop")
        st.info(
            "Pause requested. The pipeline-worker service will stop "
            "claiming new batches within a few seconds -- the CertStream "
            "listener stays connected so no events are missed."
        )
        st.rerun()
        return
    status = read_stream_status()
    if not status.get("running") and not STREAM_PID.exists():
        st.info("The continuous pipeline is already stopped.")
        return
    STREAM_STOP.write_text("stop", encoding="utf-8")
    st.success(
        "Stop requested. The current batch will finish before shutdown."
    )
    st.rerun()


def reset_current_run() -> None:
    """Stop active work and clear only unfinished current-run state."""
    if RUNTIME_MODE != "local":
        st.error(
            "Process controls are delegated to the cloud runtime. Use the "
            "cloud platform's worker controls instead."
        )
        return
    status = read_stream_status()
    pid = int(status.get("pid", 0) or 0)

    STREAM_STOP.write_text("stop", encoding="utf-8")
    if pid > 0:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass

    deadline = time.time() + 4
    while pid > 0 and time.time() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            break
        except PermissionError:
            break

    QUEUE_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(QUEUE_DB, timeout=30) as conn:
        conn.execute(
            """
            UPDATE domains
            SET status='discarded',
                next_attempt_at=NULL,
                last_error='current_run_reset'
            WHERE status IN ('pending', 'processing')
            """
        )
        conn.commit()

    reset_status = {
        "running": False,
        "stage": "reset",
        "run_id": "",
        "run_started_at": "",
        "certificate_events_received": 0,
        "new_domains_received": 0,
        "repeat_events_ignored": 0,
        "domains_received": 0,
        "processed": 0,
        "screened": 0,
        "failed_this_run": 0,
        "claude_calls": 0,
        "automatic_claude_calls": 0,
        "manual_claude_calls": 0,
        "received_by_extension": {},
        "repeats_by_extension": {},
        "current_batch": [],
        "last_domain_received": "",
        "last_domain_processed": "",
        "last_error": "",
        "reset_at": pd.Timestamp.utcnow().isoformat(),
    }
    atomic_write_json(STREAM_STATUS, reset_status)
    STREAM_PID.unlink(missing_ok=True)
    STREAM_STOP.unlink(missing_ok=True)
    st.session_state["confirm_reset_run"] = False
    st.success("Current run reset. Saved company history was preserved.")
    st.rerun()



def update_manual_claude_status(
    *,
    increment: int = 0,
    result: dict | None = None,
    error: str = "",
) -> None:
    status = read_stream_status()
    automatic_calls = int(status.get("automatic_claude_calls", 0) or 0)
    legacy_calls = int(status.get("claude_calls", 0) or 0)
    manual_calls = int(status.get("manual_claude_calls", 0) or 0)

    # Older status files only have claude_calls. Preserve that value as the
    # automatic baseline until the background service writes the explicit key.
    if "automatic_claude_calls" not in status:
        automatic_calls = max(automatic_calls, legacy_calls - manual_calls)

    manual_calls += max(0, int(increment))
    status["automatic_claude_calls"] = automatic_calls
    status["manual_claude_calls"] = manual_calls
    status["claude_calls"] = automatic_calls + manual_calls
    status["last_manual_claude_error"] = error[:800]
    if result is not None:
        status["last_manual_claude_result"] = result
    atomic_write_json(STREAM_STATUS, status)
    publish_pipeline_status(status)


def find_latest_claude_result(domain: str) -> dict | None:
    target = stable_domain(domain)
    for path in (
        LLM / "llm_approved_master.csv",
        LLM / "llm_denied_master.csv",
    ):
        frame = read_csv_safely(path)
        if frame.empty or "domain" not in frame.columns:
            continue
        matches = frame[
            frame["domain"].map(stable_domain).eq(target)
        ]
        if matches.empty:
            continue
        row = matches.iloc[-1].to_dict()
        return {
            "domain": target,
            "decision": str(row.get("llm_decision", "")),
            "score": str(row.get("llm_fit_score", "")),
            "summary": str(row.get("llm_product_summary", "")),
            "reason": (
                str(row.get("llm_reasoning", "")).strip()
                or str(row.get("llm_concerns", "")).strip()
                or str(row.get("ownership_evidence", "")).strip()
                or "Claude provided no written reason"
            ),
            "concern": (
                str(row.get("llm_concerns", "")).strip()
                or str(row.get("llm_reasoning", "")).strip()
                or "No additional concern recorded"
            ),
            "model": str(row.get("llm_model", "")),
        }
    return None


def run_manual_diligence(row: pd.Series) -> tuple[bool, str]:
    """Force exactly one selected company through Claude and verify output."""
    if not api_key_present:
        return False, "Anthropic API key is not configured."

    manual_dir = ROOT / "data" / "manual"
    manual_dir.mkdir(parents=True, exist_ok=True)
    domain = stable_domain(
        str(row.get("domain", "selected-company"))
    ) or "selected-company"
    safe_name = re.sub(r"[^a-zA-Z0-9._-]+", "_", domain)
    input_path = manual_dir / f"{safe_name}_manual_diligence.csv"

    selected = row.to_dict()
    selected["domain"] = domain

    # Manual means manual: bypass all automatic queue gates.
    selected["qualified_for_llm"] = "true"
    if str(selected.get("location_status", "")).upper() not in {
        "US",
        "UNKNOWN",
    }:
        selected["location_status"] = "UNKNOWN"

    pd.DataFrame([selected]).to_csv(input_path, index=False)

    before = find_latest_claude_result(domain)

    command = [
        sys.executable,
        str(ROOT / "claude_classifier.py"),
        str(input_path),
        "--reclassify",
        "--limit", "1",
        "--minimum-score", "0",
        "--percentage-cap", "1",
        "--max-calls", "1",
        "--exceptional-score", "0",
        "--workers", "1",
    ]

    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=240,
        )
    except subprocess.TimeoutExpired:
        message = "Claude diligence timed out after 240 seconds."
        update_manual_claude_status(error=message)
        return False, message
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        update_manual_claude_status(error=message)
        return False, message

    combined_output = "\n".join(
        part.strip()
        for part in (result.stdout, result.stderr)
        if part and part.strip()
    )

    if result.returncode != 0:
        detail = combined_output or "Claude classifier exited with no details."
        update_manual_claude_status(error=detail[-800:])
        return False, detail[-1200:]

    after = find_latest_claude_result(domain)
    if after is None:
        detail = (
            combined_output
            or "Classifier finished but did not write a result."
        )
        update_manual_claude_status(error=detail[-800:])
        return False, (
            "Claude did not process the selected company. "
            f"Classifier output: {detail[-800:]}"
        )

    # Ensure this was a real selected-company result, not a silent zero-row run.
    if "Web diligence searches: 1" not in combined_output:
        update_manual_claude_status(
            error=combined_output[-800:],
        )
        return False, (
            "Claude selected zero companies, so no API call was made. "
            f"Classifier output: {combined_output[-800:]}"
        )

    update_manual_claude_status(
        increment=1,
        result=after,
        error="",
    )

    decision = after.get("decision") or "UNKNOWN"
    score = after.get("score") or "N/A"
    summary = after.get("summary") or domain
    return True, (
        f"Claude completed: {decision} · score {score} · {summary}"
    )



OVERRIDES_PATH = MANUAL / "domain_overrides.csv"

def load_overrides() -> pd.DataFrame:
    return read_csv_safely(OVERRIDES_PATH)

def apply_overrides(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if result.empty or "domain" not in result.columns:
        if "_override_target" not in result.columns:
            result["_override_target"] = ""
        return result
    overrides = load_overrides()
    mapping = (
        dict(zip(
            overrides["domain"].map(stable_domain),
            overrides["override_target"].astype(str),
        ))
        if not overrides.empty
        else {}
    )
    result["_override_target"] = (
        result["domain"].map(stable_domain).map(mapping).fillna("")
    )
    basket_mapping: dict[str, str] = {}
    for path, basket in (
        (YES_BASKET_DATA, "yes"),
        (NO_BASKET_DATA, "no"),
    ):
        labels = read_basket_data(path)
        if not labels.empty:
            basket_mapping.update({
                stable_domain(url): basket
                for url in labels["domain_url"]
            })
    normalized_domains = result["domain"].map(stable_domain)
    basket_labels = normalized_domains.map(basket_mapping).fillna("")
    result["_training_basket"] = basket_labels
    return result

def safe_dataframe(frame: pd.DataFrame, **kwargs) -> None:
    """Render without crashing on duplicate columns or malformed values."""
    try:
        cleaned = frame.copy()
        cleaned = cleaned.loc[:, ~cleaned.columns.duplicated()].copy()
        st.dataframe(cleaned, **kwargs)
    except Exception as exc:
        st.error("This table could not render safely.")
        st.caption(f"Dashboard detail: {type(exc).__name__}")


def dataframe_to_cards(table: pd.DataFrame) -> list[dict]:
    """Turn one of this file's already-built display tables (Company/
    Domain/Website plus whatever bucket-specific columns) into card dicts
    for render_card_grid -- every non-identity column becomes an activity
    row, so this adapts automatically to each bucket's own column set."""
    title_col = "Domain" if "Domain" in table.columns else "Company"
    meta_col = "Company" if "Company" in table.columns and title_col != "Company" else None
    tag_cols = [c for c in ("Category",) if c in table.columns]
    identity_cols = {title_col, meta_col, "Website", *tag_cols}

    cards = []
    for _, row in table.iterrows():
        tags = [str(row[c]) for c in tag_cols if row.get(c)]
        decision = row.get("Claude Decision")
        if decision and decision != "Not reviewed":
            tags.append(f"Claude: {decision}")

        activity = []
        for column in table.columns:
            if column in identity_cols or column == "Claude Decision":
                continue
            value = row.get(column)
            if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
                continue
            text = str(value)
            if len(text) > 180:
                text = text[:180].rsplit(" ", 1)[0] + "…"
            activity.append((column, text))

        website = row.get("Website")
        cards.append({
            "title": str(row.get(title_col, "")),
            "link": str(website) if website else None,
            "meta": str(row[meta_col]) if meta_col and row.get(meta_col) else None,
            "tags": tags,
            "activity": activity,
        })
    return cards


def render_bucket_display(
    table: pd.DataFrame,
    empty_message: str,
    key_prefix: str,
    *,
    column_config: dict | None = None,
    page_size: int = 12,
) -> None:
    """Card grid (default) or the original dense table for one bucket's
    already-built display table. Cards are paged so a bucket of any size
    never dumps thousands of cards into the DOM at once."""
    if table.empty:
        st.info(empty_message)
        return

    view = st.radio(
        "View",
        ["Table", "Cards"],
        horizontal=True,
        key=f"{key_prefix}_view_mode",
        label_visibility="collapsed",
    )
    if view == "Table":
        render_hideable_table(
            table,
            domain_column="Domain",
            key=f"{key_prefix}_hideable_table",
            column_config=column_config or {},
        )
        return

    page_key = f"{key_prefix}_card_page"
    total = len(table)
    total_pages = max(1, (total - 1) // page_size + 1)
    page = min(int(st.session_state.get(page_key, 1)), total_pages)
    start = (page - 1) * page_size

    render_card_grid(dataframe_to_cards(table.iloc[start:start + page_size]))

    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    if nav_left.button("← Previous", disabled=page <= 1, key=f"{key_prefix}_card_prev"):
        st.session_state[page_key] = page - 1
        st.rerun()
    nav_mid.caption(f"Page {page} of {total_pages} · {total:,} companies")
    if nav_right.button("Next →", disabled=page >= total_pages, key=f"{key_prefix}_card_next"):
        st.session_state[page_key] = page + 1
        st.rerun()

def numeric_max(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    return int(values.max()) if not values.empty else 0




def format_candidate_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Create the concise main lead table shown on the dashboard."""
    if frame.empty:
        return pd.DataFrame()

    result = frame.copy()
    output = pd.DataFrame(index=result.index)

    def first_available(*columns: str) -> pd.Series:
        for column in columns:
            if column in result.columns:
                return result[column]
        return pd.Series("", index=result.index)

    output["Company"] = first_available(
        "llm_company_name",
        "page_title",
        "domain",
    )
    output["Score"] = pd.to_numeric(
        first_available("llm_fit_score", "rule_score"),
        errors="coerce",
    ).fillna(0).astype(int)
    output["Description"] = first_available(
        "llm_product_summary",
        "meta_description",
    )
    output["Category"] = first_available(
        "llm_category",
        "rule_thesis_pillar",
    )

    if "llm_decision" in result.columns:
        decisions = result["llm_decision"].astype(str).str.upper()
        output["Classification"] = decisions.map({
            "KEEP": "Verified Lead",
            "REJECT": "Denied",
        }).fillna("Needs Review")
    else:
        output["Classification"] = first_available(
            "investment_classification",
            "routing_bucket",
        )

    funding = pd.to_numeric(
        first_available("llm_funding_millions_usd"),
        errors="coerce",
    )
    latest_round = first_available("llm_latest_round").astype(str)

    def funding_stage_label(index) -> str:
        funding_value = funding.loc[index] if index in funding.index else float("nan")
        round_value = latest_round.loc[index] if index in latest_round.index else ""

        parts: list[str] = []
        funding_evidence = str(
            result.loc[index, "llm_funding_evidence"]
            if "llm_funding_evidence" in result.columns
            else ""
        ).strip().lower()

        funding_verified = (
            pd.notna(funding_value)
            and float(funding_value) > 0
        )
        verified_zero = (
            pd.notna(funding_value)
            and float(funding_value) == 0
            and any(
                phrase in funding_evidence
                for phrase in (
                    "bootstrapped",
                    "no external funding",
                    "no funding",
                    "self-funded",
                )
            )
        )

        if funding_verified or verified_zero:
            parts.append(f"${float(funding_value):g}M")
        else:
            parts.append("Funding not verified")

        if round_value and round_value not in {"unknown", "nan", "none", ""}:
            parts.append(round_value.replace("_", " ").title())

        return " · ".join(parts)

    output["Funding / stage"] = [
        funding_stage_label(index) for index in output.index
    ]
    output["Website"] = first_available("final_url")

    output = output.sort_values("Score", ascending=False)
    return output.reset_index(drop=True)


def humanize_reason(value: object) -> str:
    mapping = {
        "consumer_b2c_product": "Consumer product",
        "media_newsletter_or_aggregator": "Media or newsletter",
        "parked_or_for_sale": "Domain parked or for sale",
        "website_unavailable": "Website unavailable",
        "generic_placeholder": "Placeholder website",
        "agency_or_consultancy": "Agency or consultancy",
        "confirmed_non_us": "Non-U.S. company",
        "likely_large_or_established_company": "Too mature or established",
        "potential_comp": "Potential comp",
        "potential_comp_too_mature": "Potential comp — too mature",
        "insufficient_company_or_product_evidence": "Not enough company or product information",
        "failed_pre_llm_evidence_gate": "Insufficient product evidence",
        "failed_glasswing_b2b_gate": "Insufficient product evidence",
        "http_404": "Website returned 404",
        "http_403": "Website blocked access",
        "http_500": "Website error",
        "no_clear_enterprise_or_glasswing_fit": (
            "No clear enterprise or Glasswing thesis fit"
        ),
        "website_unavailable_or_no_usable_company_evidence": (
            "Website unavailable or no usable company evidence"
        ),
        "credible_but_requires_human_review": (
            "Credible company requiring human review"
        ),
        "enterprise_rescue_requires_human_review": (
            "Strong enterprise signal requiring human review"
        ),
    }
    raw = str(value or "").strip()
    if raw in mapping:
        return mapping[raw]
    if raw.startswith("http_"):
        return f"Website returned HTTP {raw.split('_', 1)[1]}"
    return raw.replace("_", " ").strip().title()


def format_denied_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    result = pd.DataFrame(index=frame.index)

    def first_available(*columns: str) -> pd.Series:
        for column in columns:
            if column in frame.columns:
                return frame[column]
        return pd.Series("", index=frame.index)

    result["Company"] = first_available(
        "llm_company_name",
        "page_title",
        "domain",
    )
    result["Domain"] = first_available("domain")
    result["Reason"] = first_available(
        "routing_reason",
        "llm_concerns",
        "reason",
    ).map(humanize_reason)
    result["Description"] = first_available(
        "meta_description",
        "llm_product_summary",
        "homepage_excerpt",
    )
    result["Website"] = first_available("final_url")
    return result.reset_index(drop=True)


stream_status = read_stream_status()
if RUNTIME_MODE == "local":
    running = bool(stream_status.get("running", False))
else:
    # Desired state controls the always-on Railway worker. Detailed live
    # counts and stage come independently from its shared status snapshot.
    running = bool(
        runtime_status()["services"]["pipeline"]["desired"]
    )

saved_config = load_dashboard_run_config()
backlog_mode = str(
    stream_status.get("backlog_mode", "normal")
).strip().lower()

st.markdown(
    """
    <div class="glasswing-header">
      <div class="glasswing-title">Glasswing Sourcing</div>
    </div>
    """,
    unsafe_allow_html=True,
)
with st.expander("Run Filters", expanded=not running):
    if running:
        st.info(
            "Filters are locked while the pipeline is running -- every "
            "control below is read-only. Click **Stop** above to unlock "
            "them, make your changes, then **Start** again."
        )
    st.markdown("##### Domain discovery")
    saved_extensions = [
        extension
        for extension in saved_config.get(
            "selected_extensions",
            EXTENSION_PRESETS["Focused AI & Developer"],
        )
        if extension in EXTENSION_OPTIONS
    ]

    matching_preset = "Custom"
    for preset_name, preset_extensions in EXTENSION_PRESETS.items():
        if (
            preset_name != "Custom"
            and set(saved_extensions) == set(preset_extensions)
        ):
            matching_preset = preset_name
            break

    preset_names = list(EXTENSION_PRESETS.keys())
    extension_preset = st.selectbox(
        "Coverage preset",
        preset_names,
        index=preset_names.index(matching_preset),
        help=(
            "Focused is recommended. Startup Broad captures more companies. "
            "Maximum Coverage includes .com and creates substantially more noise."
        ),
        disabled=running,
    )

    preset_extensions = EXTENSION_PRESETS[extension_preset]
    default_extensions = (
        saved_extensions
        if extension_preset == "Custom"
        else preset_extensions
    )

    if not running and extension_preset != "Custom":
        st.caption(
            "Extensions follows the coverage preset above. Set "
            "**Coverage preset** to **Custom** to edit them individually."
        )
    selected_extensions = st.multiselect(
        "Extensions",
        options=list(EXTENSION_OPTIONS.keys()),
        default=default_extensions,
        format_func=lambda extension: (
            f"{extension}  ·  {EXTENSION_OPTIONS[extension]}"
        ),
        help=(
            "Only matching CertStream domains enter the queue. "
            "Use Custom to create your own combination."
        ),
        disabled=running or extension_preset != "Custom",
    )

    selected_extension_text = "  ".join(
        f"`{extension}`"
        for extension in selected_extensions
    )
    st.caption(
        f"Active coverage: {selected_extension_text or 'None selected'}"
    )

    if ".com" in selected_extensions:
        st.warning(
            ".com creates extremely high incoming volume and much more noise. "
            "The pipeline will likely enter Catch-up or Surge mode."
        )
    elif extension_preset == "Startup Broad":
        st.info(
            "Startup Broad is higher-volume but still targeted toward "
            "technology and startup-style domains."
        )

    profile = st.selectbox(
        "Filter preset",
        [
            "Balanced Glasswing",
            "Strict high-conviction",
            "AI infrastructure",
            "Cybersecurity",
            "Vertical AI",
            "Custom",
        ],
        disabled=running,
    )

    profile_defaults = {
        "Balanced Glasswing": {
            "industries": list(INDUSTRY_OPTIONS.keys()),
            "minimum_score": 55,
            "minimum_dimensions": 2,
        },
        "Strict high-conviction": {
            "industries": list(INDUSTRY_OPTIONS.keys()),
            "minimum_score": 80,
            "minimum_dimensions": 4,
        },
        "AI infrastructure": {
            "industries": [
                "AI Infrastructure",
                "Next-Generation Compute",
            ],
            "minimum_score": 70,
            "minimum_dimensions": 3,
        },
        "Cybersecurity": {
            "industries": ["Cybersecurity"],
            "minimum_score": 68,
            "minimum_dimensions": 3,
        },
        "Vertical AI": {
            "industries": ["Vertical AI"],
            "minimum_score": 70,
            "minimum_dimensions": 3,
        },
        "Custom": {
            "industries": [
                label
                for label, value in INDUSTRY_OPTIONS.items()
                if value in saved_config.get(
                    "selected_industries",
                    [],
                )
            ],
            "minimum_score": int(
                saved_config.get("minimum_score", 55)
            ),
            "minimum_dimensions": int(
                saved_config.get(
                    "minimum_signal_dimensions",
                    2,
                )
            ),
        },
    }
    selected_profile = profile_defaults[profile]

    filter_left, filter_right = st.columns(2)
    with filter_left:
        selected_industry_labels = st.multiselect(
            "Industries",
            list(INDUSTRY_OPTIONS.keys()),
            default=selected_profile["industries"],
            disabled=running,
        )
        include_keywords_text = st.text_area(
            "Required keywords",
            value=", ".join(
                saved_config.get("include_keywords", [])
            ),
            placeholder=(
                "Example: inference, observability, underwriting"
            ),
            help=(
                "Leave blank for no required keywords. Separate terms "
                "with commas or new lines."
            ),
            disabled=running,
        )
        exclude_keywords_text = st.text_area(
            "Excluded keywords",
            value=", ".join(
                saved_config.get("exclude_keywords", [])
            ),
            placeholder=(
                "Example: agency, consulting, consumer, course"
            ),
            disabled=running,
        )
        keyword_mode_label = st.radio(
            "Required keyword matching",
            ["Match any", "Match all"],
            horizontal=True,
            disabled=running,
        )

    with filter_right:
        minimum_score = st.slider(
            "Minimum investable score",
            min_value=40,
            max_value=90,
            value=selected_profile["minimum_score"],
            help=(
                "Companies below this appear under Hard Denied with the "
                "specific filter reason that caused the rejection."
            ),
            disabled=running,
        )
        minimum_dimensions = st.slider(
            "Minimum evidence dimensions",
            min_value=1,
            max_value=6,
            value=selected_profile["minimum_dimensions"],
            help=(
                "Requires evidence across dimensions such as buyer, "
                "workflow, technical depth, value, defensibility, and "
                "problem importance."
            ),
            disabled=running,
        )
        require_us_evidence = st.checkbox(
            "Require explicit U.S. evidence",
            value=bool(
                saved_config.get(
                    "require_us_evidence",
                    False,
                )
            ),
            help=(
                "Strict option. It may miss stealth companies whose "
                "site does not list a location."
            ),
            disabled=running,
        )

    st.markdown("##### Screening speed")
    speed_preset = st.selectbox(
        "Throughput preset",
        [
            "Balanced",
            "Fast",
            "Turbo",
            "Custom",
        ],
        index=1,
        help=(
            "Higher settings process more websites simultaneously but "
            "use more CPU, memory, and network bandwidth."
        ),
        disabled=running,
    )

    speed_defaults = {
        "Balanced": (40, 16, 7),
        "Fast": (75, 28, 6),
        "Turbo": (100, 36, 6),
        "Custom": (
            min(100, int(saved_config.get("screening_batch_size", 75))),
            min(40, int(saved_config.get("scraper_workers", 28))),
            min(7, int(saved_config.get("scrape_timeout", 6))),
        ),
    }
    default_batch, default_workers, default_timeout = speed_defaults[
        speed_preset
    ]

    speed_left, speed_mid, speed_right = st.columns(3)
    with speed_left:
        screening_batch_size = st.number_input(
            "Domains per batch",
            min_value=1,
            max_value=500,
            value=default_batch,
            disabled=running,
        )
    with speed_mid:
        scraper_workers = st.number_input(
            "Concurrent website scrapers",
            min_value=1,
            max_value=40,
            value=default_workers,
            help=(
                "Network-bound website requests processed at the same "
                "time. The pipeline caps this at 40 to protect your Mac."
            ),
            disabled=running,
        )
    with speed_right:
        scrape_timeout = st.number_input(
            "Website timeout in seconds",
            min_value=5,
            max_value=45,
            value=default_timeout,
            help=(
                "Lower values clear dead sites faster. Very low values "
                "can miss slow but legitimate websites."
            ),
            disabled=running,
        )

    estimated_waves = max(
        1,
        (
            int(screening_batch_size)
            + int(scraper_workers)
            - 1
        )
        // int(scraper_workers),
    )
    st.caption(
        f"Each batch runs in about {estimated_waves} concurrent request "
        f"wave{'s' if estimated_waves != 1 else ''}, excluding slow-site "
        "timeouts."
    )

    st.markdown("##### Claude diligence")
    st.caption(
        "Fixed, not user-editable: any Needs Review company scoring 70+ "
        "is sent to Claude automatically. Claude's KEEP decision is the "
        "only way a company becomes Investable; REJECT confirms Hard "
        "Denied. There is a fixed daily/hourly call budget so this can "
        "never run away on cost."
    )

run_config = {
    "selected_extensions": list(selected_extensions),
    "selected_industries": [
        INDUSTRY_OPTIONS[label]
        for label in selected_industry_labels
    ],
    "include_keywords": parse_keyword_text(
        include_keywords_text
    ),
    "exclude_keywords": parse_keyword_text(
        exclude_keywords_text
    ),
    "keyword_mode": (
        "all"
        if keyword_mode_label == "Match all"
        else "any"
    ),
    "minimum_score": int(minimum_score),
    "minimum_signal_dimensions": int(minimum_dimensions),
    "screening_batch_size": int(screening_batch_size),
    "scraper_workers": int(scraper_workers),
    "scrape_timeout": int(scrape_timeout),
    "require_us_evidence": bool(require_us_evidence),
}

start_col, stop_col, reset_col, status_col = st.columns(
    [1, 1, 1.25, 2.75]
)

with start_col:
    if st.button(
        "Start",
        disabled=running,
        type="primary",
        width="stretch",
    ):
        if not selected_extensions:
            st.error("Select at least one domain extension.")
        elif not selected_industry_labels:
            st.error("Select at least one industry.")
        else:
            start_continuous_pipeline(run_config)

with stop_col:
    if st.button(
        "Stop",
        disabled=not running,
        width="stretch",
    ):
        stop_continuous_pipeline()

with reset_col:
    if st.button(
        "Reset current run",
        width="stretch",
    ):
        st.session_state["confirm_reset_run"] = True

with status_col:
    if RUNTIME_MODE == "local":
        stage = str(
            stream_status.get("stage", "stopped")
        ).replace("_", " ").title()
    else:
        stage = str(
            stream_status.get("stage", "waiting for worker status")
        ).replace("_", " ").title()
    state = "Running" if running else "Stopped"
    st.markdown(
        f'<div class="status-card"><strong>{state}</strong>'
        f'<br><span style="opacity:.65;font-size:.85rem">'
        f'{stage}</span></div>',
        unsafe_allow_html=True,
    )

refresh_col, refresh_note_col = st.columns([1, 4])
with refresh_col:
    if st.button(
        "Refresh now",
        width="stretch",
        key="manual_dashboard_refresh",
    ):
        st.rerun()
with refresh_note_col:
    if running:
        st.caption(
            "Automatically refreshes every 30 seconds while the pipeline is "
            "running. Use Refresh now for an immediate update."
        )

if st.session_state.get("confirm_reset_run", False):
    st.warning(
        "Reset stops the pipeline and clears unfinished queue work. "
        "Previously screened companies remain saved."
    )
    confirm_col, cancel_col, _ = st.columns([1.2, 1, 4])
    with confirm_col:
        if st.button(
            "Confirm reset",
            type="primary",
            width="stretch",
        ):
            reset_current_run()
    with cancel_col:
        if st.button("Cancel", width="stretch"):
            st.session_state["confirm_reset_run"] = False
            st.rerun()

if stream_status.get("shared_status_error"):
    st.error(
        "Live worker status is temporarily unavailable: "
        f"{stream_status['shared_status_error']}"
    )
elif stream_status.get("shared_status_waiting"):
    st.info(
        "Waiting for the pipeline worker's first live status update. "
        "This normally takes only a few seconds after deployment."
    )
elif RUNTIME_MODE != "local":
    st.caption(
        "Counts, queue details, and the current stage below are live from "
        "the pipeline worker's shared status record."
    )

queue = stream_status.get("queue", {}) or {}
status_metrics = st.columns(4)
status_metrics[0].metric(
    "New domains",
    int(
        stream_status.get(
            "new_domains_received",
            stream_status.get("domains_received", 0),
        )
        or 0
    ),
    help=(
        "Domains the persistent registry has never seen before. "
        "Only these enter screening."
    ),
)
# "Classified" used to live here as its own worker-side counter, computed
# before Python auto-rejections get purged -- it could show a different
# number than the Sourcing funnel's "Websites checked" below (the same
# concept, computed from what's actually retained). Removed rather than
# fixed in place so there's exactly one definition of this number on the
# page, not two that can drift apart.
status_metrics[1].metric(
    "Ignored repeats",
    int(stream_status.get("repeat_events_ignored", 0) or 0),
    help=(
        "Certificate renewals and repeat sightings ignored before scraping."
    ),
)
status_metrics[2].metric(
    "Claude calls",
    int(stream_status.get("claude_calls", 0) or 0),
    help=(
        f"Automatic: {int(stream_status.get('automatic_claude_calls', 0) or 0)} "
        f"· Manual: {int(stream_status.get('manual_claude_calls', 0) or 0)}"
    ),
)
status_metrics[3].metric(
    "Failed",
    int(stream_status.get("failed_this_run", 0) or 0),
    help="New domains that exhausted retries during this run.",
)

selected_status_extensions = stream_status.get(
    "selected_extensions",
    saved_config.get("selected_extensions", [".ai"]),
)
received_by_extension = stream_status.get(
    "received_by_extension",
    {},
) or {}
queue_by_extension = stream_status.get(
    "queue_by_extension",
    {},
) or {}

st.markdown("#### New domains by extension")
extension_columns = st.columns(
    max(1, len(selected_status_extensions))
)
for column, extension in zip(
    extension_columns,
    selected_status_extensions,
):
    received_count = int(
        received_by_extension.get(extension, 0) or 0
    )
    column.metric(
        extension,
        received_count,
        help=f"{received_count} genuinely new domains this run.",
    )

last_domain = (
    stream_status.get("last_domain_processed")
    or stream_status.get("last_domain_received")
    or ""
)
if last_domain:
    st.caption(f"Latest new domain: {last_domain}")

certificate_events = int(
    stream_status.get("certificate_events_received", 0) or 0
)
repeat_events = int(
    stream_status.get("repeat_events_ignored", 0) or 0
)
if certificate_events or repeat_events:
    st.caption(
        f"New-domain gate: {certificate_events} relevant certificate "
        f"events observed · {repeat_events} repeat events ignored "
        f"before scraping."
    )

mode = str(
    stream_status.get("backlog_mode", "normal")
)
if backlog_mode == "surge":
    st.warning(
        "Backlog Surge mode is active: homepage-only screening with "
        "safe concurrency. Ghost sites are separated and retried later."
    )
elif backlog_mode == "catch_up":
    st.info(
        "Catch-up mode is active: the queue exceeded 500 domains, "
        "so screening is using larger homepage-only batches."
    )

if running:
    active_batch = int(
        stream_status.get("screening_batch_size", 200) or 200
    )
    active_workers = int(
        stream_status.get("scraper_workers", 28) or 28
    )
    st.caption(
        f"Active throughput: {active_batch} domains per batch, "
        f"{active_workers} concurrent website scrapers."
    )

if stream_status.get("last_error"):
    stream_stage = str(stream_status.get("stage", "")).lower()
    connection_state = str(
        stream_status.get("stream_connection_state", stream_stage)
    ).lower()
    if connection_state in {"connecting", "reconnecting"}:
        st.warning(
            "CertStream connection was interrupted. The pipeline is "
            f"reconnecting automatically: {stream_status['last_error']}"
        )
    else:
        st.error(
            f"Python pipeline issue: {stream_status['last_error']}"
        )
    if stream_stage == "crashed":
        crash_log = ROOT / "logs" / "continuous_pipeline.log"
        if crash_log.exists():
            with st.expander("Show startup error log", expanded=True):
                lines = crash_log.read_text(
                    encoding="utf-8",
                    errors="ignore",
                ).splitlines()
                st.code("\n".join(lines[-60:]) or "No log output.")

_slow_op_labels = {
    "last_slow_scoring": "Slow per-domain scoring",
    "last_slow_persist": "Slow batch persist",
    "last_slow_register_domain_event": "Slow CertStream event registration",
}
_slow_ops = {
    label: stream_status[key]
    for key, label in _slow_op_labels.items()
    if stream_status.get(key)
}
if _slow_ops:
    with st.expander("Slow-operation diagnostics", expanded=False):
        st.caption(
            "Surfaces individual operations that took longer than "
            "expected -- helps pin down what's actually consuming time "
            "inside a batch, since railway logs doesn't capture this "
            "process's stdout/stderr."
        )
        for label, details in _slow_ops.items():
            st.write(f"**{label}**", details)

manual_message = st.session_state.pop(
    "last_manual_claude_message",
    "",
)
if manual_message:
    st.success(manual_message)
    manual_result = read_stream_status().get(
        "last_manual_claude_result",
        {},
    )
    if manual_result:
        st.json(manual_result)

reclassification_message = st.session_state.pop(
    "last_reclassification_message",
    "",
)
if reclassification_message:
    st.success(reclassification_message)


if running:
    st.caption(
        "The pipeline is live. This page refreshes automatically every 30 "
        "seconds; press Refresh now for an immediate update."
    )

current_run_id = str(
    stream_status.get("run_id", "")
).strip()

scope_col, run_id_col = st.columns([1.4, 3.6])
with scope_col:
    result_scope = st.radio(
        "Results view",
        ["Current Run", "All History"],
        index=1,
        horizontal=True,
        label_visibility="collapsed",
        key="result_scope",
    )
with run_id_col:
    if result_scope == "Current Run":
        if current_run_id:
            st.caption(f"Current run: `{current_run_id}`")
        else:
            st.caption("No active or recently completed run is selected.")
    else:
        st.caption("Showing the latest saved result for every known domain.")

# Confirmed live: pipeline-worker writes real Claude verdicts to BOTH the
# shared Postgres claude_diligence table and its own local CSV mirror
# (llm_approved_master.csv/llm_denied_master.csv) -- but only the shared
# table is visible cross-service. Reading the local mirror directly here
# meant the web dashboard's own Investable/Rejected-by-Claude tables stayed
# permanently empty despite real, successful Claude calls happening on
# pipeline-worker the whole time. query_claude_diligence() (via
# latest_claude_results(), defined below) already reads the shared store
# first with a local-CSV fallback -- reuse that instead of a second,
# local-only read path.
def _claude_master_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    from classification_store import query_claude_diligence

    shared = pd.DataFrame(query_claude_diligence())
    if not shared.empty:
        decisions = shared["llm_decision"].astype(str).str.upper()
        return (
            shared[decisions.eq("KEEP")].copy(),
            shared[decisions.eq("REJECT")].copy(),
        )
    return (
        read_csv_safely(LLM / "llm_approved_master.csv"),
        read_csv_safely(LLM / "llm_denied_master.csv"),
    )


approved_all, llm_denied = _claude_master_frames()
verified_leads = (
    approved_all[
        approved_all.get("llm_decision", "").astype(str).str.upper() == "KEEP"
    ].copy()
    if not approved_all.empty and "llm_decision" in approved_all.columns
    else pd.DataFrame()
)

python_shortlist = read_csv_safely(
    FILTERED / "python_shortlist_master.csv"
)
python_denied = read_csv_safely(FILTERED / "python_denied_master.csv")

try:
    all_screened = read_screened_classifications()
except Exception as exc:
    st.error(
        "The shared classification store is temporarily unavailable. "
        f"Showing the local compatibility export instead: "
        f"{type(exc).__name__}: {exc}"
    )
    all_screened = read_csv_safely(FILTERED / "all_screened.csv")
if all_screened.empty:
    all_screened = read_csv_safely(FILTERED / "all_screened.csv")
all_screened = apply_overrides(all_screened)

active_display_extensions = set(
    saved_config.get("selected_extensions", [".ai"])
)
if not all_screened.empty and "domain" in all_screened.columns:
    visible_extensions = (
        all_screened["domain"]
        .astype(str)
        .str.extract(r"(\.[^.]+)$", expand=False)
        .fillna("")
    )
    all_screened = all_screened[
        visible_extensions.isin(active_display_extensions)
    ].copy()

all_history_screened = all_screened.copy()
if result_scope == "Current Run":
    if (
        current_run_id
        and not all_screened.empty
        and "run_id" in all_screened.columns
    ):
        all_screened = all_screened[
            all_screened["run_id"].astype(str).eq(current_run_id)
        ].copy()
    else:
        all_screened = pd.DataFrame(
            columns=all_screened.columns
            if not all_screened.empty
            else []
        )

def latest_claude_results() -> pd.DataFrame:
    from classification_store import query_claude_diligence

    shared = pd.DataFrame(query_claude_diligence())
    if not shared.empty:
        return shared

    frames = []
    for path in (
        LLM / "llm_approved_master.csv",
        LLM / "llm_denied_master.csv",
    ):
        frame = read_csv_safely(path)
        if not frame.empty and "domain" in frame.columns:
            frames.append(frame.copy())

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["domain"] = combined["domain"].map(stable_domain)
    combined = combined.drop_duplicates("domain", keep="last")

    columns = [
        column
        for column in (
            "domain",
            "llm_decision",
            "llm_fit_score",
            "llm_product_summary",
            "llm_reasoning",
            "llm_concerns",
            "llm_model",
        )
        if column in combined.columns
    ]
    return combined[columns].copy()


def merge_claude_results(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "domain" not in frame.columns:
        return frame

    latest = latest_claude_results()
    if latest.empty:
        return frame

    output = frame.copy()
    output["domain"] = output["domain"].map(stable_domain)

    for column in latest.columns:
        if column != "domain" and column in output.columns:
            output = output.drop(columns=[column])

    return output.merge(
        latest,
        how="left",
        on="domain",
        validate="many_to_one",
    )


@st.cache_data(show_spinner=False, max_entries=4)
def visible_classification_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Assign every screened row to one of two outcomes.

    Needs Review is the default for everything that isn't a specific,
    factual disqualifier -- Investable and Coming Soon are labels Claude
    or a human applies on top of it, not something this function decides
    on its own. Hard Denied requires a factual disqualifier.
    """
    if frame.empty:
        result = frame.copy()
        result["_visible_bucket"] = ""
        return result

    result = frame.copy()
    routing = result.get(
        "routing_bucket",
        pd.Series("", index=result.index),
    ).astype(str).str.strip().str.lower()
    overrides = result.get(
        "_override_target",
        pd.Series("", index=result.index),
    ).astype(str).str.strip()
    decisions = result.get(
        "llm_decision",
        pd.Series("", index=result.index),
    ).astype(str).str.strip().str.upper()

    scores = pd.to_numeric(
        result.get(
            "diligence_rank_score",
            result.get(
                "rule_score",
                pd.Series(0, index=result.index),
            ),
        ),
        errors="coerce",
    ).fillna(0)
    website_copy = (
        result.get("page_title", pd.Series("", index=result.index)).astype(str)
        + " "
        + result.get(
            "meta_description",
            pd.Series("", index=result.index),
        ).astype(str)
        + " "
        + result.get(
            "homepage_excerpt",
            pd.Series("", index=result.index),
        ).astype(str)
    ).str.strip()
    website_copy_is_usable = website_copy.ne("")
    rescored = website_copy.map(score_text)
    revised_scores = rescored.map(lambda item: int(item[0]))
    revised_pillars = rescored.map(lambda item: str(item[1]))
    scores = scores.where(~website_copy_is_usable, revised_scores)

    def stored_truth(column: str) -> pd.Series:
        return result.get(
            column,
            pd.Series("", index=result.index),
        ).astype(str).str.strip().str.lower().isin({"true", "1", "yes"})

    routing_reasons_for_score = result.get(
        "routing_reason",
        pd.Series("", index=result.index),
    ).astype(str).str.lower()
    placeholder_reasons_for_score = result.get(
        "placeholder_reason",
        pd.Series("", index=result.index),
    ).astype(str).str.lower()
    location_for_score = result.get(
        "location_status",
        pd.Series("", index=result.index),
    ).astype(str)
    investment_labels_for_score = result.get(
        "investment_classification",
        pd.Series("", index=result.index),
    ).astype(str)
    non_english_for_score = stored_truth("predominantly_non_english")
    placeholder_for_score = stored_truth("placeholder_detected")
    media_for_score = stored_truth("media_newsletter")
    commodity_for_score = stored_truth("commodity_wrapper")
    adjusted_scores = []
    score_cap_reasons = []
    for index, score in scores.items():
        adjusted, applied = investment_eligibility_adjusted_score(
            score,
            predominantly_non_english=bool(non_english_for_score.loc[index]),
            location_status=location_for_score.loc[index],
            parked=placeholder_reasons_for_score.loc[index] == "parked_or_for_sale",
            placeholder=bool(placeholder_for_score.loc[index]),
            agency="agency_or_consultancy" in routing_reasons_for_score.loc[index],
            consumer="consumer_b2c_product" in routing_reasons_for_score.loc[index],
            media=bool(media_for_score.loc[index]),
            commodity_ai_service=bool(commodity_for_score.loc[index]),
            mature_or_funded=(
                investment_labels_for_score.loc[index] == "Potential Comp"
                or "mature_or_funded" in routing_reasons_for_score.loc[index]
            ),
        )
        adjusted_scores.append(adjusted)
        score_cap_reasons.append("; ".join(applied))
    scores = pd.Series(adjusted_scores, index=result.index, dtype="int64")
    result["_score_eligibility_caps"] = score_cap_reasons
    result["_fit_score_v2"] = scores.astype(int)
    result["_fit_pillar_v2"] = revised_pillars.where(
        website_copy_is_usable,
        result.get(
            "rule_thesis_pillar",
            pd.Series("", index=result.index),
        ).astype(str),
    )

    signal_columns = (
        "score_defensibility_signals",
        "score_important_problem_signals",
        "score_technical_signals",
        "workflow_depth_count",
        "business_value_count",
        "buyer_signal_count",
    )
    signal_dimensions = pd.Series(0, index=result.index, dtype="int64")
    for column in signal_columns:
        values = pd.to_numeric(
            result.get(
                column,
                pd.Series(0, index=result.index),
            ),
            errors="coerce",
        ).fillna(0)
        signal_dimensions += values.gt(0).astype(int)

    saved_rescue = result.get(
        "enterprise_rescue_signal",
        pd.Series("", index=result.index),
    ).astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    rescue_text = (
        result.get("page_title", pd.Series("", index=result.index)).astype(str)
        + " "
        + result.get(
            "meta_description",
            pd.Series("", index=result.index),
        ).astype(str)
    )
    inferred_rescue = enterprise_rescue_mask(rescue_text)
    enterprise_rescue = saved_rescue | inferred_rescue

    review_floor = 25

    # Two outcomes only (2026-08-08): Needs Review is the default for
    # everything that isn't a specific, factual disqualifier -- there is
    # no more low-attention "insufficient evidence" limbo that a
    # thesis-fit judgment quietly falls into. "priority" is only ever
    # reached now via a manual override or a Claude KEEP -- kept here too
    # so already-stored historical rows from before this change still
    # display the same way rather than silently reappearing as Needs
    # Review. Coming Soon is gone entirely, including for legacy rows --
    # a prelaunch page is just a Needs Review company like any other.
    visible = pd.Series(
        "needs_review",
        index=result.index,
        dtype="object",
    )
    visible.loc[routing.eq("reject")] = "hard_denied"
    visible.loc[routing.eq("priority")] = "investable"

    legacy_reasons = result.get(
        "screened_out_reasons",
        pd.Series("", index=result.index),
    ).astype(str).str.lower()
    routing_reasons = result.get(
        "routing_reason",
        pd.Series("", index=result.index),
    ).astype(str).str.lower()
    combined_reasons = legacy_reasons + ";" + routing_reasons

    # Only real factual disqualifiers count as Hard Denied here --
    # "substantive_site_no_glasswing_fit_signals" used to also promote a
    # weak-fit-but-readable site straight to Hard Denied, which is exactly
    # the same low-confidence auto-judgment problem Insufficient Evidence
    # had, just under a different label. That's now a Needs Review company
    # like any other uncertain one, not a denial.
    factual_denial_terms = (
        "parked_or_for_sale|generic_hosting_or_registrar_placeholder|"
        "confirmed_non_us|predominantly_non_english|consumer_b2c_product|"
        "agency_or_consultancy|media_newsletter_or_aggregator|"
        "commodity_ai_wrapper_or_service|mature_or_funded_company"
    )
    explicit_denial = combined_reasons.str.contains(
        factual_denial_terms,
        regex=True,
        na=False,
    )
    visible.loc[explicit_denial] = "hard_denied"

    # Claude decisions and manual overrides can promote a Needs Review
    # company to Investable, or confirm a Hard Denied. No MAYBE: every
    # Claude call resolves to one of these two.
    visible.loc[decisions.eq("KEEP")] = "investable"
    visible.loc[decisions.eq("REJECT")] = "hard_denied"

    visible.loc[overrides.eq("Investable Candidate")] = "investable"
    visible.loc[overrides.eq("Review Candidate")] = "needs_review"
    visible.loc[overrides.eq("Potential Comp")] = "needs_review"
    visible.loc[overrides.eq("Hard Denied")] = "hard_denied"

    result["_visible_bucket"] = visible
    result["_review_score_floor"] = review_floor
    result["_signal_dimensions"] = signal_dimensions

    if "routing_reason" not in result.columns:
        result["routing_reason"] = ""
    if "denial_evidence" not in result.columns:
        result["denial_evidence"] = ""

    rescue_promoted = enterprise_rescue & visible.eq("needs_review")
    result.loc[
        rescue_promoted,
        "routing_reason",
    ] = "enterprise_rescue_requires_human_review"

    return result


classified_screened = visible_classification_frame(
    merge_claude_results(all_screened)
)


def visible_bucket_frame(bucket: str) -> pd.DataFrame:
    if classified_screened.empty:
        return pd.DataFrame(columns=classified_screened.columns)
    return classified_screened[
        classified_screened["_visible_bucket"].astype(str).eq(bucket)
    ].copy()


def classification_reason_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Count specific visible outcomes without collapsing missing reasons."""
    if frame.empty:
        return pd.DataFrame(columns=["Reason", "Count", "Share"])

    reasons = frame.get(
        "routing_reason",
        pd.Series("", index=frame.index),
    ).astype(str).str.strip()
    reasons = reasons.mask(reasons.eq(""), "reason_not_recorded")
    counts = reasons.value_counts(dropna=False)
    summary = counts.rename_axis("Reason").reset_index(name="Count")
    summary["Reason"] = summary["Reason"].map(humanize_reason)
    summary["Share"] = (
        summary["Count"].div(len(frame)).mul(100).round(1)
    )
    return summary


investable_candidates = visible_bucket_frame("investable")
needs_review = visible_bucket_frame("needs_review")
hard_denied = visible_bucket_frame("hard_denied")

for visible_frame_name in (
    "investable_candidates",
    "needs_review",
    "hard_denied",
):
    visible_frame = locals()[visible_frame_name]
    if not visible_frame.empty and "domain" in visible_frame.columns:
        locals()[visible_frame_name] = visible_frame.drop_duplicates(
            "domain",
            keep="last",
        )

# Training baskets contain human decisions only. Automated classifications are
# displayed here for review, but never write their own training labels.
positive_training = investable_candidates.copy()

# This is an audit view of the Hard Denied bucket, not an additional
# classification bucket. Exclude persistent manual overrides so the tab
# reflects Claude's currently active REJECT decisions specifically.
claude_reject_decisions = hard_denied.get(
    "llm_decision",
    pd.Series("", index=hard_denied.index),
).astype(str).str.strip().str.upper()
claude_reject_overrides = hard_denied.get(
    "_override_target",
    pd.Series("", index=hard_denied.index),
).astype(str).str.strip()
claude_rejected = hard_denied[
    claude_reject_decisions.eq("REJECT")
    & claude_reject_overrides.eq("")
].copy()

st.markdown("#### Sourcing funnel")
funnel_1, funnel_2 = st.columns(2)

if result_scope == "Current Run":
    funnel_received = int(
        stream_status.get(
            "new_domains_received",
            stream_status.get("domains_received", 0),
        )
        or 0
    )
else:
    funnel_received = len(all_history_screened)

visible_screened = pd.concat(
    [
        investable_candidates,
        needs_review,
        hard_denied,
    ],
    ignore_index=True,
    sort=False,
)
if not visible_screened.empty and "domain" in visible_screened.columns:
    visible_screened = visible_screened.drop_duplicates("domain", keep="last")

# Websites checked is defined as this sum, not a separate worker-side
# attempt counter -- stream_status["processed"] counts every retry
# attempt on a domain, not unique resolved domains, so it could drift
# away from the bucket totals. New domains must always be >= this, since
# a domain can only reach a bucket after being received.
#
# Three buckets only (2026-08-08): a Python "reject" is retained and
# shows up as Hard Denied for real now, not purged to a tombstone behind
# a separate "auto-rejected" counter -- see automated_discard_mask in
# filter.py. That makes this sum complete on its own again.
funnel_screened = (
    len(investable_candidates) + len(needs_review) + len(hard_denied)
)

funnel_1.metric("New domains", funnel_received)
funnel_2.metric(
    "Websites checked",
    funnel_screened,
    help="Investable + Needs Review + Hard Denied -- always equal by definition.",
)

metric_1, metric_2, metric_3 = st.columns(3)
metric_1.metric("Investable", len(investable_candidates))
metric_2.metric("Needs review", len(needs_review))
metric_3.metric("Hard denied", len(hard_denied))

bucket_chart = pd.DataFrame({
    "Bucket": ["Investable", "Needs Review", "Hard Denied"],
    "Companies": [
        len(investable_candidates),
        len(needs_review),
        len(hard_denied),
    ],
})
if int(bucket_chart["Companies"].sum()) > 0:
    st.bar_chart(
        bucket_chart,
        x="Bucket",
        y="Companies",
        x_label="",
        y_label="Companies",
        height=280,
        color="#34C6B3",
    )
else:
    st.info(
        "The classification chart has no saved outcomes to display for "
        "this scope yet. Choose All History or wait for the first websites "
        "to finish screening."
    )

retained_total = funnel_screened
st.caption(
    f"The three review tabs contain {retained_total:,} retained records, "
    "equal to Websites checked by definition. Investable is a "
    "Claude/manual label applied on top of Needs Review, not an "
    "automatic outcome."
)

tabs = st.tabs([
    "Investable Candidates",
    "Needs Review",
    "Hard Denied",
    "Rejected by Claude",
    "Yes Basket",
    "No Basket",
    "Training Data",
    "Run Details",
])

def candidate_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    output = pd.DataFrame(index=frame.index)
    output["Company"] = frame.get(
        "page_title",
        frame.get("domain", ""),
    )
    output["Domain"] = frame.get("domain", "")
    output["Extension"] = (
        output["Domain"]
        .astype(str)
        .str.extract(r"(\.[^.]+)$", expand=False)
        .fillna("")
    )
    output["Score"] = pd.to_numeric(
        frame.get(
            "_fit_score_v2",
            frame.get("rule_score", 0),
        ),
        errors="coerce",
    ).fillna(0).astype(int)
    output["Description"] = frame.get("meta_description", "")
    output["Source"] = frame.get(
        "content_source",
        pd.Series("", index=frame.index),
    ).map(classification_source_label)
    output["Category"] = frame.get(
        "_fit_pillar_v2",
        pd.Series("", index=frame.index),
    ).astype(str).str.replace("_", " ").str.title()
    decisions = frame.get(
        "llm_decision",
        pd.Series("", index=frame.index),
    ).astype(str).str.upper()

    output["Claude Decision"] = decisions.replace({
        "KEEP": "KEEP",
        "REJECT": "REJECT",
        "": "Not reviewed",
        "NAN": "Not reviewed",
    })
    output["Claude Score"] = pd.to_numeric(
        frame.get(
            "llm_fit_score",
            pd.Series("", index=frame.index),
        ),
        errors="coerce",
    )
    output["Claude Summary"] = frame.get(
        "llm_product_summary",
        pd.Series("", index=frame.index),
    )
    output["Claude Reasoning"] = frame.get(
        "llm_reasoning",
        pd.Series("", index=frame.index),
    )
    output["Website"] = frame.get("final_url", "")
    return output.reset_index(drop=True)


def render_basket_editor(*, basket: str, path) -> None:
    frame = read_basket_data(path)
    label = "Yes" if basket == "yes" else "No"
    other = "No" if basket == "yes" else "Yes"
    st.metric(f"{label} basket companies", len(frame))
    if frame.empty:
        st.info(f"No companies are in the {label} basket yet.")
        return
    st.download_button(
        f"Download {label} basket CSV",
        data=frame.to_csv(index=False).encode("utf-8"),
        file_name=path.name,
        mime="text/csv",
        key=f"download_{basket}_basket",
    )
    editor = frame.copy()
    editor[f"Set {other}"] = False
    editor["Remove"] = False
    edited = st.data_editor(
        editor,
        width="stretch",
        hide_index=True,
        disabled=["domain_url"],
        column_config={
            "domain_url": st.column_config.LinkColumn("Domain URL"),
            "rating": st.column_config.NumberColumn(
                "Rating",
                min_value=0,
                max_value=100,
                step=1,
                format="%d",
            ),
            f"Set {other}": st.column_config.CheckboxColumn(
                f"Set {other}"
            ),
            "Remove": st.column_config.CheckboxColumn("Remove"),
        },
        key=f"{basket}_basket_editor",
    )
    if st.button(
        "Save table edits",
        type="primary",
        key=f"save_{basket}_basket",
    ):
        for index, row in edited.iterrows():
            if bool(row["Remove"]):
                remove_basket_entry(row["domain_url"])
            elif bool(row[f"Set {other}"]):
                save_basket_entry(row["domain_url"], other.lower(), row["rating"])
            elif int(row["rating"]) != int(frame.loc[index, "rating"]):
                save_basket_entry(row["domain_url"], basket, row["rating"])
        st.success("Basket changes saved.")
        st.rerun()

def render_candidate_table(
    frame: pd.DataFrame,
    empty_message: str,
    key_prefix: str,
    *,
    show_claude_status: bool = False,
) -> None:
    if frame.empty:
        st.info(empty_message)
        return

    table = candidate_table(frame)
    if not show_claude_status:
        table = table.drop(
            columns=[
                "Claude Decision",
                "Claude Score",
                "Claude Summary",
                "Claude Reasoning",
            ],
            errors="ignore",
        )

    render_bucket_display(
        table,
        empty_message,
        key_prefix,
        column_config={
            "Website": st.column_config.LinkColumn(
                "Website",
                display_text="Open",
            ),
            "Score": st.column_config.ProgressColumn(
                "Score",
                min_value=0,
                max_value=96,
                format="%d",
            ),
        },
    )

def manual_claude_panel(frame: pd.DataFrame) -> None:
    if frame.empty or "domain" not in frame.columns:
        return

    selectable = frame.copy()
    if "llm_decision" in selectable.columns:
        decisions = selectable[
            "llm_decision"
        ].astype(str).str.upper()
        selectable = selectable[
            ~decisions.isin({"KEEP", "REJECT"})
        ].copy()

    if selectable.empty:
        return

    labels = []
    row_map = {}
    for idx, row in selectable.iterrows():
        company = str(
            row.get("page_title")
            or row.get("domain")
            or "Unknown company"
        )
        domain = str(row.get("domain", ""))
        score = str(
            row.get(
                "_fit_score_v2",
                row.get(
                    "diligence_rank_score",
                    row.get("rule_score", ""),
                ),
            )
        )
        label = f"{company} — {domain} — score {score}"
        labels.append(label)
        row_map[label] = idx

    selected = st.selectbox(
        "Company to review with Claude",
        labels,
        key="manual_claude_company_filtered",
    )

    if st.button(
        "Send selected company through Claude",
        type="primary",
        key="manual_claude_button_filtered",
    ):
        selected_row = selectable.loc[row_map[selected]]
        with st.spinner(
            "Running one Claude diligence call..."
        ):
            ok, message = run_manual_diligence(selected_row)
        if ok:
            st.session_state["last_manual_claude_message"] = message
            st.rerun()
        else:
            st.error(message)

with tabs[0]:
    st.caption(
        "Only high-conviction companies that clear the active run "
        "filters appear here. Uncertain and mismatched companies are "
        "retained outside the visible dashboard, not mislabeled."
    )
    manual_claude_panel(investable_candidates)
    render_candidate_table(
        investable_candidates,
        "No companies have cleared the current investable filters.",
        "investable",
        show_claude_status=True,
    )

with tabs[1]:
    st.caption(
        "Everything without an explicit factual disqualifier lands here, "
        "regardless of score -- this is the default bucket, not a "
        "high-conviction shortlist. Domains scoring 70+ are automatically "
        "sent through Claude; a Claude KEEP promotes them to Investable."
    )
    render_candidate_table(
        needs_review,
        "No companies currently need review.",
        "needs_review",
        show_claude_status=True,
    )

with tabs[2]:
    st.caption(
        "Only companies with explicit factual disqualifying evidence "
        "appear here. Low score or incomplete evidence is not a denial."
    )
    if hard_denied.empty:
        st.info("No hard-denied companies.")
    else:
        denied = pd.DataFrame(index=hard_denied.index)
        denied["Company"] = hard_denied.get(
            "page_title",
            hard_denied.get("domain", ""),
        )
        denied["Domain"] = hard_denied.get("domain", "")
        denied["Score"] = pd.to_numeric(
            hard_denied.get("_fit_score_v2", 0),
            errors="coerce",
        ).fillna(0).astype(int)
        denied["Extension"] = (
            denied["Domain"]
            .astype(str)
            .str.extract(r"(\.[^.]+)$", expand=False)
            .fillna("")
        )
        denied["Reason"] = hard_denied.get(
            "routing_reason",
            "",
        )
        denied["Evidence"] = hard_denied.get(
            "denial_evidence",
            "",
        )
        denied["Description"] = hard_denied.get(
            "meta_description",
            "",
        )
        denied["Website"] = hard_denied.get(
            "final_url",
            "",
        )

        render_bucket_display(
            denied.reset_index(drop=True),
            "No hard-denied companies.",
            "hard_denied",
            column_config={
                "Score": st.column_config.ProgressColumn(
                    "Score",
                    min_value=0,
                    max_value=100,
                    format="%d",
                ),
                "Website": st.column_config.LinkColumn(
                    "Website",
                    display_text="Open",
                ),
                "Evidence": st.column_config.TextColumn(
                    "Evidence",
                    width="large",
                ),
            },
        )

with tabs[3]:
    st.caption(
        "Companies Claude explicitly marked REJECT after selective "
        "diligence. This is an audit subset of Hard Denied, not a sixth "
        "classification bucket; companies with persistent manual "
        "overrides are excluded."
    )
    if claude_rejected.empty:
        st.info("Claude has no active rejected companies in this view.")
    else:
        rejected = pd.DataFrame(index=claude_rejected.index)
        rejected["Company"] = claude_rejected.get(
            "page_title",
            claude_rejected.get("domain", ""),
        )
        rejected["Domain"] = claude_rejected.get("domain", "")
        rejected["Glasswing Score"] = pd.to_numeric(
            claude_rejected.get(
                "_fit_score_v2",
                claude_rejected.get("rule_score", 0),
            ),
            errors="coerce",
        ).fillna(0).astype(int)
        rejected["Claude Score"] = pd.to_numeric(
            claude_rejected.get(
                "llm_fit_score",
                pd.Series("", index=claude_rejected.index),
            ),
            errors="coerce",
        )
        rejected["Claude Summary"] = claude_rejected.get(
            "llm_product_summary",
            "",
        )
        rejected["Claude Reasoning"] = claude_rejected.get(
            "llm_reasoning",
            "",
        )
        rejected["Claude Concerns"] = claude_rejected.get(
            "llm_concerns",
            "",
        )
        rejected["Claude Model"] = claude_rejected.get("llm_model", "")
        rejected["Website"] = claude_rejected.get("final_url", "")

        render_bucket_display(
            rejected.reset_index(drop=True),
            "Claude has no active rejected companies in this view.",
            "claude_rejected",
            column_config={
                "Website": st.column_config.LinkColumn(
                    "Website",
                    display_text="Open",
                ),
                "Glasswing Score": st.column_config.ProgressColumn(
                    "Glasswing Score",
                    min_value=0,
                    max_value=96,
                    format="%d",
                ),
                "Claude Score": st.column_config.ProgressColumn(
                    "Claude Score",
                    min_value=0,
                    max_value=100,
                    format="%d",
                ),
                "Claude Reasoning": st.column_config.TextColumn(
                    "Claude Reasoning",
                    width="large",
                ),
                "Claude Concerns": st.column_config.TextColumn(
                    "Claude Concerns",
                    width="large",
                ),
            },
        )

with tabs[4]:
    render_basket_editor(
        basket="yes",
        path=YES_BASKET_DATA,
    )

with tabs[5]:
    st.caption("Zero is a valid rating for an obvious poor fit.")
    render_basket_editor(
        basket="no",
        path=NO_BASKET_DATA,
    )

with tabs[6]:
    st.caption(
        "Compact evidence and deterministic splits for small local models. "
        "Missing-page examples are excluded rather than taught as negative fit."
    )
    all_training, training_excluded, _ = build_training_dataset()
    training_ready = all_training[
        all_training["trust_tier"].eq("gold")
    ].copy()
    training_manifest = {
        "instruction_tuning_examples": len(
            balanced_instruction_frame(training_ready)
        )
    }
    metric_cols = st.columns(4)
    metric_cols[0].metric("Trusted model-ready", len(training_ready))
    metric_cols[1].metric(
        "Yes / No",
        f"{int((training_ready['label'] == 'yes').sum())} / "
        f"{int((training_ready['label'] == 'no').sum())}",
    )
    metric_cols[2].metric("No-evidence excluded", len(training_excluded))
    metric_cols[3].metric(
        "Balanced tuning rows",
        training_manifest["instruction_tuning_examples"],
    )
    if st.button("Rebuild training files", type="primary"):
        training_ready, training_excluded, training_manifest = write_training_exports()
        st.success(f"Built {len(training_ready)} model-ready examples.")
    trusted_yes = int((training_ready["label"] == "yes").sum())
    trusted_no = int((training_ready["label"] == "no").sum())
    st.progress(min(trusted_yes / 300, 1.0), text=f"Trusted Yes: {trusted_yes}/300")
    st.progress(min(trusted_no / 600, 1.0), text=f"Trusted No: {trusted_no}/600")
    if trusted_yes < 300 or trusted_no < 600:
        st.warning(
            "Not ready for final training yet. Review labels below; automatic "
            "rule labels are preserved separately and excluded from trusted exports."
        )
    csv_col, jsonl_col = st.columns(2)
    csv_col.download_button(
        "Download model-ready CSV",
        training_ready.to_csv(index=False).encode("utf-8"),
        file_name="glasswing_model_ready.csv",
        mime="text/csv",
    )
    tuning_download = balanced_instruction_frame(training_ready)
    jsonl_content = "\n".join(
        json.dumps({
            "messages": [
                {"role": "system", "content": "Classify Glasswing Ventures investment fit and score it 0-100."},
                {"role": "user", "content": row["input_text"]},
                {"role": "assistant", "content": row["target_json"]},
            ],
            "metadata": {"domain": row["domain"], "split": row["split"], "source": row["label_source"]},
        }, ensure_ascii=False)
        for _, row in tuning_download.iterrows()
    )
    jsonl_col.download_button(
        "Download instruction JSONL",
        (jsonl_content + ("\n" if jsonl_content else "")).encode("utf-8"),
        file_name="glasswing_instruction_tuning.jsonl",
        mime="application/jsonl",
    )
    st.dataframe(
        training_ready[[
            "domain", "split", "label", "rating", "label_source", "trust_tier",
        ]],
        width="stretch",
        hide_index=True,
    )

    st.markdown("#### Label Next")
    st.caption(
        "The highest-value unlabeled companies: likely positives, borderline "
        "cases, and places where the rule, learned, or Claude signals disagree. "
        "Only checked Yes/No decisions become Gold human labels."
    )
    review_queue = build_active_review_queue(250)
    if review_queue.empty:
        st.info("No evidence-backed companies are waiting for human review.")
    else:
        review_editor = st.data_editor(
            review_queue,
            width="stretch",
            hide_index=True,
            disabled=[
                "Domain", "Company", "Current Label", "Description",
                "Website", "Suggested Score", "Python suggestion",
                "Why review", "Claude", "Evidence confidence",
            ],
            column_config={
                "Review": st.column_config.CheckboxColumn("Save"),
                "Decision": st.column_config.SelectboxColumn(
                    "Human decision",
                    options=["", "yes", "no"],
                    required=False,
                ),
                "Rating": st.column_config.NumberColumn(
                    "Human score", min_value=0, max_value=100, step=1,
                ),
                "Reason": st.column_config.SelectboxColumn(
                    "Primary reason",
                    options=[
                        "", "enterprise_buyer", "strong_technical_moat",
                        "cybersecurity", "vertical_ai", "physical_ai",
                        "ai_infrastructure", "consumer_product",
                        "agency_or_services", "weak_technical_moat",
                        "too_mature_or_funded", "non_us",
                        "insufficient_evidence", "other",
                    ],
                    required=False,
                ),
                "Website": st.column_config.LinkColumn("Website"),
            },
            key="active_training_review_editor",
        )
        if st.button("Save reviewed labels", type="primary"):
            selected = review_editor[review_editor["Review"]]
            invalid = selected[selected["Decision"].astype(str).eq("")]
            if not invalid.empty:
                st.error("Every checked row needs a Yes or No decision.")
            else:
                for label in ("yes", "no"):
                    group = selected[
                        selected["Decision"].astype(str).eq(label)
                    ]
                    if group.empty:
                        continue
                    for reason, reason_group in group.groupby(
                        group["Reason"].astype(str).replace(
                            "", "human_reviewed"
                        )
                    ):
                        save_basket_entries(
                            [
                                (
                                    stable_domain(row["Domain"]),
                                    int(row["Rating"]),
                                )
                                for _, row in reason_group.iterrows()
                            ],
                            label,
                            source="human",
                            reason=str(reason),
                        )
                st.success(f"Saved {len(selected)} Gold human labels.")
                st.rerun()

with tabs[7]:
    left, right = st.columns(2)
    with left:
        st.markdown("#### Pipeline status")
        st.json(stream_status)
    with right:
        st.markdown("#### Active run filters")
        st.json(load_dashboard_run_config())

    claude_warning = str(
        stream_status.get("claude_warning", "")
    ).strip()
    if claude_warning:
        st.info(claude_warning)

    st.markdown("#### New-domain registry")
    registry_cols = st.columns(3)
    registry_cols[0].metric(
        "Certificate events",
        int(
            stream_status.get(
                "certificate_events_received",
                0,
            )
            or 0
        ),
    )
    registry_cols[1].metric(
        "New domains queued",
        int(
            stream_status.get(
                "new_domains_received",
                stream_status.get("domains_received", 0),
            )
            or 0
        ),
    )
    registry_cols[2].metric(
        "Repeat events ignored",
        int(
            stream_status.get(
                "repeat_events_ignored",
                0,
            )
            or 0
        ),
    )

    st.markdown("#### Visible classification reconciliation")
    reconciliation = pd.DataFrame([
        {"Bucket": "Investable", "Count": len(investable_candidates)},
        {"Bucket": "Needs review", "Count": len(needs_review)},
        {"Bucket": "Hard denied", "Count": len(hard_denied)},
        {"Bucket": "Retained total", "Count": retained_total},
    ])
    st.dataframe(
        reconciliation,
        width="stretch",
        hide_index=True,
    )

    st.markdown("#### Classification quality")
    quality_cols = st.columns(3)
    inaccessible_count = int(
        needs_review.get(
            "routing_reason",
            pd.Series("", index=needs_review.index),
        )
        .astype(str)
        .eq("website_unavailable_or_no_usable_company_evidence")
        .sum()
    )
    low_signal_count = int(
        needs_review.get(
            "routing_reason",
            pd.Series("", index=needs_review.index),
        )
        .astype(str)
        .eq("no_clear_enterprise_or_glasswing_fit")
        .sum()
    )
    review_with_clear_reason = int(
        needs_review.get(
            "routing_reason",
            pd.Series("", index=needs_review.index),
        ).astype(str).str.strip().ne("").sum()
    )
    quality_cols[0].metric("Inaccessible / no evidence", inaccessible_count)
    quality_cols[1].metric("Readable but low signal", low_signal_count)
    quality_cols[2].metric(
        "Review rows with a reason",
        f"{review_with_clear_reason}/{len(needs_review)}",
    )
    st.caption(
        "Use these distributions to identify systematic fetch failures or "
        "classification rules that deserve calibration. Counts respect the "
        "current-run/history scope and persistent manual overrides."
    )

    reason_left, reason_right = st.columns(2)
    with reason_left:
        st.markdown("##### Hard-denial reasons")
        safe_dataframe(
            classification_reason_summary(hard_denied),
            width="stretch",
            hide_index=True,
        )
    with reason_right:
        st.markdown("##### Needs-review reasons")
        safe_dataframe(
            classification_reason_summary(needs_review),
            width="stretch",
            hide_index=True,
        )

    with st.expander(
        "Latest continuous pipeline log",
        expanded=False,
    ):
        continuous_log = (
            ROOT / "logs" / "continuous_pipeline.log"
        )
        if continuous_log.exists():
            lines = continuous_log.read_text(
                encoding="utf-8",
                errors="ignore",
            ).splitlines()
            st.code(
                "\n".join(lines[-200:])
                or "No log entries yet."
            )
        else:
            st.caption("No pipeline log yet.")

# Mount the refresh timer only after the complete page has rendered. All
# History can require a substantial shared-store read and score refresh; when
# this timer lived near the top of the script, its next tick could interrupt
# that work before the funnel/chart/tabs were ever emitted, leaving the page
# permanently truncated at the Results view caption.
if running:
    st_autorefresh(
        interval=30_000,
        limit=None,
        key="pipeline_dashboard_refresh_30s",
    )
