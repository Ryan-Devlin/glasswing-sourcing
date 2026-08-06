from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# health_monitor imports ai_search.database and zone_universe.database.
# Unlike overview.py/ai_search.py/zone_universe.py, this page never set up
# sys.path itself -- it silently relied on one of those other pages having
# already run first in the same Streamlit process. Landing here directly
# on a fresh process (e.g. right after a deploy) crashed with
# ModuleNotFoundError: No module named 'ai_search'.
ROOT = Path(__file__).resolve().parents[1]
AI_SRC = ROOT / "ai-domain-search-engine" / "src"
ZONE_SRC = ROOT / "zone-universe" / "src"
for source in (ROOT, AI_SRC, ZONE_SRC):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from classification_store import delete_bucket_classifications, recent_digest_runs
from continuous_pipeline import connect_db, discard_pending_by_extension
from db_backend import using_postgres
from health_monitor import health_report
from manual_truth_backup import (
    create_manual_snapshot,
    list_manual_snapshots,
    restore_manual_snapshot,
    validate_manual_snapshot,
)
from model_evaluation import write_performance_metrics
from object_storage import using_object_storage
from rejection_cleanup import rebuild_full_text_indexes
from runtime_control import (
    RUNTIME_MODE,
    SERVICES,
    control_backend_available,
    ensure_supervisor,
    recover_stuck_jobs,
    request_services,
    stop_supervisor,
)


st.markdown(
    """
    <style>
    .block-container{max-width:1750px;padding:1rem 1.5rem 3rem}
    h1{font-size:1.9rem!important;margin-bottom:.1rem!important}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False, ttl=15)
def cached_health(include_integrity: bool = False):
    return health_report(include_integrity=include_integrity)


st.title("System Health")

header_left, header_middle, header_right = st.columns([1, 1, 4])
run_integrity = header_left.button("Run full checks", type="primary")
if header_middle.button("Refresh"):
    st.cache_data.clear()
    st.rerun()
report = cached_health(run_integrity)
overall = str(report["overall"])
{
    "Healthy": header_right.success,
    "Warning": header_right.warning,
    "Error": header_right.error,
}[overall](f"Overall status: **{overall}**")

counts = report["counts"]
metrics = st.columns(6)
for column, (label, value) in zip(metrics, [
    ("Companies classified", counts["classifications"]),
    ("Human decisions", counts["manual_decisions"]),
    ("Pipeline pending", counts["pipeline_pending"]),
    (".AI searchable", counts["ai_indexed"]),
    (".COM/.APP searchable", counts["zone_indexed"]),
    ("Stuck claims", counts["pipeline_processing"]),
]):
    column.metric(label, f"{int(value):,}")

tabs = st.tabs([
    "Workers",
    "Health checks",
    "Model performance",
    "Human backups",
    "Cloud readiness",
    "Daily digest",
])

with tabs[0]:
    st.subheader("Processing controls")
    runtime = report["runtime"]
    service_names = {
        "pipeline": "New-domain screening",
        "ai_crawler": ".AI discovery crawler",
        "zone_crawler": ".COM/.APP zone crawler",
    }
    if RUNTIME_MODE != "local":
        if not control_backend_available():
            st.info(
                "Worker containers are managed by Railway, and pause/resume "
                "isn't wired up yet -- set RUNTIME_CONTROL_DATABASE_URL to "
                "enable the controls below."
            )
        else:
            st.caption(
                "Railway keeps each worker's container running; these "
                "controls pause or resume that worker's own processing loop "
                "via a shared flag. Start doesn't spawn a container and "
                "Stop doesn't terminate one -- the container stays up, idle, "
                "and picks back up the moment you resume it."
            )
            start_col, pause_col = st.columns(2)
            if start_col.button(
                "Start / resume everything",
                type="primary",
                width="stretch",
            ):
                request_services(
                    {service: True for service in SERVICES},
                    requested_by="system_health_start_all",
                )
                st.cache_data.clear()
                st.success("Resume requested for every worker.")
                st.rerun()
            if pause_col.button("Pause everything safely", width="stretch"):
                request_services(
                    {service: False for service in SERVICES},
                    requested_by="system_health_pause_all",
                )
                st.cache_data.clear()
                st.info("Pause requested for every worker.")
                st.rerun()

            st.markdown("#### Individual services")
            for service in SERVICES:
                row = runtime["services"][service]
                label_col, status_col, start_col, pause_col = st.columns(
                    [3, 2, 1, 1]
                )
                label_col.write(f"**{service_names[service]}**")
                status_col.write(
                    "Desired: running" if row["desired"] else "Desired: paused"
                )
                if start_col.button(
                    "Start",
                    key=f"start_{service}",
                    disabled=bool(row["desired"]),
                    width="stretch",
                ):
                    request_services(
                        {service: True},
                        requested_by="system_health_start_one",
                    )
                    st.cache_data.clear()
                    st.rerun()
                if pause_col.button(
                    "Pause",
                    key=f"pause_{service}",
                    disabled=not row["desired"],
                    width="stretch",
                ):
                    request_services(
                        {service: False},
                        requested_by="system_health_pause_one",
                    )
                    st.cache_data.clear()
                    st.rerun()
    else:
        start_col, pause_col, recover_col = st.columns(3)
        if start_col.button(
            "Start / resume everything",
            type="primary",
            width="stretch",
        ):
            request_services(
                {service: True for service in SERVICES},
                requested_by="system_health_start_all",
            )
            ensure_supervisor()
            st.cache_data.clear()
            st.success("All services requested. The supervisor will recover them.")
            st.rerun()
        if pause_col.button("Pause everything safely", width="stretch"):
            request_services(
                {service: False for service in SERVICES},
                requested_by="system_health_pause_all",
            )
            ensure_supervisor()
            st.cache_data.clear()
            st.info("Safe stop requested for every worker.")
            st.rerun()
        if recover_col.button("Recover stuck jobs", width="stretch"):
            recovered = recover_stuck_jobs()
            st.cache_data.clear()
            st.success(
                f"Recovered {recovered['pipeline']} pipeline claim(s) and "
                f"{recovered['registration']} registration lookup(s)."
            )
            st.rerun()

        st.markdown("#### Individual services")
        service_rows = []
        for service in SERVICES:
            row = runtime["services"][service]
            service_rows.append({
                "Service": service_names[service],
                "Desired": "Running" if row["desired"] else "Paused",
                "Actual": "Running" if row["running"] else "Stopped",
                "PID": row["pid"] or "",
            })
        st.dataframe(
            pd.DataFrame(service_rows),
            hide_index=True,
            width="stretch",
        )
        supervisor = runtime["supervisor"]
        untracked_pids = supervisor.get("untracked_pids", [])
        supervisor_col, stop_col = st.columns([4, 1])
        if supervisor.get("running"):
            supervisor_col.success(
                f"Automatic recovery supervisor running · PID "
                f"{supervisor.get('pid', '')}"
            )
        else:
            supervisor_col.warning(
                "Automatic recovery supervisor is stopped. It starts "
                "automatically when you use the controls above."
            )
        if untracked_pids:
            supervisor_col.error(
                "Found "
                f"{len(untracked_pids)} supervisor process(es) running "
                f"outside tracking (PID {', '.join(map(str, untracked_pids))}). "
                "Use \"Stop supervisor\" to clean this up."
            )
        if stop_col.button(
            "Stop supervisor",
            width="stretch",
            disabled=not (supervisor.get("running") or untracked_pids),
        ):
            stopped_cleanly = stop_supervisor()
            st.cache_data.clear()
            if stopped_cleanly:
                st.success("Supervisor stopped.")
            else:
                st.error(
                    "Supervisor did not stop cleanly; a process may have "
                    "needed a forced kill. Refresh to confirm."
                )
            st.rerun()

    st.markdown("#### New-domain queue cleanup")
    st.caption(
        "Discards not-yet-attempted domains for one TLD from the "
        "new-domain screening queue. Only affects undiscovered/unscored "
        "hostnames waiting to be checked -- never classifications, "
        "training truth, or any human decision. Use this after an "
        "Extensions misconfiguration leaves a stale backlog for a TLD "
        "you no longer intend to capture."
    )
    queue_extension = st.text_input(
        "TLD to discard (e.g. .co)",
        key="queue_cleanup_extension",
    )
    queue_confirm = st.checkbox(
        "I understand this discards every not-yet-attempted domain for "
        "this TLD from the queue.",
        key="queue_cleanup_confirm",
    )
    if st.button(
        "Discard pending queue for this TLD",
        disabled=not (queue_confirm and queue_extension.strip()),
    ):
        conn = connect_db()
        try:
            discarded = discard_pending_by_extension(
                conn, queue_extension.strip()
            )
        finally:
            conn.close()
        st.cache_data.clear()
        st.success(
            f"Discarded {discarded:,} pending {queue_extension.strip()} "
            "domains from the queue."
        )
        st.rerun()

    st.markdown("#### Wipe the entire new-domain queue")
    st.caption(
        "Discards every not-yet-attempted domain in the new-domain "
        "screening queue, regardless of TLD. Only affects undiscovered/"
        "unscored hostnames waiting to be checked -- never "
        "classifications, training truth, or any human decision. New "
        "CertStream events keep arriving and refill the queue normally "
        "afterward."
    )
    wipe_confirm = st.checkbox(
        "I understand this discards every not-yet-attempted domain in "
        "the entire queue, for every TLD.",
        key="queue_wipe_confirm",
    )
    if st.button(
        "Wipe entire new-domain queue",
        disabled=not wipe_confirm,
    ):
        conn = connect_db()
        try:
            discarded = discard_pending_by_extension(conn, None)
        finally:
            conn.close()
        st.cache_data.clear()
        st.success(f"Discarded {discarded:,} pending domains from the queue.")
        st.rerun()

with tabs[1]:
    st.subheader("Readiness checks")
    st.dataframe(
        pd.DataFrame(report["checks"]),
        hide_index=True,
        width="stretch",
    )
    if st.button("Repair search indexes"):
        repaired = rebuild_full_text_indexes()
        st.cache_data.clear()
        st.success(
            f"Rebuilt {repaired['ai_indexed_pages']:,} .AI and "
            f"{repaired['zone_indexed_pages']:,} Zone index rows."
        )
        st.rerun()
    with st.expander("Raw pipeline status"):
        st.json(report["pipeline_status"])

with tabs[2]:
    st.subheader("Frozen-model feedback")
    performance = report["performance"]
    cols = st.columns(6)
    for column, (label, value) in zip(cols, [
        ("Model version", performance["model_version"]),
        ("Human decisions", performance["human_decisions"]),
        (
            "Agreement",
            (
                f"{float(performance['agreement_rate']):.1%}"
                if performance["agreement_rate"] is not None
                else "Not enough data"
            ),
        ),
        ("False-negative reversals", performance["false_negative_reversals"]),
        ("False-positive reversals", performance["false_positive_reversals"]),
        ("Investable / 1,000", performance["investable_per_1000"]),
    ]):
        column.metric(label, value)
    st.caption(
        "A false-negative reversal means a human moved a Python Hard Denial "
        "to Investable. This is the most important safety metric."
    )
    by_extension = performance.get("by_extension", {})
    if by_extension:
        extension_frame = pd.DataFrame.from_dict(
            by_extension,
            orient="index",
        ).reset_index(names="Extension")
        st.dataframe(extension_frame, hide_index=True, width="stretch")
    if st.button("Write metrics snapshot"):
        saved = write_performance_metrics()
        st.success(
            f"Saved metrics for {saved['human_decisions']} human decisions."
        )

with tabs[3]:
    st.subheader("Human ground-truth backups")
    st.caption(
        "Snapshots contain explicit human decisions and human training labels "
        "only—never Python or LLM classifications. Every manual change creates "
        "a checksummed snapshot automatically."
    )
    if st.button("Create snapshot now", type="primary"):
        path = create_manual_snapshot(reason="manual_system_health_snapshot")
        st.cache_data.clear()
        st.success(f"Created {path.name}")
        st.rerun()
    snapshots = list_manual_snapshots()
    if not snapshots:
        st.info("No manual snapshot exists yet.")
    else:
        snapshot_table = pd.DataFrame([
            {
                "Snapshot": row["name"],
                "Created": row["created_at"],
                "Reason": row["reason"],
                "Valid": row["valid"],
                "Human decisions": row.get("counts", {}).get(
                    "manual_decisions", 0
                ),
                "Training labels": row.get("counts", {}).get(
                    "human_training_labels", 0
                ),
            }
            for row in snapshots[:100]
        ])
        st.dataframe(snapshot_table, hide_index=True, width="stretch")
        valid = [row for row in snapshots if row["valid"]]
        if not valid:
            st.error(
                "No valid snapshot is available to restore. Create a new "
                "snapshot after checking the current manual ledger."
            )
        else:
            selected_name = st.selectbox(
                "Restore snapshot",
                [row["name"] for row in valid],
            )
            selected = next(
                row for row in valid if row["name"] == selected_name
            )
            try:
                validate_manual_snapshot(selected["path"])
                st.success("Checksum valid")
            except ValueError as exc:
                st.error(str(exc))
            confirm = st.checkbox(
                "I understand this replaces current manual decisions and labels."
            )
            if st.button(
                "Restore selected snapshot",
                disabled=not confirm,
            ):
                result = restore_manual_snapshot(selected["path"])
                st.cache_data.clear()
                st.success(
                    f"Restored {result['restored_decisions']} decisions and "
                    f"{result['restored_training_labels']} training labels."
                )
                st.rerun()

    st.markdown("#### Bulk bucket cleanup")
    st.caption(
        "Permanently deletes every classification currently in one bucket. "
        "Explicit human decisions (Mark Yes/No, manual moves) and verified "
        "training truth are never deleted by this, regardless of bucket."
    )
    bucket_to_delete = st.selectbox(
        "Bucket",
        ["Potential Comp", "Coming Soon", "Needs Review", "Insufficient Evidence", "Hard Denied"],
        key="bucket_cleanup_choice",
    )
    bucket_codes = {
        "Potential Comp": "comp",
        "Coming Soon": "coming_soon",
        "Needs Review": "review",
        "Insufficient Evidence": "insufficient",
        "Hard Denied": "reject",
    }
    delete_confirm = st.checkbox(
        f"I understand this permanently deletes {bucket_to_delete} "
        "classifications and cannot be undone.",
        key="bucket_cleanup_confirm",
    )
    if st.button(
        f"Delete all {bucket_to_delete} classifications",
        disabled=not delete_confirm,
    ):
        snapshot_path = create_manual_snapshot(
            reason=f"pre_bucket_delete_{bucket_codes[bucket_to_delete]}"
        )
        deleted = delete_bucket_classifications(bucket_codes[bucket_to_delete])
        st.cache_data.clear()
        st.success(
            f"Deleted {deleted:,} {bucket_to_delete} classifications. "
            f"Safety snapshot saved first: {snapshot_path.name}"
        )
        st.rerun()

with tabs[4]:
    st.subheader("Cloud migration boundary")
    subsystems = [
        ("Classifications and manual ledger", "DATABASE_URL", "classification_store"),
        ("New-domain screening pipeline", "PIPELINE_DATABASE_URL", "pipeline"),
        (".AI discovery search index", "AI_SEARCH_DATABASE_URL", "ai_search"),
        (".COM/.APP zone universe", "ZONE_UNIVERSE_DATABASE_URL", "zone_universe"),
    ]
    on_postgres = 0
    rows = []
    for label, env_var, schema in subsystems:
        cloud = using_postgres(env_var)
        on_postgres += int(cloud)
        rows.append({
            "Subsystem": label,
            "Backend": "PostgreSQL" if cloud else "Local SQLite",
            "Schema / file": f"schema \"{schema}\"" if cloud else f"{schema}.db",
        })
    object_storage_on = using_object_storage()
    rows.append({
        "Subsystem": "HTML archives",
        "Backend": "S3-compatible object storage" if object_storage_on else "Local files",
        "Schema / file": "bucket (per-page objects)" if object_storage_on else "sharded .htmlpack files",
    })

    summary_cols = st.columns(3)
    summary_cols[0].metric("Databases on Postgres", f"{on_postgres}/4")
    summary_cols[1].metric(
        "Archives on object storage",
        "Yes" if object_storage_on else "No",
    )
    summary_cols[2].metric(
        "Worker supervision",
        "Cloud-managed" if RUNTIME_MODE != "local" else "Local supervisor",
    )

    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    if on_postgres == 4 and object_storage_on:
        st.success(
            "Every subsystem is live on managed Postgres and object storage. "
            "Local SQLite/file mode remains the zero-config fallback."
        )
    elif on_postgres == 0 and not object_storage_on:
        st.info("Running fully in local mode: SQLite + local files, no cloud env vars set.")
    else:
        st.warning("A mix of cloud and local backends is active -- see the table above.")

    if on_postgres > 0:
        st.markdown("#### Postgres backups")
        st.caption("On-demand only -- there's no automatic schedule yet.")
        if st.button("Back up Postgres now"):
            import backup_database
            with st.spinner("Copying tables out of Postgres..."):
                try:
                    result = backup_database.create_database_backup()
                except Exception as exc:
                    st.error(f"Backup failed: {exc}")
                else:
                    st.success(
                        "Backed up: " + ", ".join(
                            f"{name} → {key}" for name, key in result.items()
                        )
                    )

    st.code(
        "\n".join([
            f"GLASSWING_RUNTIME_MODE={RUNTIME_MODE}",
            f"GLASSWING_DATA_ROOT={report['paths']['data_root']}",
            f"GLASSWING_LOG_ROOT={report['paths']['log_root']}",
            f"GLASSWING_CLASSIFICATION_DB={report['paths']['classification_db']}",
        ]),
        language="bash",
    )

with tabs[5]:
    st.subheader("Daily digest runs")
    st.caption(
        "History of daily_digest.py's cron runs -- previously written but "
        "never surfaced anywhere, so a failed run was invisible."
    )
    if st.button("Refresh digest history"):
        st.cache_data.clear()
        st.rerun()
    runs = recent_digest_runs(limit=20)
    if not runs:
        st.info(
            "No digest runs recorded yet. If the cron job has already run "
            "today, this means it never reached run_digest() at all -- "
            "check its own logs/env vars, since it fails before any row "
            "is written on a startup-time error (e.g. a missing setting)."
        )
    else:
        latest = runs[0]
        if latest["status"] == "failed":
            st.error(f"Most recent run failed: {latest['error'] or 'no error message recorded'}")
        table = pd.DataFrame([
            {
                "Status": row["status"],
                "Started": row["started_at"],
                "Completed": row["completed_at"],
                "Candidates": row["candidate_count"],
                "Sent": row["sent_count"],
                "Recipients": row["recipient_count"],
                "Error": row["error"],
            }
            for row in runs
        ])
        st.dataframe(table, hide_index=True, width="stretch")
