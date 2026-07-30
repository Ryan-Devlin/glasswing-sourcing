from __future__ import annotations

import pandas as pd
import streamlit as st

from health_monitor import health_report
from manual_truth_backup import (
    create_manual_snapshot,
    list_manual_snapshots,
    restore_manual_snapshot,
    validate_manual_snapshot,
)
from model_evaluation import write_performance_metrics
from rejection_cleanup import rebuild_full_text_indexes
from runtime_control import (
    RUNTIME_MODE,
    SERVICES,
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
    div[data-testid="stMetric"]{border:1px solid rgba(128,128,128,.18);
    border-radius:10px;padding:.55rem .75rem}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False, ttl=15)
def cached_health(include_integrity: bool = False):
    return health_report(include_integrity=include_integrity)


st.title("System Health")
st.caption(
    "One operational view for the dashboard, workers, queues, search indexes, "
    "human ground truth, and frozen-model feedback."
)

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
])

with tabs[0]:
    st.subheader("Processing controls")
    runtime = report["runtime"]
    if RUNTIME_MODE != "local":
        st.info(
            "Worker lifecycle is delegated to the cloud runtime. This page "
            "remains read-only; Railway or the future container platform "
            "starts and restarts worker services."
        )
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
        service_names = {
            "pipeline": "New-domain screening",
            "ai_crawler": ".AI discovery crawler",
            "zone_crawler": ".COM/.APP zone crawler",
        }
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

with tabs[4]:
    st.subheader("Cloud migration boundary")
    st.caption(
        "The UI now talks to explicit truth, runtime-control, health, and "
        "evaluation modules. In the cloud, their local implementations can be "
        "replaced independently without changing the product pages."
    )
    st.dataframe(
        pd.DataFrame([
            {
                "Local component": "SQLite classifications and manual ledger",
                "Cloud replacement": "Managed PostgreSQL",
            },
            {
                "Local component": "Compressed HTML archives",
                "Cloud replacement": "S3-compatible object storage",
            },
            {
                "Local component": "Worker supervisor and PID files",
                "Cloud replacement": "Railway worker services / restart policy",
            },
            {
                "Local component": "Queue SQLite and zone shards",
                "Cloud replacement": "PostgreSQL queue or managed message queue",
            },
            {
                "Local component": "Checksummed manual snapshots",
                "Cloud replacement": "Versioned private object-storage bucket",
            },
        ]),
        hide_index=True,
        width="stretch",
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
