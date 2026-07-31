"""Portable, checksummed backups for human-authored decisions only."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from classification_store import CLASSIFICATION_DB, connect_store, utc_now
from common import (
    MANUAL,
    NO_BASKET_DATA,
    YES_BASKET_DATA,
    atomic_write_csv,
    label_metadata_path,
    read_label_metadata,
    stable_domain,
)
from db_backend import begin_immediate, using_postgres
from object_storage import (
    exists,
    list_keys,
    read_bytes,
    using_object_storage,
    write_bytes,
)


SNAPSHOT_ROOT = Path(
    os.getenv(
        "GLASSWING_MANUAL_SNAPSHOT_ROOT",
        str(MANUAL / "snapshots"),
    )
).expanduser().resolve()
# S3 keys always use '/' regardless of OS; this prefix is only consulted
# when using_object_storage() is true, so it never touches SNAPSHOT_ROOT.
OBJECT_STORAGE_SNAPSHOT_PREFIX = os.getenv(
    "GLASSWING_MANUAL_SNAPSHOT_PREFIX", "manual-snapshots"
)


def _snapshot_root_key(snapshot_root: Path | str | None = None) -> str:
    if snapshot_root is not None:
        return str(snapshot_root)
    if using_object_storage():
        return OBJECT_STORAGE_SNAPSHOT_PREFIX
    return str(SNAPSHOT_ROOT)
HUMAN_SOURCES = {
    "human",
    "manual",
    "glasswing_dashboard",
    "ai_search_page",
    "zone_universe_page",
    "legacy_manual_override",
    "human_restore",
}
SCHEMA_VERSION = 1


def _canonical_bytes(payload: dict[str, object]) -> bytes:
    clean_payload = {
        key: value
        for key, value in payload.items()
        if key != "sha256"
    }
    return json.dumps(
        clean_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: dict[str, object]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read_csv(path: Path) -> list[dict[str, object]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        return pd.read_csv(path, dtype=str).fillna("").to_dict("records")
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return []


def create_manual_snapshot(
    *,
    reason: str,
    classification_db: Path | str = CLASSIFICATION_DB,
    snapshot_root: Path | str | None = None,
) -> Path:
    """Save a complete portable copy containing no automated labels."""
    database = Path(classification_db)
    decisions: list[dict[str, object]] = []
    movements: list[dict[str, object]] = []
    if using_postgres() or database.exists():
        with connect_store(database, read_only=True) as conn:
            decisions = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT domain,target_label,target_bucket,objective_score,
                           note,source,decided_at
                    FROM manual_decisions
                    ORDER BY domain
                    """
                )
                if str(row["source"]).lower() in HUMAN_SOURCES
            ]
            movements = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT id,domain,from_bucket,to_bucket,objective_score,
                           note,source,moved_at,predicted_bucket,model_version
                    FROM movement_log
                    ORDER BY id
                    """
                )
                if str(row["source"]).lower() in HUMAN_SOURCES
            ]
    metadata = read_label_metadata()
    human_labels = (
        metadata[
            metadata["source"].astype(str).str.lower().isin(HUMAN_SOURCES)
        ].to_dict("records")
        if not metadata.empty
        else []
    )
    overrides = _read_csv(MANUAL / "domain_overrides.csv")
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reason": str(reason),
        "manual_decisions": decisions,
        "movement_log": movements,
        "human_training_labels": human_labels,
        "domain_overrides": overrides,
        "counts": {
            "manual_decisions": len(decisions),
            "movement_events": len(movements),
            "human_training_labels": len(human_labels),
            "domain_overrides": len(overrides),
        },
    }
    payload["sha256"] = _sha256(payload)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    filename = f"manual-truth-{stamp}-{uuid.uuid4().hex[:8]}.json"
    destination_key = f"{_snapshot_root_key(snapshot_root)}/{filename}"
    payload_bytes = (
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    write_bytes(destination_key, payload_bytes)
    write_bytes(
        destination_key + ".sha256",
        f"{payload['sha256']}  {filename}\n".encode("utf-8"),
    )
    return Path(destination_key)


def validate_manual_snapshot(path: Path | str) -> dict[str, object]:
    key = str(path)
    payload = json.loads(read_bytes(key).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Snapshot root must be a JSON object.")
    if int(payload.get("schema_version", 0)) != SCHEMA_VERSION:
        raise ValueError("Unsupported manual snapshot schema.")
    expected = str(payload.get("sha256", ""))
    actual = _sha256(payload)
    if not expected or expected != actual:
        raise ValueError("Manual snapshot checksum does not match.")
    sidecar_key = key + ".sha256"
    if exists(sidecar_key):
        sidecar_hash = read_bytes(sidecar_key).decode("utf-8").split()[0]
        if sidecar_hash != actual:
            raise ValueError("Manual snapshot sidecar checksum does not match.")
    return payload


def list_manual_snapshots(
    snapshot_root: Path | str | None = None,
) -> list[dict[str, object]]:
    prefix = _snapshot_root_key(snapshot_root)
    keys = sorted(
        list_keys(prefix, suffix=".json"),
        reverse=True,
    )
    rows: list[dict[str, object]] = []
    for key in keys:
        path = Path(key)
        try:
            payload = validate_manual_snapshot(path)
            rows.append({
                "path": path,
                "name": path.name,
                "created_at": payload.get("created_at", ""),
                "reason": payload.get("reason", ""),
                "counts": payload.get("counts", {}),
                "valid": True,
            })
        except Exception as exc:
            # One bad snapshot must not break the listing -- every failure
            # mode (local I/O errors, object-storage client errors, bad
            # JSON, checksum mismatch) surfaces as "invalid" for this row
            # alone.
            rows.append({
                "path": path,
                "name": path.name,
                "created_at": "",
                "reason": "",
                "counts": {},
                "valid": False,
                "error": str(exc),
            })
    return rows


def restore_manual_snapshot(
    path: Path | str,
    *,
    classification_db: Path | str = CLASSIFICATION_DB,
) -> dict[str, int | str]:
    """Restore current human decisions without rewriting append-only history."""
    payload = validate_manual_snapshot(path)
    create_manual_snapshot(
        reason=f"before_restore:{Path(path).name}",
        classification_db=classification_db,
    )
    decisions = list(payload.get("manual_decisions", []))
    human_labels = list(payload.get("human_training_labels", []))
    overrides = list(payload.get("domain_overrides", []))
    database = Path(classification_db)
    with connect_store(database) as conn:
        begin_immediate(conn)
        conn.execute(
            "DELETE FROM manual_decisions WHERE lower(source) IN "
            f"({','.join('?' for _ in HUMAN_SOURCES)})",
            tuple(sorted(HUMAN_SOURCES)),
        )
        for row in decisions:
            domain = stable_domain(row.get("domain", ""))
            if not domain:
                continue
            conn.execute(
                """
                INSERT INTO manual_decisions(
                    domain,target_label,target_bucket,objective_score,
                    note,source,decided_at
                ) VALUES (?,?,?,?,?,'human',?)
                """,
                (
                    domain,
                    str(row.get("target_label", "")),
                    str(row.get("target_bucket", "")),
                    int(row.get("objective_score", 0) or 0),
                    str(row.get("note", "")),
                    str(row.get("decided_at", "")) or utc_now(),
                ),
            )
            current = conn.execute(
                "SELECT routing_bucket,model_version "
                "FROM classifications WHERE domain=?",
                (domain,),
            ).fetchone()
            from_bucket = str(current["routing_bucket"]) if current else ""
            model_version = str(current["model_version"]) if current else ""
            conn.execute(
                """
                INSERT INTO movement_log(
                    domain,from_bucket,to_bucket,objective_score,
                    note,source,moved_at,predicted_bucket,model_version
                ) VALUES (?,?,?,?,?,'human_restore',?,?,?)
                """,
                (
                    domain,
                    from_bucket,
                    str(row.get("target_bucket", "")),
                    int(row.get("objective_score", 0) or 0),
                    f"restored_from:{Path(path).name}",
                    utc_now(),
                    from_bucket,
                    model_version,
                ),
            )
        conn.commit()

    override_columns = [
        "domain", "override_target", "override_note", "updated_at",
    ]
    override_frame = pd.DataFrame(overrides)
    for column in override_columns:
        if column not in override_frame:
            override_frame[column] = ""
    atomic_write_csv(
        MANUAL / "domain_overrides.csv",
        override_frame[override_columns],
    )

    metadata = read_label_metadata()
    portfolio = metadata[
        metadata["source"].astype(str).eq("glasswing_portfolio")
    ].copy()
    restored_labels = pd.DataFrame(human_labels)
    label_columns = [
        "domain", "label", "rating", "source", "reason", "labeled_at",
    ]
    for column in label_columns:
        if column not in restored_labels:
            restored_labels[column] = ""
    if not restored_labels.empty:
        restored_labels["source"] = "human"
    restored_metadata = pd.concat(
        [portfolio[label_columns], restored_labels[label_columns]],
        ignore_index=True,
    ).drop_duplicates("domain", keep="last")
    atomic_write_csv(label_metadata_path(), restored_metadata)

    from training_truth import rebuild_human_ground_truth

    rebuilt = rebuild_human_ground_truth(
        classification_db=classification_db
    )
    create_manual_snapshot(
        reason=f"after_restore:{Path(path).name}",
        classification_db=classification_db,
    )
    return {
        "restored_decisions": len(decisions),
        "restored_training_labels": len(human_labels),
        "ground_truth": int(rebuilt["ground_truth"]),
        "snapshot": str(path),
    }
