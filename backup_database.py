"""On-demand logical backup of every Postgres-backed subsystem.

Only meaningful in cloud mode: local SQLite mode is already one
self-contained file per subsystem, and human-authored decisions
additionally get their own checksummed snapshots via manual_truth_backup.py.
This covers the Postgres-only case -- a managed database with no local file
a person could simply copy.

Each subsystem gets its own dedicated env-var gate (never DATABASE_URL for
anything but the classification store -- see db_backend.using_postgres),
so this only backs up whichever subsystems are actually cut over to
Postgres; a subsystem still on local SQLite is silently skipped (nothing to
back up here -- the SQLite file itself is the backup unit for that one).

Each backup is a zip of one CSV per table plus a manifest, produced via
Postgres's native COPY (no `pg_dump` binary required, so this runs
identically on Railway, in CI, and on a developer's Mac), written through
the same dual local-file-or-S3 backend as manual snapshots, with a sha256
sidecar for tamper detection.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from classification_store import utc_now
from common import DATA
from db_backend import connect_postgres, using_postgres
from object_storage import using_object_storage, write_bytes


@dataclass(frozen=True)
class BackupTarget:
    name: str
    env_var: str
    schema: str
    tables: tuple[str, ...]


TARGETS: tuple[BackupTarget, ...] = (
    BackupTarget(
        name="classification_store",
        env_var="DATABASE_URL",
        schema="public",
        tables=("classifications", "manual_decisions", "movement_log", "store_meta"),
    ),
    BackupTarget(
        name="pipeline",
        env_var="PIPELINE_DATABASE_URL",
        schema="pipeline",
        tables=("domains", "claude_calls", "registry_meta"),
    ),
    BackupTarget(
        name="ai_search",
        env_var="AI_SEARCH_DATABASE_URL",
        schema="ai_search",
        tables=(
            "domains", "pages", "crawl_attempts", "shortlists", "watch_queries",
            "query_discoveries", "worker_state", "html_blobs",
        ),
    ),
    BackupTarget(
        name="zone_universe",
        env_var="ZONE_UNIVERSE_DATABASE_URL",
        schema="zone_universe",
        tables=(
            "zones", "shards", "pages", "worker_state", "event_log",
            "crawl_retries", "domain_registration", "registration_queue",
            "html_blobs",
        ),
    ),
)

LOCAL_BACKUP_ROOT = Path(
    os.getenv("GLASSWING_DB_BACKUP_ROOT", str(DATA / "db_backups"))
).expanduser().resolve()
OBJECT_STORAGE_BACKUP_PREFIX = os.getenv(
    "GLASSWING_DB_BACKUP_PREFIX", "postgres-backups"
)


def _backup_key(filename: str) -> str:
    prefix = OBJECT_STORAGE_BACKUP_PREFIX if using_object_storage() else str(LOCAL_BACKUP_ROOT)
    return f"{prefix.rstrip('/')}/{filename}"


def _write_backup(target: BackupTarget) -> str:
    conn = connect_postgres(read_only=True, env_var=target.env_var)
    try:
        conn.execute(f"SET search_path TO {target.schema}, public")
        buffer = io.BytesIO()
        manifest: dict[str, object] = {
            "created_at": utc_now(), "target": target.name, "tables": {},
        }
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for table in target.tables:
                csv_bytes = conn.copy_out(
                    f"COPY {table} TO STDOUT WITH (FORMAT csv, HEADER)"
                )
                archive.writestr(f"{table}.csv", csv_bytes)
                manifest["tables"][table] = len(csv_bytes)
            archive.writestr(
                "manifest.json", json.dumps(manifest, indent=2, sort_keys=True)
            )
        payload = buffer.getvalue()
    finally:
        conn.close()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{target.name}_{timestamp}.zip"
    key = _backup_key(filename)
    write_bytes(key, payload)
    write_bytes(f"{key}.sha256", hashlib.sha256(payload).hexdigest().encode("utf-8"))
    return key


def create_database_backup() -> dict[str, str]:
    """Back up every subsystem currently on Postgres.

    Returns {target_name: backup_key} for whichever subsystems are actually
    cut over -- a subsystem still on local SQLite is skipped, not an error.
    """
    active = [t for t in TARGETS if using_postgres(t.env_var)]
    if not active:
        raise RuntimeError(
            "No subsystem is on Postgres right now (checked DATABASE_URL, "
            "PIPELINE_DATABASE_URL, AI_SEARCH_DATABASE_URL, "
            "ZONE_UNIVERSE_DATABASE_URL) -- nothing to back up."
        )
    return {target.name: _write_backup(target) for target in active}


if __name__ == "__main__":
    for name, key in create_database_backup().items():
        print(f"{name}: {key}")
