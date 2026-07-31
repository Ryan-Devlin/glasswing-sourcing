"""On-demand logical backup of the Postgres classification store.

Only meaningful in cloud mode (`DATABASE_URL` set): local SQLite mode is
already one self-contained file under `GLASSWING_CLASSIFICATION_DB`, and
human-authored decisions additionally get their own checksummed snapshots
via manual_truth_backup.py. This covers the Postgres-only case -- a managed
database with no local file a person could simply copy.

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
from datetime import datetime, timezone
from pathlib import Path

from classification_store import utc_now
from common import DATA
from db_backend import connect_postgres, using_postgres
from object_storage import using_object_storage, write_bytes

TABLES = ("classifications", "manual_decisions", "movement_log", "store_meta")

LOCAL_BACKUP_ROOT = Path(
    os.getenv("GLASSWING_DB_BACKUP_ROOT", str(DATA / "db_backups"))
).expanduser().resolve()
OBJECT_STORAGE_BACKUP_PREFIX = os.getenv(
    "GLASSWING_DB_BACKUP_PREFIX", "postgres-backups"
)


def _backup_key(filename: str) -> str:
    prefix = OBJECT_STORAGE_BACKUP_PREFIX if using_object_storage() else str(LOCAL_BACKUP_ROOT)
    return f"{prefix.rstrip('/')}/{filename}"


def create_database_backup() -> str:
    """Write a timestamped backup, return its object-storage/local key."""
    if not using_postgres():
        raise RuntimeError(
            "create_database_backup() requires DATABASE_URL (Postgres) mode; "
            "local SQLite mode has no separate backup step."
        )

    conn = connect_postgres(read_only=True)
    try:
        buffer = io.BytesIO()
        manifest: dict[str, object] = {"created_at": utc_now(), "tables": {}}
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for table in TABLES:
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
    filename = f"classification_store_{timestamp}.zip"
    key = _backup_key(filename)
    write_bytes(key, payload)
    write_bytes(f"{key}.sha256", hashlib.sha256(payload).hexdigest().encode("utf-8"))
    return key


if __name__ == "__main__":
    print(create_database_backup())
