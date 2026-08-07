"""One-time repair for shard files that only ever existed on a local
machine.

The .com/.app domain queues were originally built by running the importer
locally (on a developer's Mac), which wrote absolute local file paths (e.g.
``/Users/name/.../queues/com/<version>/003.txt``) into the shared Postgres
``shards`` table. Those paths can never resolve inside a Railway container,
so ``claim_batch`` raised ``FileNotFoundError`` on every single attempt --
the cloud crawler had done zero real crawling despite the queue metadata
claiming otherwise. The same shard files were uploaded once to object
storage under ``zone-universe-queue-migration/<tld>/<queue_version>/
<shard>.txt`` as a one-time relay; this module downloads whichever of them
are still missing locally and repoints ``shards.path`` at the copy that now
actually exists inside this container, using its own configured data root.
Idempotent and safe to run on every worker startup: once a shard's local
file exists, it's left alone.
"""

from __future__ import annotations

from pathlib import Path

from .config import Settings
from .database import log_event


def migrate_queue_files_from_object_storage(
    conn,
    settings: Settings,
) -> dict[str, int]:
    import object_storage as store

    if not store.using_object_storage():
        return {"checked": 0, "downloaded": 0, "missing_in_storage": 0}

    rows = conn.execute(
        """
        SELECT s.tld, s.queue_version, s.shard, s.path
        FROM shards s
        JOIN zones z ON z.tld = s.tld AND z.active_queue_version = s.queue_version
        """
    ).fetchall()

    checked = 0
    downloaded = 0
    missing_in_storage = 0
    for row in rows:
        checked += 1
        local_path = Path(str(row["path"]))
        if local_path.exists():
            continue
        tld = str(row["tld"])
        queue_version = str(row["queue_version"])
        shard = int(row["shard"])
        key = (
            f"zone-universe-queue-migration/{tld}/{queue_version}/"
            f"{shard:03d}.txt"
        )
        if not store.exists(key):
            missing_in_storage += 1
            continue
        new_path = (
            settings.database.parent
            / "queues"
            / tld
            / queue_version
            / f"{shard:03d}.txt"
        )
        new_path.parent.mkdir(parents=True, exist_ok=True)
        new_path.write_bytes(store.read_bytes(key))
        conn.execute(
            "UPDATE shards SET path=? WHERE tld=? AND queue_version=? AND shard=?",
            (str(new_path), tld, queue_version, shard),
        )
        conn.commit()
        downloaded += 1

    result = {
        "checked": checked,
        "downloaded": downloaded,
        "missing_in_storage": missing_in_storage,
    }
    if downloaded or missing_in_storage:
        log_event(
            conn,
            "queue_migration_from_object_storage",
            f"checked={checked} downloaded={downloaded} "
            f"missing_in_storage={missing_in_storage}",
        )
    return result
