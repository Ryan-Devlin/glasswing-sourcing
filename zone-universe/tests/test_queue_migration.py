from __future__ import annotations

from pathlib import Path

import object_storage
from zone_universe.config import Settings
from zone_universe.database import connect, utc_now
from zone_universe.queue_migration import migrate_queue_files_from_object_storage


def make_settings(database: Path) -> Settings:
    return Settings(
        database=database,
        tlds=("com",),
        concurrency=1,
        batch_size=1,
        preflight_enabled=False,
        preflight_concurrency=1,
        preflight_max_response_bytes=1,
        timeout_seconds=1.0,
        max_response_bytes=1,
        minimum_free_gb=0.0,
        retain_raw_html=False,
        html_archive_minimum_score=75,
        html_archive=database.parent / "html",
        max_pages_per_domain=1,
        max_crawl_priority=2,
        dns_preflight=False,
        max_retries=0,
        auto_screen=False,
        auto_classify=False,
        classifier_workers=1,
        auto_registration=False,
        registration_daily_limit=1,
        registration_interval_seconds=1.0,
        registration_timeout_seconds=1.0,
        auto_import_zones=False,
        delete_imported_zonefiles=False,
        screening_queue=database.parent / "pipeline_queue.sqlite3",
        poll_seconds=1.0,
        user_agent="test",
    )


def test_missing_local_shard_is_restored_from_object_storage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("S3_BUCKET", raising=False)
    storage_root = tmp_path / "storage"
    monkeypatch.setattr(object_storage, "using_object_storage", lambda: True)
    monkeypatch.setattr(
        object_storage, "exists", lambda key: (storage_root / key).exists()
    )
    monkeypatch.setattr(
        object_storage,
        "read_bytes",
        lambda key: (storage_root / key).read_bytes(),
    )

    key = "zone-universe-queue-migration/com/v1/000.txt"
    staged = storage_root / key
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(b"alpha.com\nbeta.com\n")

    database = tmp_path / "container" / "universe.sqlite3"
    settings = make_settings(database)
    conn = connect(database)
    # This is exactly the bug: a path that only ever existed on a
    # developer's own machine, never inside this container.
    stale_local_path = "/Users/someone/old-mac/queues/com/v1/000.txt"
    conn.execute(
        """
        INSERT INTO zones(
            tld,import_status,active_queue_version,discovered_domains
        ) VALUES ('com','ready','v1',2)
        """
    )
    conn.execute(
        """
        INSERT INTO shards(
            tld,queue_version,shard,path,total_bytes,
            queued_domains,updated_at
        ) VALUES ('com','v1',0,?,?,2,?)
        """,
        (stale_local_path, len(b"alpha.com\nbeta.com\n"), utc_now()),
    )
    conn.commit()

    result = migrate_queue_files_from_object_storage(conn, settings)
    assert result == {"checked": 1, "downloaded": 1, "missing_in_storage": 0}

    row = conn.execute(
        "SELECT path FROM shards WHERE tld='com' AND queue_version='v1' AND shard=0"
    ).fetchone()
    new_path = Path(row["path"])
    assert new_path != Path(stale_local_path)
    assert new_path.exists()
    assert new_path.read_bytes() == b"alpha.com\nbeta.com\n"
    conn.close()


def test_shard_already_present_locally_is_left_untouched(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(object_storage, "using_object_storage", lambda: True)

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("should never check storage for an existing file")

    monkeypatch.setattr(object_storage, "exists", fail_if_called)

    local_path = tmp_path / "already-here.txt"
    local_path.write_bytes(b"gamma.com\n")

    database = tmp_path / "container" / "universe.sqlite3"
    settings = make_settings(database)
    conn = connect(database)
    conn.execute(
        """
        INSERT INTO zones(
            tld,import_status,active_queue_version,discovered_domains
        ) VALUES ('com','ready','v1',1)
        """
    )
    conn.execute(
        """
        INSERT INTO shards(
            tld,queue_version,shard,path,total_bytes,
            queued_domains,updated_at
        ) VALUES ('com','v1',0,?,?,1,?)
        """,
        (str(local_path), local_path.stat().st_size, utc_now()),
    )
    conn.commit()

    result = migrate_queue_files_from_object_storage(conn, settings)
    assert result == {"checked": 1, "downloaded": 0, "missing_in_storage": 0}
    conn.close()


def test_no_object_storage_configured_is_a_cheap_noop(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(object_storage, "using_object_storage", lambda: False)
    database = tmp_path / "universe.sqlite3"
    settings = make_settings(database)
    conn = connect(database)
    result = migrate_queue_files_from_object_storage(conn, settings)
    assert result == {
        "checked": 0,
        "downloaded": 0,
        "missing_in_storage": 0,
    }
    conn.close()
