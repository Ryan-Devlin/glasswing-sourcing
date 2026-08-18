import concurrent.futures
from types import SimpleNamespace
import errno
import os
import time
from pathlib import Path
from datetime import datetime, timezone

import pytest

from zone_universe.database import connect, utc_now
from zone_universe.worker import (
    download_scheduled_snapshots,
    open_stream_for_write,
    reclaim_orphaned_queue_directories,
    retire_full_universe_queues,
    retire_zonefile,
    scheduled_snapshot_day,
    snapshot_day_key,
)


def test_snapshot_schedule_opens_at_six_am_eastern() -> None:
    assert scheduled_snapshot_day(
        datetime(2026, 8, 17, 9, 59, tzinfo=timezone.utc)
    ) == ""
    assert scheduled_snapshot_day(
        datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
    ) == "2026-08-17"


def test_retire_zonefile_deletes_redownloadable_source_and_records_state(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    source = tmp_path / "com.txt.gz"
    source.write_bytes(b"recoverable zone source")
    conn = connect(database)

    released = retire_zonefile(
        conn,
        source,
        tld="com",
        queue_version="test-version",
    )

    assert released == len(b"recoverable zone source")
    assert not source.exists()
    state = conn.execute(
        "SELECT value FROM worker_state WHERE key='last_retired_zonefile'"
    ).fetchone()
    assert state is not None
    assert "test-version" in str(state["value"])


def test_retire_full_universe_queues_preserves_delta_shards(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    legacy = tmp_path / "legacy.txt"
    delta = tmp_path / "delta.txt"
    legacy.write_text("old.app\n", encoding="utf-8")
    delta.write_text("new.app\n", encoding="utf-8")
    now = utc_now()
    for shard, priority, path in ((0, 2, legacy), (1, 0, delta)):
        conn.execute(
            """
            INSERT INTO shards(
                tld,queue_version,shard,priority,path,total_bytes,
                queued_domains,updated_at
            ) VALUES ('app','version',?,?,?,?,1,?)
            """,
            (shard, priority, str(path), 4, now),
        )
    conn.commit()

    result = retire_full_universe_queues(conn)

    assert result["shards_removed"] == 1
    assert not legacy.exists()
    assert delta.exists()
    rows = conn.execute("SELECT priority FROM shards").fetchall()
    assert [row["priority"] for row in rows] == [0]
    conn.close()


def test_reclaim_orphaned_queue_directories_keeps_referenced_versions(
    tmp_path: Path,
) -> None:
    """Unreferenced queue directories are what fill the 5 GB volume.

    Both an import that died after renaming its build directory into place
    and one killed mid-build leave a directory nothing points at.
    """
    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    queues = tmp_path / "queues"
    live = queues / "app" / "20260818T140852Z-live"
    orphan = queues / "app" / "20260818T135206Z-orphan"
    killed_build = queues / "com" / ".building-20260818T140951Z-killed"
    for folder in (live, orphan, killed_build):
        folder.mkdir(parents=True)
        (folder / "domains.txt.gz").write_bytes(b"snapshot bytes")
    conn.execute(
        """
        INSERT INTO shards(
            tld,queue_version,shard,priority,path,total_bytes,
            queued_domains,updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            "app",
            "20260818T140852Z-live",
            0,
            0,
            str(live / "000.txt"),
            0,
            0,
            utc_now(),
        ),
    )
    conn.commit()

    result = reclaim_orphaned_queue_directories(conn, queue_root=queues)

    assert live.is_dir()
    assert (live / "domains.txt.gz").exists()
    assert not orphan.exists()
    assert not killed_build.exists()
    assert result["directories_removed"] == 2
    assert result["bytes_released"] == 2 * len(b"snapshot bytes")
    state = conn.execute(
        "SELECT value FROM worker_state WHERE key='orphaned_queue_cleanup'"
    ).fetchone()
    assert state is not None
    conn.close()


def test_reclaim_orphaned_queue_directories_honours_snapshot_references(
    tmp_path: Path,
) -> None:
    """A baseline zone has a snapshot but no queued shards to point at it."""
    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    queues = tmp_path / "queues"
    baseline = queues / "com" / "20260818T140951Z-baseline"
    baseline.mkdir(parents=True)
    snapshot = baseline / "domains.txt.gz"
    snapshot.write_bytes(b"snapshot bytes")
    conn.execute(
        """
        INSERT INTO zones(
            tld,zone_path,source_size,source_modified_at,
            source_fingerprint,import_status,snapshot_path
        ) VALUES (?,?,?,?,?,?,?)
        """,
        ("com", "", 0, "", "", "ready", str(snapshot)),
    )
    conn.commit()

    result = reclaim_orphaned_queue_directories(conn, queue_root=queues)

    assert snapshot.exists()
    assert result["directories_removed"] == 0
    conn.close()


def test_stream_open_reports_importer_failure_instead_of_deadlocking(
    tmp_path: Path,
) -> None:
    """A blocking write-open on a FIFO waits forever for a reader.

    The importer creates 64 shard files before it opens the read end, so an
    out-of-space failure in there left the CZDS download wedged on a reader
    that was never coming. The real error has to surface instead.
    """
    fifo = tmp_path / "com.stream.gz"
    os.mkfifo(fifo)

    def out_of_space() -> None:
        raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        importer = pool.submit(out_of_space)
        concurrent.futures.wait([importer], timeout=5)
        with pytest.raises(OSError) as failure:
            open_stream_for_write(fifo, importer, timeout=5.0)

    assert failure.value.errno == errno.ENOSPC


def test_stream_open_hands_back_a_writable_pipe_for_a_live_importer(
    tmp_path: Path,
) -> None:
    fifo = tmp_path / "app.stream.gz"
    os.mkfifo(fifo)
    received: list[bytes] = []

    def consume() -> None:
        with fifo.open("rb") as handle:
            received.append(handle.read())

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        importer = pool.submit(consume)
        with open_stream_for_write(fifo, importer, timeout=10.0) as output:
            output.write(b"zone bytes")
        importer.result(timeout=10)

    assert received == [b"zone bytes"]


def test_stream_open_times_out_when_the_importer_never_reads(
    tmp_path: Path,
) -> None:
    fifo = tmp_path / "com.stream.gz"
    os.mkfifo(fifo)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        importer = pool.submit(time.sleep, 3)
        with pytest.raises(TimeoutError):
            open_stream_for_write(fifo, importer, timeout=0.5)


def test_a_zone_that_already_imported_today_is_not_imported_again(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure in one zone must not re-import the zones that succeeded.

    Success is only recorded once every TLD finishes, so a `.com` failure
    used to send the retry back through `.app`. That second run diffs the
    day's file against the snapshot the first run just wrote, collapsing
    added_domains to zero -- the newly delegated names are not merely
    mis-reported, they are never queued, and the snapshot has already moved
    past them so no later run can recover them.
    """
    import asyncio

    from zone_universe import worker as worker_module

    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    settings = SimpleNamespace(tlds=("app", "com"), database=database)
    monkeypatch.setenv("CZDS_USERNAME", "user")
    monkeypatch.setenv("CZDS_PASSWORD", "secret")
    monkeypatch.setattr(worker_module, "ZONEFILES", tmp_path)
    client = tmp_path / "czds.py"
    client.write_text("", encoding="utf-8")
    monkeypatch.setattr(worker_module, "CZDS_CLIENT", client)
    streamed: list[str] = []

    def fake_stream(_settings: object, tld: str) -> dict[str, object]:
        streamed.append(tld)
        if tld == "com":
            raise RuntimeError("com import failed")
        return {"delegated_domains": 10, "added_domains": 7, "removed_domains": 1}

    monkeypatch.setattr(worker_module, "stream_czds_zone", fake_stream)
    monkeypatch.setattr(
        worker_module, "scheduled_snapshot_day", lambda *a, **k: "2026-08-18"
    )

    asyncio.run(download_scheduled_snapshots(conn, settings))
    assert streamed == ["app", "com"]
    assert _state(conn, snapshot_day_key("app")) == "2026-08-18"
    assert _state(conn, snapshot_day_key("com")) == ""

    # The retry after the .com failure must resume at .com, not redo .app.
    conn.execute("DELETE FROM worker_state WHERE key='last_czds_attempt_at'")
    conn.commit()
    asyncio.run(download_scheduled_snapshots(conn, settings))

    assert streamed == ["app", "com", "com"]
    conn.close()


def _state(conn, key: str) -> str:
    row = conn.execute(
        "SELECT value FROM worker_state WHERE key=?", (key,)
    ).fetchone()
    return str(row["value"]) if row is not None else ""
