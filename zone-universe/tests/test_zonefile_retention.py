from pathlib import Path

from zone_universe.database import connect, utc_now
from zone_universe.worker import retire_full_universe_queues, retire_zonefile


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
