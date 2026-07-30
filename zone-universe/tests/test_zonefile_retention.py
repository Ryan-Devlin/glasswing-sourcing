from pathlib import Path

from zone_universe.database import connect
from zone_universe.worker import retire_zonefile


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
