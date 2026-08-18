from __future__ import annotations

import gzip
import json
import os
import concurrent.futures
from pathlib import Path

import pytest

from zone_universe.database import connect
from zone_universe import zone_parser
from zone_universe.zone_parser import (
    extract_delegated_domain,
    import_zone,
)


def test_extract_delegated_domain_only_accepts_requested_tld() -> None:
    assert (
        extract_delegated_domain(
            b"alpha.app. 3600 IN NS ns1.example.net.\n",
            "app",
        )
        == "alpha.app"
    )
    assert (
        extract_delegated_domain(
            b"alpha.app. 3600 IN A 192.0.2.1\n",
            "app",
        )
        == ""
    )
    assert (
        extract_delegated_domain(
            b"alpha.com. 3600 IN NS ns1.example.net.\n",
            "app",
        )
        == ""
    )
    assert (
        extract_delegated_domain(
            b" app. 3600 IN NS ns1.example.net.\n",
            "app",
        )
        == ""
    )


def test_import_zone_streams_unique_delegations_into_shards(
    tmp_path: Path,
) -> None:
    zone_path = tmp_path / "app.zone.gz"
    zone_data = b"""$ORIGIN app.
app. 3600 IN SOA ns1.example. hostmaster.example. 1 2 3 4 5
alpha.app. 3600 IN NS ns1.example.net.
alpha.app. 3600 IN NS ns2.example.net.
alpha.app. 3600 IN A 192.0.2.1
beta.app. 3600 IN NS ns1.example.net.
12987choeiusryhfq38boeuirsy.app. 3600 IN NS ns1.example.net.
wrong.com. 3600 IN NS ns1.example.net.
"""
    with gzip.open(zone_path, "wb") as handle:
        handle.write(zone_data)

    database = tmp_path / "universe.sqlite3"
    queues = tmp_path / "queues"
    result = import_zone(
        zone_path,
        tld="app",
        shard_count=4,
        database=database,
        queue_root=queues,
    )

    assert result["delegated_domains"] == 2
    assert result["added_domains"] == 0
    assert result["repeated_records"] == 1
    assert result["rejected_gibberish"] == 1
    queue_dir = Path(str(result["queue_directory"]))
    domains = {
        domain
        for shard in queue_dir.glob("*.txt")
        for domain in shard.read_text().splitlines()
        if domain
    }
    assert domains == set()
    audit_path = Path(str(result["rejection_audit"]))
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["rejected_domains"] == 1
    assert any(
        "12987choeiusryhfq38boeuirsy.app" in domains
        for domains in audit["examples"].values()
    )
    assert audit["by_reason"]["generated_alphanumeric_label"] == 1

    conn = connect(database)
    zone = conn.execute(
        "SELECT * FROM zones WHERE tld='app'"
    ).fetchone()
    assert zone is not None
    assert zone["import_status"] == "ready"
    assert zone["discovered_domains"] == 2
    assert zone["rejected_gibberish"] == 1
    assert (
        conn.execute(
            "SELECT sum(queued_domains) FROM shards WHERE tld='app'"
        ).fetchone()[0]
        == 0
    )
    conn.close()


def test_zone_refresh_records_additions_and_removals(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    queues = tmp_path / "queues"
    first = tmp_path / "first.app.zone.gz"
    second = tmp_path / "second.app.zone.gz"
    with gzip.open(first, "wb") as handle:
        handle.write(
            b"alpha.app. 3600 IN NS ns1.example.net.\n"
            b"beta.app. 3600 IN NS ns1.example.net.\n"
        )
    initial = import_zone(
        first,
        tld="app",
        shard_count=4,
        database=database,
        queue_root=queues,
    )
    assert initial["added_domains"] == 0

    with gzip.open(second, "wb") as handle:
        handle.write(
            b"beta.app. 3600 IN NS ns1.example.net.\n"
            b"gamma.app. 3600 IN NS ns1.example.net.\n"
        )
    refreshed = import_zone(
        second,
        tld="app",
        shard_count=4,
        database=database,
        queue_root=queues,
    )
    assert refreshed["added_domains"] == 1
    assert refreshed["removed_domains"] == 1
    assert refreshed["unchanged_domains"] == 1
    with gzip.open(refreshed["delta_added_path"], "rt") as handle:
        assert handle.read().splitlines() == ["gamma.app"]
    with gzip.open(refreshed["delta_removed_path"], "rt") as handle:
        assert handle.read().splitlines() == ["alpha.app"]
    queue_dir = Path(str(refreshed["queue_directory"]))
    queued = {
        domain
        for shard in queue_dir.glob("*.txt")
        for domain in shard.read_text().splitlines()
        if domain
    }
    assert queued == {"gamma.app"}


def test_import_zone_accepts_a_one_shot_gzip_fifo(tmp_path: Path) -> None:
    fifo = tmp_path / "app.stream.gz"
    os.mkfifo(fifo)
    database = tmp_path / "universe.sqlite3"
    queues = tmp_path / "queues"
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            import_zone,
            fifo,
            tld="app",
            shard_count=4,
            database=database,
            queue_root=queues,
        )
        with gzip.open(fifo, "wb") as output:
            output.write(
                b"alpha.app. 3600 IN NS ns1.example.net.\n"
                b"beta.app. 3600 IN NS ns1.example.net.\n"
            )
        result = future.result(timeout=5)
    assert result["delegated_domains"] == 2
    assert result["added_domains"] == 0


def test_failed_database_write_does_not_strand_the_queue_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The build directory is renamed into place before the database work.

    A failure after that rename used to leave the finished directory --
    snapshot plus every shard -- on disk with no shards row pointing at it,
    so nothing ever reclaimed it and the volume filled up.
    """
    zone_path = tmp_path / "app.zone.gz"
    with gzip.open(zone_path, "wb") as handle:
        handle.write(b"alpha.app. 3600 IN NS ns1.example.net.\n")
    database = tmp_path / "universe.sqlite3"
    queues = tmp_path / "queues"

    def explode() -> str:
        raise RuntimeError("simulated database failure after rename")

    monkeypatch.setattr(zone_parser, "utc_now", explode)
    with pytest.raises(RuntimeError):
        import_zone(
            zone_path,
            tld="app",
            shard_count=4,
            database=database,
            queue_root=queues,
        )

    assert list((queues / "app").iterdir()) == []
    conn = connect(database)
    zone = conn.execute("SELECT * FROM zones WHERE tld='app'").fetchone()
    assert zone is not None
    assert zone["import_status"] == "error"
    assert "simulated database failure" in str(zone["import_error"])
    conn.close()


def test_committed_queue_directory_survives_a_later_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once shard rows are durable the directory must be kept.

    log_event and the obsolete-version sweep both run after the commit, so a
    failure there must not delete a queue the database still points at.
    """
    zone_path = tmp_path / "app.zone.gz"
    with gzip.open(zone_path, "wb") as handle:
        handle.write(b"alpha.app. 3600 IN NS ns1.example.net.\n")
    database = tmp_path / "universe.sqlite3"
    queues = tmp_path / "queues"

    def explode(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated failure after commit")

    monkeypatch.setattr(zone_parser, "log_event", explode)
    with pytest.raises(RuntimeError):
        import_zone(
            zone_path,
            tld="app",
            shard_count=4,
            database=database,
            queue_root=queues,
        )

    conn = connect(database)
    shard = conn.execute(
        "SELECT path FROM shards WHERE tld='app' LIMIT 1"
    ).fetchone()
    assert shard is not None
    retained = Path(str(shard["path"])).parent
    assert retained.is_dir()
    assert (retained / "domains.txt.gz").exists()
    conn.close()
