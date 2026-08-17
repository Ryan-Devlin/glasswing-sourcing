from __future__ import annotations

import gzip
import json
from pathlib import Path

from zone_universe.database import connect
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
