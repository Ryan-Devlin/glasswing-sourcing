from __future__ import annotations

from pathlib import Path

from ai_search.extract import extract_page
from zone_universe.crawler import (
    CrawlOutcome,
    claim_batch,
    exclude_already_successful,
    save_success,
)
from zone_universe.database import connect, utc_now


def test_shard_checkpoint_resumes_at_exact_byte_offset(
    tmp_path: Path,
) -> None:
    shard_path = tmp_path / "000.txt"
    shard_path.write_bytes(b"alpha.app\nbeta.app\ngamma.app\n")
    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    conn.execute(
        """
        INSERT INTO zones(
            tld,import_status,active_queue_version,discovered_domains
        ) VALUES ('app','ready','v1',3)
        """
    )
    conn.execute(
        """
        INSERT INTO shards(
            tld,queue_version,shard,path,total_bytes,
            queued_domains,updated_at
        ) VALUES ('app','v1',0,?,?,3,?)
        """,
        (str(shard_path), shard_path.stat().st_size, utc_now()),
    )
    conn.commit()

    shard, first, offset = claim_batch(conn, "app", 2)
    assert shard is not None
    assert first == ["alpha.app", "beta.app"]
    conn.execute(
        """
        UPDATE shards SET byte_offset=?
        WHERE tld='app' AND queue_version='v1' AND shard=0
        """,
        (offset,),
    )
    conn.commit()

    _, second, final_offset = claim_batch(conn, "app", 2)
    assert second == ["gamma.app"]
    assert final_offset == shard_path.stat().st_size
    conn.close()


def test_completed_domains_are_skipped_after_a_zone_refresh(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    conn = connect(database)
    html = "<html><title>Known</title><body>Known company</body></html>"
    save_success(
        conn,
        CrawlOutcome(
            domain="known.app",
            tld="app",
            status="success",
            final_url="https://known.app",
            html=html,
            page=extract_page(html, "https://known.app"),
        ),
        retain_raw_html=False,
    )
    conn.commit()
    pending, skipped = exclude_already_successful(
        conn,
        ["known.app", "new.app"],
    )
    assert pending == ["new.app"]
    assert skipped == 1
    conn.close()
