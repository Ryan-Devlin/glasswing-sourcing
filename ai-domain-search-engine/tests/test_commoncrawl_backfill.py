import json
import sys
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import commoncrawl_backfill as ccb
from ai_search.database import connect


def _fake_get(pages_by_url: dict[str, bytes]):
    def get(url: str, *, retries: int = 4) -> bytes:
        for prefix, body in pages_by_url.items():
            if url.startswith(prefix):
                return body
        raise AssertionError(f"unexpected URL: {url}")

    return get


def test_latest_crawl_id_reads_first_collection():
    collinfo = json.dumps([{"id": "CC-MAIN-2026-30"}, {"id": "CC-MAIN-2026-26"}]).encode()
    with patch.object(ccb, "_get", _fake_get({ccb.COLLINFO_URL: collinfo})):
        assert ccb.latest_crawl_id() == "CC-MAIN-2026-30"


def test_fetch_domains_paginates_and_normalizes(monkeypatch):
    crawl_id = "CC-MAIN-2026-30"
    api = ccb._cdx_api_for(crawl_id)
    page_info = json.dumps({"pages": 2, "pageSize": 2}).encode()
    page0 = "\n".join([
        json.dumps({"url": "https://one.ai/path"}),
        json.dumps({"url": "https://WWW.Two.ai/"}),
    ]).encode()
    page1 = "\n".join([
        json.dumps({"url": "https://one.ai/other-page"}),  # duplicate domain
        json.dumps({"url": "https://three.com/"}),  # wrong TLD, dropped
        "not-json",  # malformed line, skipped
    ]).encode()
    responses = {
        f"{api}?url=ai&matchType=domain&output=json&showNumPages=true": page_info,
        f"{api}?url=ai&matchType=domain&output=json&page=0&fl=url": page0,
        f"{api}?url=ai&matchType=domain&output=json&page=1&fl=url": page1,
    }
    with patch.object(ccb, "_get", _fake_get(responses)):
        progress_calls = []
        domains = ccb.fetch_domains(
            crawl_id, on_progress=lambda *a: progress_calls.append(a)
        )
    assert domains == {"one.ai", "two.ai"}
    assert progress_calls == [(1, 2, 2), (2, 2, 2)]


def test_fetch_domains_respects_max_pages():
    crawl_id = "CC-MAIN-2026-30"
    api = ccb._cdx_api_for(crawl_id)
    page_info = json.dumps({"pages": 5, "pageSize": 1}).encode()
    page0 = json.dumps({"url": "https://one.ai/"}).encode()
    responses = {
        f"{api}?url=ai&matchType=domain&output=json&showNumPages=true": page_info,
        f"{api}?url=ai&matchType=domain&output=json&page=0&fl=url": page0,
    }
    with patch.object(ccb, "_get", _fake_get(responses)):
        domains = ccb.fetch_domains(crawl_id, max_pages=1)
    assert domains == {"one.ai"}


def test_sync_only_adds_domains_not_already_known(tmp_path, monkeypatch):
    database = tmp_path / "search.sqlite3"
    conn = connect(database)
    conn.execute(
        "INSERT INTO domains(domain, first_seen_at, last_seen_at, discovery_source) "
        "VALUES ('known.ai', 't', 't', 'manual')"
    )
    conn.commit()
    conn.close()

    class FakeSettings:
        pass

    fake_settings = FakeSettings()
    fake_settings.database = database
    monkeypatch.setattr(ccb, "load_settings", lambda: fake_settings)
    monkeypatch.setattr(
        ccb, "fetch_domains", lambda crawl_id, **kwargs: {"known.ai", "brand-new.ai"}
    )

    result = ccb.sync("CC-MAIN-2026-30", max_pages=None)

    assert result == {
        "crawl_id": "CC-MAIN-2026-30",
        "fetched": 2,
        "already_known": 1,
        "newly_added": 1,
        "would_add": 0,
        "dry_run": False,
    }
    conn = connect(database, read_only=True)
    rows = {row["domain"] for row in conn.execute("SELECT domain FROM domains")}
    conn.close()
    assert rows == {"known.ai", "brand-new.ai"}


def test_sync_dry_run_reports_without_writing(tmp_path, monkeypatch):
    database = tmp_path / "search.sqlite3"

    class FakeSettings:
        pass

    fake_settings = FakeSettings()
    fake_settings.database = database
    monkeypatch.setattr(ccb, "load_settings", lambda: fake_settings)
    monkeypatch.setattr(
        ccb, "fetch_domains", lambda crawl_id, **kwargs: {"preview.ai"}
    )

    result = ccb.sync("CC-MAIN-2026-30", max_pages=None, dry_run=True)

    assert result["newly_added"] == 0
    assert result["would_add"] == 1
    conn = connect(database, read_only=True)
    rows = list(conn.execute("SELECT domain FROM domains"))
    conn.close()
    assert rows == []
