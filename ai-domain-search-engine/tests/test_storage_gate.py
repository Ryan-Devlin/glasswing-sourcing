import asyncio
from dataclasses import replace

import classification_store

from ai_search.config import load_settings
from ai_search.crawler import crawl_pending
from ai_search.database import add_domain, connect


def test_excluded_domain_is_deleted_before_network_crawl(tmp_path, monkeypatch):
    database = tmp_path / "search.sqlite3"
    conn = connect(database)
    add_domain(conn, "discarded.ai", "fixture")
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda domains: {"discarded.ai"},
    )
    settings = replace(load_settings(), database=database)

    counts = asyncio.run(crawl_pending(conn, settings, 10))

    assert counts == {"storage_excluded": 1}
    assert conn.execute("SELECT count(*) FROM domains").fetchone()[0] == 0
    conn.close()
