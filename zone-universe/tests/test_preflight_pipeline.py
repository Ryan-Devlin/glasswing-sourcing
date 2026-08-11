import asyncio
from dataclasses import replace
from pathlib import Path

import classification_store
from ai_search.extract import extract_page
from zone_universe import crawler
from zone_universe.config import load_settings
from zone_universe.crawler import CrawlOutcome
from zone_universe.database import connect, diagnostics, utc_now


def prepared_database(tmp_path: Path):
    database = tmp_path / "universe.sqlite3"
    shard_path = tmp_path / "000.txt"
    shard_path.write_text("candidateai.com\n", encoding="ascii")
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
        (str(shard_path), shard_path.stat().st_size, utc_now()),
    )
    conn.commit()
    settings = replace(
        load_settings(),
        database=database,
        batch_size=10,
        minimum_free_gb=2,
        retain_raw_html=False,
        auto_screen=False,
        auto_classify=False,
        auto_registration=False,
    )
    return conn, settings, shard_path


def test_rejected_preflight_advances_checkpoint_without_storing_html(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, shard_path = prepared_database(tmp_path)

    async def rejected(session, domain, selected_settings):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="preflight_rejected",
            preflight_reason="no_clear_technology_company_signal",
            response_bytes=4096,
        )

    monkeypatch.setattr(crawler, "preflight_domain", rejected)
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: set(),
    )
    counts = asyncio.run(crawler.crawl_once(conn, settings, tld="com"))

    assert counts["preflight_checked"] == 1
    assert counts["preflight_rejected"] == 1
    assert conn.execute("SELECT count(*) FROM pages").fetchone()[0] == 0
    shard = conn.execute("SELECT * FROM shards").fetchone()
    assert shard["attempted"] == 1
    assert shard["preflight_rejected"] == 1
    assert shard["preflight_response_bytes"] == 4096
    assert shard["byte_offset"] == shard_path.stat().st_size
    assert diagnostics(conn)["preflight_rejected"] == 1
    conn.close()


def test_tombstoned_domain_skips_even_the_homepage_preflight(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, shard_path = prepared_database(tmp_path)
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: {"candidateai.com"},
    )

    counts = asyncio.run(crawler.crawl_once(conn, settings, tld="com"))

    assert counts["storage_excluded"] == 1
    assert conn.execute("SELECT count(*) FROM pages").fetchone()[0] == 0
    assert conn.execute("SELECT byte_offset FROM shards").fetchone()[0] == (
        shard_path.stat().st_size
    )
    conn.close()


def test_new_domain_must_clear_quality_gate_before_network(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, shard_path = prepared_database(tmp_path)
    cold_domain = "localareashoppingnetworks.com"
    shard_path.write_text(cold_domain + "\n", encoding="ascii")
    conn.execute(
        "UPDATE shards SET total_bytes=?,queued_domains=1",
        (shard_path.stat().st_size,),
    )
    conn.commit()
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: set(),
    )

    async def should_not_fetch(*_args, **_kwargs):
        raise AssertionError("cold new domain reached the network")

    monkeypatch.setattr(crawler, "preflight_domain", should_not_fetch)
    counts = asyncio.run(crawler.crawl_once(conn, settings, tld="com"))

    assert counts["skipped_low_priority"] == 1
    assert conn.execute("SELECT attempted FROM shards").fetchone()[0] == 0
    assert conn.execute("SELECT byte_offset FROM shards").fetchone()[0] == (
        shard_path.stat().st_size
    )
    conn.close()


def test_external_redirect_alias_is_rejected_before_classification(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, settings, _ = prepared_database(tmp_path)
    page = extract_page(
        "<html><title>Strong AI</title><body>Enterprise artificial intelligence platform</body></html>",
        "https://established.ai/",
    )

    class Response:
        status = 200
        url = "https://established.ai/"

        def release(self):
            return None

    async def resolves(*_args, **_kwargs):
        return True

    async def redirected(*_args, **_kwargs):
        return Response(), "", page, 2048

    monkeypatch.setattr(crawler, "domain_resolves", resolves)
    monkeypatch.setattr(crawler, "_fetch_homepage_preflight", redirected)

    outcome = asyncio.run(
        crawler.preflight_domain(None, "alias.app", settings)
    )

    assert outcome.status == "preflight_rejected"
    assert outcome.preflight_reason == "redirected_to_unrelated_domain"


def test_successful_fetch_without_investable_classification_is_discarded(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, _ = prepared_database(tmp_path)
    html = """
    <html><title>Candidate AI</title><body>
    Enterprise artificial intelligence infrastructure software platform.
    </body></html>
    """

    async def promising(session, domain, selected_settings):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="preflight_promising",
            final_url=f"https://{domain}",
            preflight_reason="frontier_or_ai_product_signal",
            preflight_score=80,
            response_bytes=2048,
        )

    async def full_crawl(
        session,
        domain,
        selected_settings,
        **kwargs,
    ):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="success",
            final_url=f"https://{domain}",
            http_status=200,
            html=html,
            page=extract_page(html, f"https://{domain}"),
        )

    monkeypatch.setattr(crawler, "preflight_domain", promising)
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: set(),
    )
    monkeypatch.setattr(crawler, "crawl_domain", full_crawl)
    counts = asyncio.run(crawler.crawl_once(conn, settings, tld="com"))

    assert counts["preflight_promising"] == 1
    assert counts["success"] == 1
    assert conn.execute("SELECT count(*) FROM pages").fetchone()[0] == 0
    shard = conn.execute("SELECT * FROM shards").fetchone()
    assert shard["preflight_promising"] == 1
    assert shard["success"] == 1
    conn.close()


def test_investable_classification_is_retained(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, _ = prepared_database(tmp_path)
    html = """
    <html><title>Candidate AI</title><body>
    Enterprise artificial intelligence infrastructure software platform.
    </body></html>
    """

    async def promising(session, domain, selected_settings):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="preflight_promising",
            final_url=f"https://{domain}",
            preflight_score=80,
        )

    async def full_crawl(session, domain, selected_settings, **kwargs):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="success",
            final_url=f"https://{domain}",
            http_status=200,
            html=html,
            page=extract_page(html, f"https://{domain}"),
        )

    async def classified(domains, selected_settings):
        return {
            "classified": 1,
            "scores": {"candidateai.com": 91},
            "buckets": {"candidateai.com": "priority"},
        }

    monkeypatch.setattr(crawler, "preflight_domain", promising)
    monkeypatch.setattr(crawler, "crawl_domain", full_crawl)
    monkeypatch.setattr(crawler, "classify_cached_successes", classified)
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: set(),
    )

    counts = asyncio.run(crawler.crawl_once(conn, settings, tld="com"))

    assert counts["success"] == 1
    assert conn.execute("SELECT domain FROM pages").fetchone()[0] == (
        "candidateai.com"
    )
    conn.close()


def test_high_scoring_review_candidate_is_retained_temporarily(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, _ = prepared_database(tmp_path)
    html = """
    <html><title>Candidate AI</title><body>
    Enterprise artificial intelligence infrastructure software platform.
    </body></html>
    """

    async def promising(session, domain, selected_settings):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="preflight_promising",
            final_url=f"https://{domain}",
            preflight_score=80,
        )

    async def full_crawl(session, domain, selected_settings, **kwargs):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="success",
            final_url=f"https://{domain}",
            http_status=200,
            html=html,
            page=extract_page(html, f"https://{domain}"),
        )

    async def classified(domains, selected_settings):
        return {
            "classified": 1,
            "scores": {"candidateai.com": 82},
            "buckets": {"candidateai.com": "review"},
        }

    monkeypatch.setattr(crawler, "preflight_domain", promising)
    monkeypatch.setattr(crawler, "crawl_domain", full_crawl)
    monkeypatch.setattr(crawler, "classify_cached_successes", classified)
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: set(),
    )

    asyncio.run(crawler.crawl_once(conn, settings, tld="com"))

    assert conn.execute("SELECT domain FROM pages").fetchone()[0] == (
        "candidateai.com"
    )
    conn.close()


def test_pause_after_quick_check_does_not_lose_queue_position(
    tmp_path: Path,
    monkeypatch,
) -> None:
    conn, settings, _ = prepared_database(tmp_path)

    async def promising(session, domain, selected_settings):
        return CrawlOutcome(
            domain=domain,
            tld="com",
            status="preflight_promising",
            final_url=f"https://{domain}",
        )

    monkeypatch.setattr(crawler, "preflight_domain", promising)
    monkeypatch.setattr(
        classification_store,
        "excluded_domain_set",
        lambda _domains: set(),
    )
    counts = asyncio.run(crawler.crawl_once(
        conn,
        settings,
        tld="com",
        should_pause=lambda: True,
    ))

    assert counts["pause_requested"] == 1
    assert conn.execute("SELECT count(*) FROM pages").fetchone()[0] == 0
    shard = conn.execute("SELECT * FROM shards").fetchone()
    assert shard["byte_offset"] == 0
    assert shard["attempted"] == 0
    conn.close()
