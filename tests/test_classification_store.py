import json
import os

import pandas as pd
import pytest

import classification_store
from classification_store import (
    classification_bucket_counts,
    classification_map,
    classification_source_label,
    connect_store,
    corpus_bucket_counts,
    hidden_domain_set,
    hide_domains,
    list_hidden_domains,
    query_classifications,
    query_classifications_by_domains,
    query_corpus_classifications,
    record_automated_exclusions,
    excluded_domain_set,
    remove_training_labels,
    upsert_training_labels,
    upsert_claude_diligence,
    query_claude_diligence,
    record_manual_move,
    import_private_sqlite_into_shared,
    unhide_domains,
    upsert_classifications,
)


def test_master_bucket_queries_are_paged_and_source_labeled(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    frame = pd.DataFrame([
        {
            "domain": "cert.ai",
            "run_id": "run-one",
            "page_title": "Cert Company",
            "content_source": "network_fetch",
            "routing_bucket": "priority",
            "diligence_rank_score": 88,
        },
        {
            "domain": "crawler.ai",
            "run_id": "run-one",
            "content_source": "crawler_cache",
            "routing_bucket": "review",
        },
        {
            "domain": "zone.app",
            "run_id": "run-two",
            "content_source": "zone_universe_cache",
            "routing_bucket": "reject",
        },
    ])
    assert upsert_classifications(frame, database) == 3

    assert classification_bucket_counts(path=database) == {
        "priority": 1,
        "reject": 1,
        "review": 1,
    }
    assert classification_bucket_counts(
        run_id="run-one",
        path=database,
    ) == {"priority": 1, "review": 1}
    rows = query_classifications(
        ["priority", "review"],
        limit=1,
        path=database,
    )
    assert len(rows) == 1
    assert rows[0]["domain"] == "cert.ai"
    assert classification_source_label(rows[0]["content_source"]) == "CertStream"
    assert classification_source_label("crawler_cache_stale") == ".AI crawler"
    assert classification_source_label("zone_universe_cache") == ".COM/.APP crawler"
    domain_rows = query_classifications_by_domains(
        ["https://zone.app", "missing.ai"],
        path=database,
    )
    assert [row["domain"] for row in domain_rows] == ["zone.app"]


def test_claude_diligence_is_shared_and_upserted(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    first = {
        "domain": "company.ai",
        "llm_decision": "MAYBE",
        "llm_fit_score": 60,
        "llm_product_summary": "Initial review",
        "llm_diligence_version": "v1",
        "llm_model": "claude-one",
    }
    second = {
        **first,
        "llm_decision": "KEEP",
        "llm_fit_score": 88,
        "llm_product_summary": "Approved company",
    }
    assert upsert_claude_diligence([first], path=database) == 1
    assert upsert_claude_diligence([second], path=database) == 1
    rows = query_claude_diligence(
        ["company.ai"], diligence_version="v1", path=database
    )
    assert len(rows) == 1
    assert rows[0]["llm_decision"] == "KEEP"
    assert rows[0]["llm_fit_score"] == 88


def test_automated_exclusion_is_tiny_and_human_truth_overrides_it(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "classifications.sqlite3"
    assert record_automated_exclusions(
        [
            {
                "domain": "junk.ai",
                "routing_bucket": "ghost",
                "routing_reason": "no_content",
                "glasswing_rating": 0,
                "homepage_excerpt": "must never be stored in tombstone",
            },
            {
                "domain": "human.ai",
                "routing_bucket": "reject",
                "glasswing_rating": 3,
            },
        ],
        path=database,
    ) == 2
    monkeypatch.setattr(
        "training_truth.protected_training_domains",
        lambda: {"human.ai"},
    )
    assert excluded_domain_set(
        ["junk.ai", "human.ai", "unknown.ai"],
        path=database,
    ) == {"junk.ai"}
    with connect_store(database, read_only=True) as conn:
        row = conn.execute(
            "SELECT * FROM automated_exclusions WHERE domain='junk.ai'"
        ).fetchone()
    assert set(row.keys()) == {
        "domain", "bucket", "reason", "glasswing_rating",
        "content_source", "excluded_at",
    }


def test_shared_training_labels_are_exclusive_and_removable(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    assert upsert_training_labels(
        [{
            "domain": "labeled.ai",
            "label": "yes",
            "rating": 91,
            "source": "human",
            "reason": "manual review",
        }],
        path=database,
    ) == 1
    assert upsert_training_labels(
        [{
            "domain": "labeled.ai",
            "label": "no",
            "rating": 2,
            "source": "human",
        }],
        path=database,
    ) == 1
    with connect_store(database, read_only=True) as conn:
        row = conn.execute(
            "SELECT label,rating FROM training_labels WHERE domain=?",
            ("labeled.ai",),
        ).fetchone()
    assert (row["label"], row["rating"]) == ("no", 2)
    assert remove_training_labels(["labeled.ai"], path=database) == 1


def test_pipeline_database_url_is_cloud_fallback(monkeypatch):
    monkeypatch.delenv("CLASSIFICATION_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PIPELINE_DATABASE_URL", "postgresql://shared/example")

    assert (
        classification_store.classification_database_env_var()
        == "PIPELINE_DATABASE_URL"
    )
    assert classification_store.using_classification_postgres() is True


def test_dedicated_classification_database_takes_precedence(monkeypatch):
    monkeypatch.setenv(
        "CLASSIFICATION_DATABASE_URL",
        "postgresql://classification/example",
    )
    monkeypatch.setenv("DATABASE_URL", "postgresql://web/example")
    monkeypatch.setenv("PIPELINE_DATABASE_URL", "postgresql://pipeline/example")

    assert (
        classification_store.classification_database_env_var()
        == "CLASSIFICATION_DATABASE_URL"
    )


def _live_postgres_url() -> str | None:
    """Only used by the opt-in Postgres parity test below.

    Skips (rather than failing) whenever DATABASE_URL isn't set or isn't
    reachable, so the normal test run never depends on a live database --
    this is purely for manually verifying the Postgres backend once
    credentials are in place.
    """
    url = os.getenv("TEST_DATABASE_URL", "").strip()
    if not url:
        return None
    try:
        import psycopg

        with psycopg.connect(url, connect_timeout=5):
            pass
    except Exception:
        return None
    return url


def test_store_separates_fit_confidence_and_priority(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    frame = pd.DataFrame([
        {
            "domain": "strong.ai",
            "page_title": "Strong Systems",
            "meta_description": "Enterprise robotics infrastructure",
            "homepage_excerpt": "Technical platform " * 80,
            "word_count": 160,
            "pages_scraped": 2,
            "http_status": 200,
            "crawl_status": "success",
            "content_source": "crawler_cache",
            "routing_bucket": "priority",
            "routing_reason": "strong_investable_candidate",
            "diligence_rank_score": 84,
            "rule_evidence": "robotics; enterprise platform",
        },
        {
            "domain": "missing.ai",
            "fetch_error": "crawler_dns_error",
            "crawl_status": "dns_error",
            "routing_bucket": "ghost",
            "routing_reason": "ghost_or_inaccessible_site",
            "diligence_rank_score": 0,
        },
    ])
    assert upsert_classifications(frame, database) == 2

    with connect_store(database, read_only=True) as conn:
        strong = conn.execute(
            "SELECT * FROM classifications WHERE domain='strong.ai'"
        ).fetchone()
        missing = conn.execute(
            "SELECT * FROM classifications WHERE domain='missing.ai'"
        ).fetchone()

    assert strong["availability_status"] == "live"
    assert strong["fit_score"] == 84
    assert strong["confidence_score"] > 80
    assert strong["priority_score"] >= 55
    assert missing["availability_status"] == "dns_error"
    assert missing["confidence_score"] == 0


def test_hide_domains_is_a_view_preference_not_a_deletion(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    frame = pd.DataFrame([
        {"domain": "keep.ai", "routing_bucket": "priority"},
        {"domain": "toggle.ai", "routing_bucket": "priority"},
    ])
    assert upsert_classifications(frame, database) == 2

    assert hidden_domain_set(["keep.ai", "toggle.ai"], path=database) == set()

    hide_domains(["toggle.ai"], path=database)
    assert hidden_domain_set(["keep.ai", "toggle.ai"], path=database) == {
        "toggle.ai"
    }

    # Still present in the real dataset -- hiding is bookkeeping, not deletion.
    with connect_store(database, read_only=True) as conn:
        row = conn.execute(
            "SELECT * FROM classifications WHERE domain='toggle.ai'"
        ).fetchone()
    assert row is not None

    unhide_domains(["toggle.ai"], path=database)
    assert hidden_domain_set(["keep.ai", "toggle.ai"], path=database) == set()

    # Hiding the same domain twice (re-checking the box after an unhide, or
    # a duplicate call) must not error.
    hide_domains(["toggle.ai"], path=database)
    hide_domains(["toggle.ai"], path=database)
    assert hidden_domain_set(["toggle.ai"], path=database) == {"toggle.ai"}
    listed = list_hidden_domains(path=database)
    assert [row["domain"] for row in listed] == ["toggle.ai"]


def test_hidden_domain_queries_handle_more_than_sqlite_variable_limit(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    domains = [f"company-{number}.ai" for number in range(1200)]

    assert hide_domains(domains, path=database) == 1200
    assert hidden_domain_set(domains, path=database) == set(domains)
    assert unhide_domains(domains, path=database) == 1200
    assert hidden_domain_set(domains, path=database) == set()


def test_manual_move_is_transactional_and_audited(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    upsert_classifications(
        pd.DataFrame([
            {
                "domain": "move.ai",
                "routing_bucket": "review",
                "rule_score": 67,
                "word_count": 120,
                "homepage_excerpt": "Enterprise software platform",
            }
        ]),
        database,
    )

    moved = record_manual_move(
        ["move.ai"],
        target_label="Investable Candidate",
        target_bucket="priority",
        scores={"move.ai": 67},
        note="human review",
        path=database,
    )
    assert moved == 1
    assert classification_map(path=database) == {"move.ai": "priority"}

    with connect_store(database, read_only=True) as conn:
        decision = conn.execute(
            "SELECT * FROM manual_decisions WHERE domain='move.ai'"
        ).fetchone()
        history = conn.execute(
            "SELECT * FROM movement_log WHERE domain='move.ai'"
        ).fetchall()
        stored = conn.execute(
            "SELECT record_json FROM classifications WHERE domain='move.ai'"
        ).fetchone()

    assert decision["target_bucket"] == "priority"
    assert len(history) == 1
    assert history[0]["from_bucket"] == "review"
    assert json.loads(stored["record_json"])["manual_override"] is True

    # A later automated crawl may refresh website fields, but it must never
    # erase a human decision while upserting that new evidence.
    upsert_classifications(
        pd.DataFrame([{
            "domain": "move.ai",
            "routing_bucket": "reject",
            "routing_reason": "automated_rejection",
            "rule_score": 2,
            "meta_description": "Refreshed website copy",
        }]),
        database,
    )
    with connect_store(database, read_only=True) as conn:
        preserved = conn.execute(
            "SELECT routing_bucket,manual_override,record_json "
            "FROM classifications WHERE domain='move.ai'"
        ).fetchone()
    assert preserved["routing_bucket"] == "priority"
    assert preserved["manual_override"] == 1
    assert json.loads(preserved["record_json"])["manual_override"] is True


def test_private_sqlite_import_only_merges_newer_records(tmp_path, monkeypatch):
    source = tmp_path / "private.sqlite3"
    import sqlite3

    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE classifications(record_json TEXT)")
        conn.executemany(
            "INSERT INTO classifications(record_json) VALUES (?)",
            [
                (json.dumps({
                    "domain": "old.ai",
                    "observed_at": "2026-07-01T00:00:00Z",
                }),),
                (json.dumps({
                    "domain": "new.ai",
                    "observed_at": "2026-08-04T00:00:00Z",
                }),),
            ],
        )

    class SharedConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql):
            class Result:
                @staticmethod
                def fetchone():
                    return {"latest": "2026-07-30T00:00:00Z"}

            return Result()

    imported = []
    monkeypatch.setattr(
        classification_store,
        "using_classification_postgres",
        lambda: True,
    )
    monkeypatch.setattr(
        classification_store,
        "connect_store",
        lambda *args, **kwargs: SharedConnection(),
    )
    monkeypatch.setattr(
        classification_store,
        "upsert_classifications",
        lambda frame: imported.extend(frame["domain"].tolist()) or len(frame),
    )

    assert import_private_sqlite_into_shared(source) == 1
    assert imported == ["new.ai"]


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_postgres_backend_matches_sqlite_behavior(monkeypatch):
    """Opt-in parity check: same scenario as the two tests above, on Postgres.

    Skipped by default (see _live_postgres_url) -- run explicitly with
    TEST_DATABASE_URL set to verify the cloud backend once credentials are
    available. Cleans up its own rows before and after so it never leaves
    residue in a database that may later hold real classifications.
    """
    url = _live_postgres_url()
    monkeypatch.setenv("DATABASE_URL", url)
    test_domains = ["pg-strong.ai", "pg-move.ai"]

    with connect_store() as conn:
        marks = ",".join("?" for _ in test_domains)
        conn.execute(f"DELETE FROM classifications WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM manual_decisions WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM movement_log WHERE domain IN ({marks})", test_domains)
        conn.execute(f"DELETE FROM hidden_domains WHERE domain IN ({marks})", test_domains)
        conn.commit()

    try:
        frame = pd.DataFrame([{
            "domain": "pg-strong.ai",
            "page_title": "Strong Systems",
            "meta_description": "Enterprise robotics infrastructure",
            "homepage_excerpt": "Technical platform " * 80,
            "word_count": 160,
            "pages_scraped": 2,
            "http_status": 200,
            "crawl_status": "success",
            "content_source": "crawler_cache",
            "routing_bucket": "priority",
            "routing_reason": "strong_investable_candidate",
            "diligence_rank_score": 84,
            "rule_evidence": "robotics; enterprise platform",
        }, {
            "domain": "pg-move.ai",
            "routing_bucket": "review",
            "rule_score": 67,
            "word_count": 120,
            "homepage_excerpt": "Enterprise software platform",
        }])
        assert upsert_classifications(frame) == 2

        with connect_store(read_only=True) as conn:
            strong = conn.execute(
                "SELECT * FROM classifications WHERE domain=?", ("pg-strong.ai",)
            ).fetchone()
        assert strong["availability_status"] == "live"
        assert strong["fit_score"] == 84
        assert strong["confidence_score"] > 80
        assert strong["priority_score"] >= 55

        moved = record_manual_move(
            ["pg-move.ai"],
            target_label="Investable Candidate",
            target_bucket="priority",
            scores={"pg-move.ai": 67},
            note="human review",
        )
        assert moved == 1
        assert classification_map()["pg-move.ai"] == "priority"

        with connect_store(read_only=True) as conn:
            decision = conn.execute(
                "SELECT * FROM manual_decisions WHERE domain=?", ("pg-move.ai",)
            ).fetchone()
            history = conn.execute(
                "SELECT * FROM movement_log WHERE domain=?", ("pg-move.ai",)
            ).fetchall()
        assert decision["target_bucket"] == "priority"
        assert len(history) == 1
        assert history[0]["from_bucket"] == "review"

        assert hide_domains(["pg-strong.ai"]) == 1
        assert hidden_domain_set(test_domains) == {"pg-strong.ai"}
        assert list_hidden_domains()[0]["domain"] == "pg-strong.ai"
        assert unhide_domains(["pg-strong.ai"]) == 1
        assert hidden_domain_set(test_domains) == set()
    finally:
        with connect_store() as conn:
            marks = ",".join("?" for _ in test_domains)
            conn.execute(
                f"DELETE FROM classifications WHERE domain IN ({marks})", test_domains
            )
            conn.execute(
                f"DELETE FROM manual_decisions WHERE domain IN ({marks})", test_domains
            )
            conn.execute(
                f"DELETE FROM movement_log WHERE domain IN ({marks})", test_domains
            )
            conn.execute(
                f"DELETE FROM hidden_domains WHERE domain IN ({marks})", test_domains
            )
            conn.commit()


def _make_corpus_db(tmp_path, crawled_domains):
    import sqlite3

    path = tmp_path / "corpus.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE domains(domain TEXT PRIMARY KEY, last_crawled_at TEXT)"
        )
        conn.executemany(
            "INSERT INTO domains(domain, last_crawled_at) VALUES (?, ?)",
            [(domain, "2026-08-01T00:00:00Z") for domain in crawled_domains],
        )
    return path


class _RecordingPostgresConnection:
    """Fakes just enough of the Postgres connection surface to assert how
    many round trips the corpus-scoped queries make and with what SQL."""

    def __init__(self, rows):
        self._rows = rows
        self.queries: list[tuple[str, list]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=()):
        self.queries.append((sql, list(params)))

        class Result:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        return Result(self._rows)


def test_corpus_bucket_counts_uses_one_query_regardless_of_corpus_size(
    tmp_path, monkeypatch
):
    # A large crawled corpus used to force one Postgres round trip per 800
    # domains (chunked IN (...) batches) -- confirm it now costs exactly one
    # query no matter how many crawled domains there are.
    crawled = [f"crawled-{i}.ai" for i in range(2000)]
    corpus_db = _make_corpus_db(tmp_path, crawled)
    classification_store._crawled_corpus_cache.clear()

    fake_conn = _RecordingPostgresConnection([
        {"domain": "crawled-0.ai", "routing_bucket": "priority"},
        {"domain": "crawled-1.ai", "routing_bucket": "priority"},
        {"domain": "crawled-2.ai", "routing_bucket": "review"},
        {"domain": "not-crawled.ai", "routing_bucket": "priority"},
    ])
    monkeypatch.setattr(
        classification_store, "using_classification_postgres", lambda: True
    )
    monkeypatch.setattr(
        classification_store, "connect_store", lambda *a, **k: fake_conn
    )

    counts = corpus_bucket_counts(corpus_db)

    assert len(fake_conn.queries) == 1
    assert counts == {"priority": 2, "review": 1}


def test_query_corpus_classifications_uses_one_query_regardless_of_corpus_size(
    tmp_path, monkeypatch
):
    crawled = [f"crawled-{i}.ai" for i in range(2000)]
    corpus_db = _make_corpus_db(tmp_path, crawled)
    classification_store._crawled_corpus_cache.clear()

    fake_conn = _RecordingPostgresConnection([
        {
            "domain": "crawled-0.ai",
            "routing_bucket": "priority",
            "fit_score": 90,
            "priority_score": 90,
            "record_json": "{}",
        },
        {
            "domain": "not-crawled.ai",
            "routing_bucket": "priority",
            "fit_score": 99,
            "priority_score": 99,
            "record_json": "{}",
        },
    ])
    monkeypatch.setattr(
        classification_store, "using_classification_postgres", lambda: True
    )
    monkeypatch.setattr(
        classification_store, "connect_store", lambda *a, **k: fake_conn
    )

    rows = query_corpus_classifications(corpus_db, ["priority"])

    assert len(fake_conn.queries) == 1
    assert [row["domain"] for row in rows] == ["crawled-0.ai"]


class _FakeCorpusPostgresConnection:
    """Fakes the ai_search/zone_universe connect()'s Postgres return value:
    a connection whose rows support dict-style access and that must be
    closed explicitly (unlike the classification store's context-managed
    connect_store)."""

    def __init__(self, rows):
        self._rows = rows
        self.closed = False

    def execute(self, _sql):
        return self._rows

    def close(self):
        self.closed = True


def test_crawled_corpus_domains_uses_connect_fn_when_local_file_is_absent(
    tmp_path, monkeypatch
):
    # Reproduces the exact production bug: ai_search/zone_universe moved to
    # their own Postgres backend, and the local SQLite corpus file was
    # correctly identified as unused and deleted -- but corpus_bucket_counts
    # and query_corpus_classifications still assumed a local file and
    # silently returned nothing ("Bucketed records: 0") instead of erroring.
    classification_store._crawled_corpus_cache.clear()
    missing_corpus_db = tmp_path / "does-not-exist.sqlite3"
    assert not missing_corpus_db.exists()

    fake_corpus_conn = _FakeCorpusPostgresConnection([
        {"domain": "crawled-0.ai"},
        {"domain": "crawled-1.ai"},
    ])
    connect_calls = []

    def fake_connect_fn(path, *, read_only=False):
        connect_calls.append((path, read_only))
        return fake_corpus_conn

    domains = classification_store._crawled_corpus_domains(
        missing_corpus_db, connect_fn=fake_connect_fn
    )

    assert domains == ["crawled-0.ai", "crawled-1.ai"]
    assert connect_calls == [(missing_corpus_db, True)]
    assert fake_corpus_conn.closed is True


def test_corpus_bucket_counts_works_via_connect_fn_without_a_local_file(
    tmp_path, monkeypatch
):
    classification_store._crawled_corpus_cache.clear()
    missing_corpus_db = tmp_path / "does-not-exist.sqlite3"

    fake_corpus_conn = _FakeCorpusPostgresConnection([
        {"domain": "crawled-0.ai"},
    ])
    fake_classification_conn = _RecordingPostgresConnection([
        {"domain": "crawled-0.ai", "routing_bucket": "priority"},
        {"domain": "not-crawled.ai", "routing_bucket": "priority"},
    ])
    monkeypatch.setattr(
        classification_store, "using_classification_postgres", lambda: True
    )
    monkeypatch.setattr(
        classification_store, "connect_store", lambda *a, **k: fake_classification_conn
    )

    counts = classification_store.corpus_bucket_counts(
        missing_corpus_db, connect_fn=lambda path, **k: fake_corpus_conn
    )

    assert counts == {"priority": 1}
