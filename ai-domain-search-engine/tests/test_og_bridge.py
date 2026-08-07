import sqlite3

from ai_search.database import add_domain, connect, set_worker_state, utc_now
from ai_search.og_bridge import (
    complete_screening_records,
    crawler_state,
    enqueue_for_screening,
    screenable_domains,
)


def create_queue(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE domains(
            domain TEXT PRIMARY KEY,
            first_seen TEXT,
            last_seen TEXT,
            status TEXT,
            attempts INTEGER,
            next_attempt_at TEXT,
            last_processed_at TEXT,
            classification TEXT,
            last_error TEXT,
            seen_count INTEGER,
            first_seen_run_id TEXT,
            last_seen_run_id TEXT
        )
        """
    )
    conn.commit()
    return conn


def test_crawler_state_exposes_worker_report(tmp_path):
    search_db = tmp_path / "search.sqlite3"
    conn = connect(search_db)
    set_worker_state(conn, "worker_status", "running")
    set_worker_state(conn, "crawler_status", "paused")
    conn.close()

    assert crawler_state(search_db) == {
        "worker_status": "running",
        "crawler_status": "paused",
    }


def test_successful_new_crawl_requeues_stale_ghost(tmp_path):
    search_db = tmp_path / "search.sqlite3"
    queue_db = tmp_path / "queue.sqlite3"
    search_conn = connect(search_db)
    add_domain(search_conn, "recovered.ai", "fixture")
    domain_id = search_conn.execute(
        "SELECT id FROM domains WHERE domain='recovered.ai'"
    ).fetchone()[0]
    search_conn.execute(
        """
        INSERT INTO pages(
            domain_id,url,final_url,title,cleaned_text,raw_html,
            crawled_at,word_count
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            domain_id,
            "https://recovered.ai",
            "https://recovered.ai",
            "Recovered",
            "Enterprise robotics platform",
            "<p>Enterprise robotics platform</p>",
            "2026-07-29T12:00:00Z",
            3,
        ),
    )
    search_conn.execute(
        """
        UPDATE domains SET
            latest_crawl_status='success',
            last_crawled_at='2026-07-29T12:00:00Z'
        WHERE id=?
        """,
        (domain_id,),
    )
    search_conn.commit()
    search_conn.close()

    queue_conn = create_queue(queue_db)
    queue_conn.execute(
        """
        INSERT INTO domains(
            domain,first_seen,last_seen,status,attempts,next_attempt_at,
            last_processed_at,classification,last_error,seen_count,
            first_seen_run_id,last_seen_run_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "recovered.ai",
            utc_now(),
            utc_now(),
            "complete",
            1,
            None,
            "2026-07-28T12:00:00Z",
            "ghost",
            "unreachable",
            1,
            "old",
            "old",
        ),
    )
    queue_conn.commit()
    queue_conn.close()

    domains = screenable_domains(
        search_db,
        exclude_queue_db=queue_db,
        include_stale=True,
    )
    assert domains == ["recovered.ai"]
    result = enqueue_for_screening(
        queue_db,
        domains,
        requeue_existing=True,
    )
    assert result["queued"] == 1

    queue_conn = sqlite3.connect(queue_db)
    row = queue_conn.execute(
        "SELECT status,attempts,last_error FROM domains "
        "WHERE domain='recovered.ai'"
    ).fetchone()
    queue_conn.close()
    assert row == ("pending", 0, "")


def test_enqueue_normalizes_so_variants_never_double_count(tmp_path):
    # register_domain_event (the other entry point into this same queue)
    # already normalizes on its own -- this function must guarantee the
    # same regardless of what any caller passes in, or "Www.Foo.com" and
    # "foo.com/" pass this function's own dedup (dict.fromkeys on the raw
    # strings) as two different domains and insert as two rows for what
    # is really the same one, inflating the total domain count.
    queue_db = tmp_path / "queue.sqlite3"
    create_queue(queue_db).close()

    result = enqueue_for_screening(
        queue_db,
        ["Www.Foo.com", "https://foo.com/", "foo.com", "bar.io"],
    )

    assert result["queued"] == 2
    queue_conn = sqlite3.connect(queue_db)
    domains = {
        row[0] for row in
        queue_conn.execute("SELECT domain FROM domains").fetchall()
    }
    queue_conn.close()
    assert domains == {"foo.com", "bar.io"}


def test_direct_classification_completes_canonical_queue_record(tmp_path):
    queue_db = tmp_path / "queue.sqlite3"
    queue_conn = create_queue(queue_db)
    now = utc_now()
    queue_conn.execute(
        """
        INSERT INTO domains(
            domain,first_seen,last_seen,status,attempts,next_attempt_at,
            last_processed_at,classification,last_error,seen_count,
            first_seen_run_id,last_seen_run_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "promising.app",
            now,
            now,
            "pending",
            0,
            now,
            None,
            "",
            "",
            1,
            "zone",
            "zone",
        ),
    )
    queue_conn.commit()
    queue_conn.close()

    assert complete_screening_records(
        queue_db,
        {"promising.app": "priority"},
    ) == 1

    queue_conn = sqlite3.connect(queue_db)
    row = queue_conn.execute(
        "SELECT status,classification,next_attempt_at,last_error "
        "FROM domains WHERE domain='promising.app'"
    ).fetchone()
    queue_conn.close()
    assert row == ("complete", "priority", None, "")
