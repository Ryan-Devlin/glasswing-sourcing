"""Domains that never served a page get a second look, and only those.

A TLS certificate is issued when a domain is *provisioned*, days or weeks
before anything is deployed at it -- which is the whole reason CertStream is
worth watching, and also why most of what it delivers is an empty address.
Measured on the live corpus: 52,829 rows, 16.6% of everything ever screened,
carry a terminal verdict recorded against a page nobody ever saw. The funnel
names it as the largest single loss in the pipeline, bigger than every factual
disqualifier combined.

The narrowness is the design. Two further looks, at 14 and 30 days from the
first screening, and only for reasons meaning "there was nothing to read".
Re-deciding a settled question would spend the crawl budget the reopening is
for.
"""

import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd

import continuous_pipeline
import revisit


def queue(tmp_path):
    conn = sqlite3.connect(tmp_path / "queue.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE domains (
            domain TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            last_processed_at TEXT,
            classification TEXT,
            last_error TEXT DEFAULT '',
            seen_count INTEGER NOT NULL DEFAULT 1,
            first_seen_run_id TEXT NOT NULL DEFAULT '',
            last_seen_run_id TEXT NOT NULL DEFAULT ''
        )
    """)
    continuous_pipeline.ensure_domain_registry_schema(conn)
    revisit.ensure_schema(conn)
    return conn


def add_domain(conn, domain, *, status="complete", attempts=3):
    now = revisit.utc_now()
    conn.execute(
        "INSERT INTO domains(domain,first_seen,last_seen,status,attempts,"
        "next_attempt_at,last_error) VALUES (?,?,?,?,?,?,?)",
        (domain, now, now, status, attempts, now, "timeout"),
    )
    conn.commit()


def test_only_an_unread_page_earns_a_second_look(tmp_path):
    conn = queue(tmp_path)
    output = pd.DataFrame([
        {"domain": "parked.ai", "routing_reason": "parked_or_for_sale"},
        {"domain": "placeholder.ai",
         "routing_reason": "generic_hosting_or_registrar_placeholder"},
        {"domain": "unreachable.ai",
         "routing_reason": "website_never_reachable_after_max_attempts"},
        # These were read. The answer will not have changed in a fortnight.
        {"domain": "consumer.ai", "routing_reason": "consumer_b2c_product"},
        {"domain": "agency.ai", "routing_reason": "agency_or_consultancy"},
        {"domain": "nofit.ai", "routing_reason": "no_clear_enterprise_or_glasswing_fit"},
    ])
    assert continuous_pipeline.schedule_batch_revisits(conn, output) == 3
    queued = {
        str(row["domain"]) for row in
        conn.execute("SELECT domain FROM revisit_queue").fetchall()
    }
    assert queued == {"parked.ai", "placeholder.ai", "unreachable.ai"}


def test_a_scheduled_revisit_is_not_due_yet(tmp_path):
    """Two weeks, not immediately -- reopening it now would just read the same
    empty page and burn one of its two looks."""
    conn = queue(tmp_path)
    add_domain(conn, "parked.ai")
    continuous_pipeline.schedule_batch_revisits(conn, pd.DataFrame([
        {"domain": "parked.ai", "routing_reason": "parked_or_for_sale"},
    ]))
    assert continuous_pipeline.requeue_due_revisits(conn) == 0
    row = conn.execute(
        "SELECT status FROM domains WHERE domain='parked.ai'"
    ).fetchone()
    assert row["status"] == "complete"


def test_a_due_revisit_goes_back_to_pending_with_a_clean_slate(tmp_path):
    """The retry counter measures "this fetch keeps failing". A revisit is a
    new question asked a fortnight later, so it must not inherit that."""
    conn = queue(tmp_path)
    add_domain(conn, "parked.ai", status="complete", attempts=3)
    past = (
        datetime.now(timezone.utc) - timedelta(days=20)
    ).isoformat().replace("+00:00", "Z")
    conn.execute(
        "INSERT INTO revisit_queue(domain,first_reason,first_seen_at,attempts,"
        "next_visit_at,last_outcome,updated_at) VALUES (?,?,?,0,?,'',?)",
        ("parked.ai", "parked_or_for_sale", past, past, past),
    )
    conn.commit()

    assert continuous_pipeline.requeue_due_revisits(conn) == 1
    row = conn.execute(
        "SELECT status, attempts, last_error FROM domains WHERE domain='parked.ai'"
    ).fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] == ""


def test_a_domain_gets_two_looks_and_then_retires(tmp_path):
    """A domain still parked at 30 days was bought and never used. A third
    visit costs more than it returns."""
    conn = queue(tmp_path)
    add_domain(conn, "parked.ai")
    long_ago = (
        datetime.now(timezone.utc) - timedelta(days=400)
    ).isoformat().replace("+00:00", "Z")
    conn.execute(
        "INSERT INTO revisit_queue(domain,first_reason,first_seen_at,attempts,"
        "next_visit_at,last_outcome,updated_at) VALUES (?,?,?,0,?,'',?)",
        ("parked.ai", "parked_or_for_sale", long_ago, long_ago, long_ago),
    )
    conn.commit()

    assert continuous_pipeline.requeue_due_revisits(conn) == 1
    assert continuous_pipeline.requeue_due_revisits(conn) == 1
    # Out of looks, and gone rather than sitting on a far-future date.
    assert continuous_pipeline.requeue_due_revisits(conn) == 0
    assert revisit.pending_count(conn) == 0


def test_rescreening_a_still_empty_domain_does_not_reset_its_looks(tmp_path):
    """Otherwise a parked domain loops fortnightly forever and never retires."""
    conn = queue(tmp_path)
    output = pd.DataFrame([
        {"domain": "parked.ai", "routing_reason": "parked_or_for_sale"},
    ])
    assert continuous_pipeline.schedule_batch_revisits(conn, output) == 1
    assert continuous_pipeline.schedule_batch_revisits(conn, output) == 0
    row = conn.execute(
        "SELECT attempts FROM revisit_queue WHERE domain='parked.ai'"
    ).fetchone()
    assert row["attempts"] == 0


def test_a_broken_revisit_never_takes_the_batch_down_with_it(tmp_path):
    """A second look is an extra chance. Failing to schedule one must not fail
    the batch that is actually in front of us -- the listener's own
    except-clause treats anything that throws as a connection fault, and that
    has turned into a silent reconnect storm three times in this pipeline.
    """
    conn = queue(tmp_path)
    conn.execute("DROP TABLE revisit_queue")
    conn.commit()
    conn.close()  # every subsequent use raises

    assert continuous_pipeline.schedule_batch_revisits(conn, pd.DataFrame([
        {"domain": "parked.ai", "routing_reason": "parked_or_for_sale"},
    ])) == 0
    assert continuous_pipeline.requeue_due_revisits(conn) == 0
