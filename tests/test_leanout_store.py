"""What the lean-out keeps, and what it must never keep by accident.

This deletes hundreds of thousands of rows from a live store and cannot be
undone, so every clause of the keep rule is pinned here rather than trusted.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
os.environ.setdefault("BACKFILL_REPO", str(REPO))
sys.path.insert(0, str(REPO))

import classification_store  # noqa: E402
import leanout_store  # noqa: E402


def store(monkeypatch, tmp_path):
    database = tmp_path / "store.sqlite3"
    monkeypatch.setattr(classification_store, "CLASSIFICATION_DB", database)
    with classification_store.connect_store(database):
        pass
    monkeypatch.setattr(leanout_store, "DRY", False)
    monkeypatch.setattr(leanout_store, "BATCH", 100)
    monkeypatch.setattr(leanout_store, "PAUSE", 0.0)
    # Local sqlite has no volume to fill; the floor is exercised separately.
    monkeypatch.setattr(leanout_store, "MIN_FREE_MB", 0.0)
    monkeypatch.setattr(leanout_store, "CAPACITY_MB", 10_000.0)
    return database


def add_classification(database, domain, bucket, score=50):
    with classification_store.connect_store(database) as conn:
        conn.execute(
            "INSERT INTO classifications(domain,routing_bucket,fit_score,"
            "record_json,classified_at,updated_at) VALUES (?,?,?,?,?,?)",
            (domain, bucket, score, '{"domain":"%s"}' % domain,
             "2026-08-20T00:00:00Z", "2026-08-20T00:00:00Z"),
        )
        conn.commit()


def add_row(database, table, **values):
    keys = ",".join(values)
    marks = ",".join("?" for _ in values)
    with classification_store.connect_store(database) as conn:
        conn.execute(
            f"INSERT INTO {table}({keys}) VALUES ({marks})",
            tuple(values.values()),
        )
        conn.commit()


def surviving(database):
    with classification_store.connect_store(database, read_only=True) as conn:
        return {
            str(r["domain"]) for r in
            conn.execute("SELECT domain FROM classifications").fetchall()
        }


def test_it_keeps_exactly_the_three_categories_and_human_truth(
    monkeypatch, tmp_path
):
    database = store(monkeypatch, tmp_path)

    add_classification(database, "investable.ai", "priority", 92)
    add_classification(database, "comparable.ai", "comp", 40)
    add_classification(database, "claude-called.ai", "reject", 12)
    add_classification(database, "tracked.ai", "review", 55)
    add_classification(database, "human-said-yes.ai", "reject", 8)
    add_classification(database, "labelled.ai", "insufficient", 3)
    # The ones that go: machine opinion, no call, no human.
    add_classification(database, "plain-review.ai", "review", 61)
    add_classification(database, "plain-reject.ai", "reject", 4)
    add_classification(database, "thin.ai", "insufficient", 1)

    add_row(database, "claude_diligence", domain="claude-called.ai",
            decision="REJECT", fit_score=20, updated_at="2026-08-20T00:00:00Z")
    add_row(database, "tracked_domains", domain="tracked.ai",
            tracked_at="2026-08-20T00:00:00Z")
    add_row(database, "manual_decisions", domain="human-said-yes.ai",
            target_label="yes", target_bucket="priority",
            decided_at="2026-08-20T00:00:00Z")
    add_row(database, "training_labels", domain="labelled.ai",
            label="yes", labeled_at="2026-08-20T00:00:00Z")

    leanout_store.main()

    assert surviving(database) == {
        "investable.ai",      # the investable bucket
        "comparable.ai",      # bucketed comparables
        "claude-called.ai",   # cost a Claude call, verdict irrelevant
        "tracked.ai",         # explicitly tracked
        "human-said-yes.ai",  # human truth
        "labelled.ai",        # human truth
    }


def test_a_high_scoring_review_row_is_still_only_a_machine_opinion(
    monkeypatch, tmp_path
):
    """Python scorer output is a signal, not truth. A 95 with no Claude call
    and no human decision is still a guess about a domain we can screen again.
    """
    database = store(monkeypatch, tmp_path)
    add_classification(database, "tempting.ai", "review", 95)
    add_classification(database, "keeper.ai", "priority", 70)

    leanout_store.main()

    assert surviving(database) == {"keeper.ai"}


def test_satellite_rows_follow_their_classification(monkeypatch, tmp_path):
    database = store(monkeypatch, tmp_path)
    add_classification(database, "investable.ai", "priority", 92)
    add_classification(database, "gone.ai", "reject", 4)
    add_row(database, "automated_exclusions", domain="gone.ai",
            reason="non_english", excluded_at="2026-08-20T00:00:00Z")
    add_row(database, "automated_exclusions", domain="investable.ai",
            reason="kept_for_reference", excluded_at="2026-08-20T00:00:00Z")

    leanout_store.main()

    with classification_store.connect_store(database, read_only=True) as conn:
        left = {
            str(r["domain"]) for r in
            conn.execute("SELECT domain FROM automated_exclusions").fetchall()
        }
    assert left == {"investable.ai"}


def test_it_refuses_to_run_without_headroom(monkeypatch, tmp_path):
    """Deleting rows writes WAL before it frees anything. Filling this volume
    is what took the database down on 2026-08-25, and the guard that was
    supposed to prevent it watched a number that excluded WAL."""
    database = store(monkeypatch, tmp_path)
    add_classification(database, "investable.ai", "priority", 92)
    monkeypatch.setattr(leanout_store, "MIN_FREE_MB", 9_999_999.0)

    with pytest.raises(SystemExit) as exit_info:
        leanout_store.main()
    assert exit_info.value.code == 1
    assert surviving(database) == {"investable.ai"}, "must not have deleted"


def test_a_dry_run_deletes_nothing(monkeypatch, tmp_path):
    database = store(monkeypatch, tmp_path)
    add_classification(database, "plain-reject.ai", "reject", 4)
    monkeypatch.setattr(leanout_store, "DRY", True)

    leanout_store.main()

    assert surviving(database) == {"plain-reject.ai"}


def test_transient_tables_are_cleared_entirely(monkeypatch, tmp_path):
    database = store(monkeypatch, tmp_path)
    add_classification(database, "investable.ai", "priority", 92)
    add_row(database, "admin_audit_log", action="login",
            created_at="2026-08-20T00:00:00Z")

    leanout_store.main()

    with classification_store.connect_store(database, read_only=True) as conn:
        n = int(conn.execute(
            "SELECT count(*) AS n FROM admin_audit_log"
        ).fetchone()["n"])
    assert n == 0


def test_the_export_and_the_deletion_cannot_disagree():
    """The export promises to have saved what the lean-out then deletes. Two
    copies of that rule is one copy too many: if they drifted, the export
    would be a promise about rows that had already gone."""
    import export_keepers
    import keeper_rules

    assert export_keepers.KEEP_PREDICATE == keeper_rules.keep_clause(
        keeper_rules.KEEP_BUCKETS
    )
    assert leanout_store.keep_clause() == keeper_rules.keep_clause(
        keeper_rules.KEEP_BUCKETS
    )
    assert leanout_store.KEEP_BUCKETS == export_keepers.KEEP_BUCKETS


def test_everything_kept_is_everything_exported(monkeypatch, tmp_path):
    """Whatever survives the lean-out must be exactly what the export wrote,
    checked against one store rather than by reading both predicates."""
    import export_keepers

    database = store(monkeypatch, tmp_path)
    add_classification(database, "investable.ai", "priority", 92)
    add_classification(database, "called.ai", "reject", 10)
    add_classification(database, "gone.ai", "review", 80)
    add_row(database, "claude_diligence", domain="called.ai", decision="REJECT",
            fit_score=11, updated_at="2026-08-20T00:00:00Z")

    out_dir = tmp_path / "export"
    monkeypatch.setattr(export_keepers, "OUT_DIR", out_dir)
    monkeypatch.setattr(export_keepers, "KEEP_BUCKETS",
                        leanout_store.KEEP_BUCKETS)
    export_keepers.main()

    import sqlite3
    written = sorted(out_dir.glob("glasswing_keepers_*.sqlite3"))
    assert written, "the export produced no file"
    con = sqlite3.connect(written[0])
    exported = {
        row[0] for row in con.execute("SELECT domain FROM classifications")
    }
    con.close()

    leanout_store.main()
    assert surviving(database) == exported == {"investable.ai", "called.ai"}
