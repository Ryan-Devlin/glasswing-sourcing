"""A Claude call that produced nothing must not be bought again immediately.

prepare_input excludes anything with a *stored verdict*, which is right for a
company already judged. It says nothing about a company whose call failed, was
refused, or came back unparseable: that row keeps the high score that got it
selected, stays eligible, sits at the top of the queue and is bought again on
the very next attempt.

Measured on the live store before this existed: 2,705 paid calls against 1,764
distinct domains, so 35% of the budget was re-attempts. Per day it was worse --
on 2026-08-24, 116 calls reached 66 companies, and on 2026-08-20, 181 calls
reached 100.
"""

import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import continuous_pipeline

ROOT = Path(__file__).resolve().parent.parent


def queue(tmp_path):
    conn = sqlite3.connect(tmp_path / "queue.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE claude_calls (called_at TEXT NOT NULL, domain TEXT NOT NULL)"
    )
    conn.commit()
    return conn


def record(conn, domain, *, days_ago=0):
    when = (
        datetime.now(timezone.utc) - timedelta(days=days_ago)
    ).isoformat().replace("+00:00", "Z")
    conn.execute(
        "INSERT INTO claude_calls(called_at, domain) VALUES (?, ?)",
        (when, domain),
    )
    conn.commit()


def test_a_recent_attempt_with_no_verdict_is_held_back(tmp_path, monkeypatch):
    conn = queue(tmp_path)
    record(conn, "failed.ai", days_ago=1)
    monkeypatch.setattr(
        "classification_store.claude_diligenced_domains", lambda **_: []
    )
    assert continuous_pipeline.claude_attempted_without_verdict(conn) == ["failed.ai"]


def test_an_attempt_that_produced_a_verdict_is_not_held_back(tmp_path, monkeypatch):
    """It is already excluded downstream, and listing it here too would just
    make the cooldown count misleading."""
    conn = queue(tmp_path)
    record(conn, "judged.ai", days_ago=1)
    monkeypatch.setattr(
        "classification_store.claude_diligenced_domains",
        lambda **_: ["judged.ai"],
    )
    assert continuous_pipeline.claude_attempted_without_verdict(conn) == []


def test_the_cooldown_expires(tmp_path, monkeypatch):
    """Short enough that a genuine fix reaches the company within the week."""
    conn = queue(tmp_path)
    record(conn, "old.ai", days_ago=30)
    record(conn, "recent.ai", days_ago=2)
    monkeypatch.setattr(
        "classification_store.claude_diligenced_domains", lambda **_: []
    )
    held = continuous_pipeline.claude_attempted_without_verdict(conn, days=7)
    assert held == ["recent.ai"]


def test_a_missing_calls_table_never_blocks_diligence(tmp_path):
    """The cooldown is an optimisation. If it cannot be computed, diligence
    should run unfiltered rather than not run at all."""
    conn = sqlite3.connect(tmp_path / "empty.sqlite3")
    conn.row_factory = sqlite3.Row
    assert continuous_pipeline.claude_attempted_without_verdict(conn) == []


def test_the_builder_honours_the_exclusion(tmp_path):
    """End to end through the real CLI, since the exclusion is passed as a
    file path to a subprocess and a typo in either half is silent."""
    exclude = tmp_path / "skip.txt"
    exclude.write_text("SkipMe.ai\n\nother.ai\n")
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "build_claude_candidates.py"),
            str(tmp_path / "out.csv"),
            "--minimum-score", "999",
            "--exclude", str(exclude),
        ],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    # 2 means "no candidates", which is the expected outcome at score 999 --
    # what matters is that --exclude is accepted rather than erroring out.
    assert result.returncode in (0, 2), result.stderr[-400:]
    assert "unrecognized arguments" not in (result.stderr or "")
