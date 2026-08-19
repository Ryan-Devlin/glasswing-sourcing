"""The intake counters that have to survive a worker restart.

CertStream drop tallies lived only in the status record, which holds current
state and not history. A restart therefore reset the top of the funnel to
zero mid-day, which is the specific way a "domains found today" number lies.
"""

import ast
from pathlib import Path

SOURCE = Path("continuous_pipeline.py").read_text(encoding="utf-8")


def test_intake_drops_is_bound_exactly_once_in_the_listener():
    """It was bound twice. The second binding restored drops from a previous
    run without the run-id check or the drops-cannot-exceed-observed reset
    that the first one applies, so the guarded value was thrown away one line
    after being computed and the funnel showed every hostname dying at the
    first guard."""
    tree = ast.parse(SOURCE)
    listener = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "listener"
    )
    bindings = [
        node for node in ast.walk(listener)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "intake_drops"
    ]
    assert len(bindings) == 1, (
        f"intake_drops annotated-assigned {len(bindings)} times; a second "
        "binding silently discards the invariant applied to the first"
    )


def test_the_invariant_reset_is_not_dead_code():
    """The reset must come after the last binding, or it cannot take."""
    guard = SOURCE.index("if sum(intake_drops.values()) > domains_observed:")
    flush = SOURCE.index("intake_status_flushed_at = time.monotonic()")
    between = SOURCE[guard:flush]
    assert "intake_drops: dict[str, int] =" not in between


# --- the durable daily tally -------------------------------------------------

import sqlite3

from continuous_pipeline import read_intake_daily, record_intake_daily


def a_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE intake_daily (
            day TEXT NOT NULL, source TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '', count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL, PRIMARY KEY (day, source, reason)
        )
    """)
    return conn


def test_two_runs_on_one_day_add_up():
    """The restart case. Each run counts from zero, so absolutes would make
    the second run's flush overwrite the first run's whole contribution."""
    conn = a_db()
    record_intake_daily(conn, observed=1_000,
                        drops={"machine_generated": 400}, day="2026-08-19")
    record_intake_daily(conn, observed=750,
                        drops={"machine_generated": 300}, day="2026-08-19")
    day = read_intake_daily(conn, day="2026-08-19")["certstream"]
    assert day[""] == 1_750
    assert day["machine_generated"] == 700


def test_days_are_kept_apart():
    conn = a_db()
    record_intake_daily(conn, observed=10, drops={}, day="2026-08-18")
    record_intake_daily(conn, observed=20, drops={}, day="2026-08-19")
    assert read_intake_daily(conn, day="2026-08-19")["certstream"][""] == 20
    assert read_intake_daily(conn, day="2026-08-18")["certstream"][""] == 10


def test_a_negative_delta_never_eats_an_earlier_count():
    """The invariant reset in the listener zeroes its counters mid-run, which
    produces a negative delta on the next flush. Applying it would subtract
    work that genuinely happened."""
    conn = a_db()
    record_intake_daily(conn, observed=500, drops={"motif_spam": 100},
                        day="2026-08-19")
    record_intake_daily(conn, observed=-500, drops={"motif_spam": -100},
                        day="2026-08-19")
    day = read_intake_daily(conn, day="2026-08-19")["certstream"]
    assert day[""] == 500
    assert day["motif_spam"] == 100


def test_a_flush_with_nothing_new_writes_nothing():
    conn = a_db()
    record_intake_daily(conn, observed=0, drops={"motif_spam": 0},
                        day="2026-08-19")
    assert read_intake_daily(conn, day="2026-08-19") == {}


def test_sources_are_tallied_separately():
    conn = a_db()
    record_intake_daily(conn, observed=100, drops={}, day="2026-08-19")
    record_intake_daily(conn, observed=900, drops={}, source="zone",
                        day="2026-08-19")
    day = read_intake_daily(conn, day="2026-08-19")
    assert day["certstream"][""] == 100
    assert day["zone"][""] == 900
