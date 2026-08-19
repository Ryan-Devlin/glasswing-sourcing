"""The rescue scan has to be able to reach every eligible row eventually.

The live rescore exists because a stored score goes stale as scoring improves
and buries real companies (the fluenci.ai bug). Bounding the scan to stop an
OOM quietly reintroduced that bug for most of the corpus: measured on the live
store 2026-08-19, the newest-first 25,000-row window contained 5,996 eligible
rows and 91,286 eligible rows sat outside it, permanently unreachable.
"""

import classification_store as store


def test_the_scan_only_reads_buckets_diligence_can_draw_from():
    """64% of the corpus is reject/insufficient. Spending the scan budget on
    them is why the window saw 5,996 eligible rows out of 25,000."""
    import inspect

    source = inspect.getsource(store.iter_eligible_scoring_rows)
    assert "routing_bucket IN ('review','priority','comp')" in source
    assert "LEFT JOIN claude_diligence" in source


def test_the_scan_is_ordered_by_primary_key_not_recency():
    """A newest-first window resamples the same head forever. Ordering on the
    primary key makes a rotating offset cover the whole set."""
    import inspect

    source = inspect.getsource(store.iter_eligible_scoring_rows)
    assert "ORDER BY c.domain" in source
    assert "updated_at DESC" not in source
    assert "OFFSET" in source


def test_the_cursor_advances_even_when_a_window_finds_nothing():
    """A cursor that only moved on success would stick on a barren stretch
    and never reach the rows past it."""
    import inspect

    import build_claude_candidates as builder

    source = inspect.getsource(builder.eligible_domains)
    advance = source.index("write_store_meta(")
    # The advance must not sit inside the per-row keep branch.
    assert "cursor + rescue_limit" in source[advance:advance + 200]
    assert source.count("write_store_meta(") == 1


def test_a_cursor_past_the_end_wraps_to_the_start():
    import inspect

    import build_claude_candidates as builder

    source = inspect.getsource(builder.eligible_domains)
    assert "cursor >= eligible_total" in source
    assert "cursor = 0" in source


def test_an_unsaveable_cursor_costs_coverage_not_correctness():
    """Writing the cursor must never take down candidate building."""
    import inspect

    import build_claude_candidates as builder

    source = inspect.getsource(builder.eligible_domains)
    tail = source[source.index("write_store_meta"):]
    assert "except Exception" in tail
    assert "return keep" in tail


def test_store_meta_round_trips(tmp_path):
    path = tmp_path / "store.sqlite3"
    with store.connect_store(path) as conn:
        conn.commit()
    store.write_store_meta("claude_rescue_scan_offset", "25000", path=path)
    assert store.read_store_meta("claude_rescue_scan_offset", path=path) == "25000"
    store.write_store_meta("claude_rescue_scan_offset", "50000", path=path)
    assert store.read_store_meta("claude_rescue_scan_offset", path=path) == "50000"


def test_a_missing_key_returns_the_default(tmp_path):
    path = tmp_path / "store.sqlite3"
    with store.connect_store(path) as conn:
        conn.commit()
    assert store.read_store_meta("never_written", "0", path=path) == "0"
