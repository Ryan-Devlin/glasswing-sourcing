"""A domain the queue will never look at again must not be left stateless.

Measured live 2026-08-19: 588,402 rows marked complete carried no
classification, and 379,488 of those had no exclusion record either -- no
verdict, no tombstone, nothing. In every count they were indistinguishable
from "not screened yet", which is what made the funnel show a limbo state.

An unreadable site is genuinely not a judgement about a company; we never saw
one. But once it is out of retries it is terminal, and terminal means it gets
recorded.
"""

import inspect

import filter as screening_filter


SOURCE = inspect.getsource(screening_filter.persist_screening_output)


def test_terminal_unreadable_rows_are_recorded():
    assert "terminal_unreadable" in SOURCE
    assert "unreadable_no_retry_left" in SOURCE
    assert "record_automated_exclusions(" in SOURCE


def test_only_the_ones_out_of_retries_are_recorded():
    """A blocked or mid-deploy site is requeued, not written off. Recording
    it as terminal would close out a domain that is merely unreachable
    today."""
    window = SOURCE[SOURCE.index("terminal_unreadable = "):]
    window = window[:window.index(".copy()")]
    assert "~retryable_inaccessible" in window
    assert "unreadable" in window


def test_a_domain_with_an_existing_record_is_left_alone():
    """One bad fetch must not overwrite a good classification. This is the
    reason the original code persisted nothing at all here."""
    assert "classified_domain_set(" in SOURCE
    window = SOURCE[SOURCE.index("classified_domain_set("):]
    assert "~terminal_unreadable" in window[:400]


def test_nothing_is_purged_for_being_unreadable():
    """purge_automated_rejections deletes payloads. Unreadable is not a
    judgement, so it must never trigger deletion -- only the automated
    discard path does that."""
    purge = SOURCE.index("purge_automated_rejections(")
    # Just the argument list, not whatever happens to follow it.
    call = SOURCE[purge:SOURCE.index(")", SOURCE.index("clean_csv", purge))]
    assert "automated_discarded" in call
    assert "terminal_unreadable" not in call
    assert SOURCE.count("purge_automated_rejections(") == 1


def test_manually_protected_domains_are_never_recorded_as_unreadable():
    window = SOURCE[SOURCE.index("terminal_unreadable = "):]
    window = window[:window.index(".copy()")]
    assert "~manually_protected" in window


def test_the_count_is_reported_so_the_funnel_can_show_it():
    assert '"unreadable_recorded": unreadable_recorded' in SOURCE
    assert '"unreadable_recorded": 0' in SOURCE


def test_an_empty_batch_still_reports_the_key():
    """The early return runs when every domain in a batch timed out. A
    missing key there is a KeyError in the caller, not a zero."""
    import pandas as pd

    stats = screening_filter.persist_screening_output(pd.DataFrame())
    assert stats["unreadable_recorded"] == 0
