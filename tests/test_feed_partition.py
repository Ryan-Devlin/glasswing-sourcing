"""The CertStream tiles have to add up to the box above them.

They did not, and could not: two of the boxes were subtotals of the rows
below them rather than siblings, so the row double-counted every screened
domain, while the guards' own work -- the largest share by far -- had no box.
"""

from funnel import FEED_PARTS, partition_feed

PARTS = ("junk", "repeats", "still_processing", "no_verdict",
         "candidates", "filtered_out")


def total(split):
    return sum(split[key] for key in PARTS)


def test_the_parts_sum_to_the_feed():
    split = partition_feed(
        observed=159_365, junk_dropped=142_000, queued=369,
        unscreened=120, no_verdict=30, candidates=45,
    )
    assert total(split) == split["observed"] == 159_365


def test_repeats_absorb_what_no_counter_records():
    """A domain already on the queue is never written again, so nothing
    tallies it. It is the residual of intake, not a measurement."""
    split = partition_feed(
        observed=1_000, junk_dropped=600, queued=300,
        unscreened=300, no_verdict=0, candidates=0,
    )
    assert split["repeats"] == 100
    assert total(split) == 1_000


def test_a_stale_guard_tally_cannot_exceed_the_feed():
    """Guard drops are cumulative and the hostname count resets; a tally
    larger than the population it describes rendered as a 4040% drop rate."""
    split = partition_feed(
        observed=500, junk_dropped=999_999, queued=10,
        unscreened=10, no_verdict=0, candidates=0,
    )
    assert split["junk"] == 500
    assert total(split) == 500
    assert all(split[key] >= 0 for key in PARTS)


def test_stores_disagreeing_mid_write_still_close():
    """Queue and classification store are read in separate statements. The
    last part is the remainder so the row cannot miss its own total."""
    split = partition_feed(
        observed=1_000, junk_dropped=0, queued=1_000,
        unscreened=900, no_verdict=900, candidates=900,
    )
    assert total(split) == 1_000
    assert all(split[key] >= 0 for key in PARTS)


def test_an_idle_run_is_all_zeros_not_a_negative():
    split = partition_feed(observed=0, junk_dropped=0, queued=0,
                           unscreened=0, no_verdict=0, candidates=0)
    assert total(split) == 0


def test_every_part_has_a_tile_label():
    """A part with no label is a number that silently vanishes from the row
    and breaks the sum the user reads."""
    assert {key for key, _ in FEED_PARTS} == set(PARTS)
