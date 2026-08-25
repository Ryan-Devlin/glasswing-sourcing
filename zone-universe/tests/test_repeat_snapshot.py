"""A repeat CZDS snapshot must not close the day out.

CZDS sometimes serves the previous day's zone file again. The diff is then
against content already held, so added and removed both come back zero and
nothing is queued -- which on the dashboard is indistinguishable from a working
day that happened to find no new domains. Observed live: .app on 2026-08-23,
then both .app and .com on 2026-08-25, each time silently, with
import_status "ready" and an empty import_error.

Every real day moves both counters. .com runs 100-165k added against 95-128k
removed; .app a few thousand of each. Zero of both, against a prior snapshot,
means the same file came back.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from zone_universe import worker


def classify(added, removed, unchanged):
    """The worker's own repeat test, kept in one place."""
    return (
        int(added or 0) == 0
        and int(removed or 0) == 0
        and int(unchanged or 0) > 0
    )


def test_a_repeated_snapshot_is_recognised():
    # 2026-08-25 .com, exactly as recorded.
    assert classify(0, 0, 162_882_674) is True
    # 2026-08-23 .app.
    assert classify(0, 0, 1_480_649) is True


def test_a_real_day_is_not_a_repeat():
    assert classify(107_000, 95_713, 162_775_674) is False
    assert classify(6_118, 2_283, 1_478_366) is False
    # Even a day that only removed domains has moved.
    assert classify(0, 4_120, 1_480_000) is False


def test_a_first_import_is_not_a_repeat():
    """The first import of a zone is deliberately a baseline: it records the
    universe without queueing 163 million existing domains, so it reports zero
    added and zero removed too. unchanged_domains is what separates them --
    it only counts when there was a prior snapshot to compare against -- and
    without that distinction a new zone would be retried forever.
    """
    assert classify(0, 0, 0) is False


def test_the_worker_only_closes_the_day_on_a_fresh_delta():
    """The guard that makes the retry possible at all. Re-importing after a
    *successful* import destroys that day's delta, which is why the per-TLD
    day key exists; after a repeat it is safe, because the snapshot just
    written is identical to the one before it.
    """
    source = (SRC / "zone_universe" / "worker.py").read_text()
    assert "if not repeated_tlds:" in source
    assert 'set_state(conn, "last_czds_snapshot_day", schedule_day)' in source
    # And the per-TLD key is skipped on a repeat rather than set.
    assert "if repeated:" in source
    assert "czds_repeat_snapshot" in source
