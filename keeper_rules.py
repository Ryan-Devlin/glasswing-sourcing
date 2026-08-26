"""What is worth keeping, defined once.

The export copies these rows out and the lean-out deletes their complement. If
the two ever drifted apart, the export would be a promise about data that had
already been deleted -- so both import from here rather than each carrying its
own copy of the rule.

Phrased through ``classifications`` rather than against ``routing_bucket``
directly, because it has to hold for the satellite tables too, and those have
no bucket column of their own -- only a domain.
"""
from __future__ import annotations

import os

# The investable bucket, plus comparables: 760 rows of bucketed work that cost
# almost nothing to carry.
KEEP_BUCKETS: tuple[str, ...] = tuple(
    bucket.strip()
    for bucket in os.environ.get("KEEP_BUCKETS", "priority,comp").split(",")
    if bucket.strip()
)

_KEEP_SQL = """
    domain IN (SELECT domain FROM claude_diligence)
 OR domain IN (SELECT domain FROM tracked_domains)
 OR domain IN (SELECT domain FROM manual_decisions)
 OR domain IN (SELECT domain FROM training_labels)
 OR domain IN (SELECT domain FROM classifications WHERE routing_bucket IN ({marks}))
"""


def keep_clause(buckets: tuple[str, ...] = KEEP_BUCKETS) -> str:
    """A SQL boolean over ``domain``, true for rows that survive.

    Takes one parameter per kept bucket, in order -- see ``keep_params``.
    """
    return _KEEP_SQL.format(marks=",".join("?" for _ in buckets))


def keep_params(buckets: tuple[str, ...] = KEEP_BUCKETS) -> list[str]:
    return list(buckets)
