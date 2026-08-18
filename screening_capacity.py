#!/usr/bin/env python3
"""Size the screening pool from measured feedback, not a constant.

Concurrency was a hardcoded 40. That number was arrived at honestly -- raised
from 24 after the extension filter widened, with fetch failure rate, scoring
headroom and RSS all checked -- but it is still a single value chosen once, on
one day, against one arrival rate. When intake outruns screening the queue
grows and domains go unlooked-at; when it does not, the same number wastes
sockets and memory. Neither condition announces itself.

The gap between arrival and capacity could not be measured from a laptop: the
local queue snapshot stopped receiving rows weeks ago, and production
credentials are not present. So this is written to need no such measurement --
it is a closed loop on two signals the worker already publishes every batch:

- ``fetch_failure_rate`` -- the share of fetches that failed. Rising failures
  are the symptom of pushing concurrency past what DNS, sockets and the event
  loop will carry.
- ``worker_rss_mb`` -- resident memory, which is what actually kills this
  worker: it recycles itself past its own ceiling, and a recycle costs the
  whole in-flight wave.

Ramp up only while there is a backlog to justify it *and* failures stay low;
back off the moment either signal says otherwise. If intake never outruns
screening the pool simply sits at its floor and nothing changes.

Two thresholds rather than one, on purpose. A single trip point makes a
controller oscillate: it steps up, crosses the line, steps down, and thrashes
concurrency (and therefore the failure rate) forever. The gap between
``failure_floor`` and ``failure_ceiling`` is the dead band where it holds.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Where the loop is allowed to operate. The floor is the old non-surge
# concurrency, so the pool can always fall back to a level this pipeline has
# run at for months. The ceiling starts modest above the validated 40 because
# nothing above that has ever been observed here -- this explores upward, it
# does not leap.
WORKER_FLOOR = max(1, int(os.getenv("SCREENING_WORKER_FLOOR", "10")))
WORKER_CEILING = max(WORKER_FLOOR, int(os.getenv("SCREENING_WORKER_CEILING", "80")))

# Failure-rate dead band. 0.25-0.35 was measured as normal across 10, 24 and
# 40 workers, so the ceiling sits above the top of the observed healthy range
# and the floor at its bottom.
FAILURE_CEILING = float(os.getenv("SCREENING_FAILURE_CEILING", "0.45"))
FAILURE_FLOOR = float(os.getenv("SCREENING_FAILURE_FLOOR", "0.30"))

# Back off well before the worker's own recycle threshold (1200 MB): a recycle
# discards the in-flight wave, so hitting it costs more than the extra
# concurrency ever bought.
RSS_CEILING_MB = float(os.getenv("SCREENING_RSS_CEILING_MB", "900"))

# Below this there is no backlog worth spending concurrency on.
BACKLOG_FOR_RAMP = int(os.getenv("SCREENING_BACKLOG_FOR_RAMP", "250"))

_STEP_UP = 1.25
_STEP_DOWN = 0.7


@dataclass(frozen=True)
class CapacityDecision:
    workers: int
    reason: str
    changed: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "screening_workers": self.workers,
            "screening_capacity_reason": self.reason,
        }


def next_worker_count(
    current: int,
    *,
    failure_rate: float | None,
    rss_mb: float | None,
    pending: int,
    floor: int = WORKER_FLOOR,
    ceiling: int = WORKER_CEILING,
    failure_floor: float = FAILURE_FLOOR,
    failure_ceiling: float = FAILURE_CEILING,
    rss_ceiling_mb: float = RSS_CEILING_MB,
    backlog_for_ramp: int = BACKLOG_FOR_RAMP,
) -> CapacityDecision:
    """Decide the next screening concurrency from the last batch's signals.

    ``failure_rate`` or ``rss_mb`` of ``None`` means the worker has not
    reported yet -- the first batch after a deploy. That is treated as "hold",
    never as "healthy": ramping on absent evidence is how a controller runs
    away before it has seen a single result.
    """
    current = max(floor, min(ceiling, int(current or floor)))

    if rss_mb is not None and rss_mb >= rss_ceiling_mb:
        # Memory outranks throughput. A recycle costs the whole in-flight wave.
        target = max(floor, int(current * _STEP_DOWN))
        return CapacityDecision(
            target,
            f"memory {rss_mb:.0f}MB at/over {rss_ceiling_mb:.0f}MB ceiling",
            target != current,
        )

    if failure_rate is None or rss_mb is None:
        return CapacityDecision(current, "holding: no measurement yet", False)

    if failure_rate >= failure_ceiling:
        target = max(floor, int(current * _STEP_DOWN))
        return CapacityDecision(
            target,
            f"fetch failures {failure_rate:.0%} at/over {failure_ceiling:.0%}",
            target != current,
        )

    if pending < backlog_for_ramp:
        # Screening is keeping up. Extra concurrency would buy nothing and
        # still cost sockets and memory.
        target = max(floor, min(current, int(current * _STEP_DOWN)))
        return CapacityDecision(
            target,
            f"backlog {pending} under {backlog_for_ramp}; screening keeps up",
            target != current,
        )

    if failure_rate <= failure_floor:
        target = min(ceiling, max(current + 1, int(current * _STEP_UP)))
        return CapacityDecision(
            target,
            f"backlog {pending} with failures {failure_rate:.0%} under "
            f"{failure_floor:.0%}",
            target != current,
        )

    return CapacityDecision(
        current,
        f"holding: failures {failure_rate:.0%} inside the "
        f"{failure_floor:.0%}-{failure_ceiling:.0%} dead band",
        False,
    )
