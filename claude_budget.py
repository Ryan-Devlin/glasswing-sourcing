#!/usr/bin/env python3
"""Split a day's diligence candidates between immediate and batched calls.

The two ways to spend the Claude budget each give up something the other
keeps. A synchronous call returns a verdict in the same worker tick, which is
what you want for a company that looks like a real deal. The Batch API costs
half as much but answers within the hour, which is fine for the long tail and
wrong for the top of it.

So this routes by conviction rather than picking one:

- the highest-scoring candidates, up to ``sync_reserve``, go out synchronously
  and are answered immediately;
- everything else the budget allows goes into a batch at half price;
- the remainder waits for tomorrow.

At the default 100/day with a reserve of 10, that is 10 full-price plus 90
half-price calls -- 55 calls' worth of spend for 100 verdicts, a ~45% saving
against running all of them synchronously, while the best-looking companies
still surface the moment they are found.

The budget is denominated in **calls, not dollars**, because the cap is a
governor on how much diligence runs per day, not a spend target. Letting the
batch discount buy 200 half-price calls instead of 100 would honour the
dollars and break the cap.

Zero I/O: no client, no database, no clock beyond what the caller passes in,
so every routing rule here is directly testable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# A day's ceiling on diligence calls, both tiers combined.
DAILY_CALL_BUDGET = max(1, int(os.getenv("CLAUDE_DAILY_CALL_BUDGET", "100")))

# How many of that budget are held back for same-tick answers. Kept small on
# purpose: this is the expensive tier, and it only earns its premium for
# candidates a human would want to see today.
SYNC_RESERVE = max(0, int(os.getenv("CLAUDE_SYNC_RESERVE", "10")))

# The Batch API bills at half the standard rate.
BATCH_DISCOUNT = 0.5


@dataclass(frozen=True)
class Plan:
    """How one attempt intends to spend the remaining daily budget."""

    sync: list[dict] = field(default_factory=list)
    batched: list[dict] = field(default_factory=list)
    deferred: list[dict] = field(default_factory=list)
    budget_remaining: int = 0
    note: str = ""

    @property
    def total_calls(self) -> int:
        return len(self.sync) + len(self.batched)

    @property
    def cost_multiple(self) -> float:
        """Spend relative to running the same calls synchronously, 0.0-1.0.

        1.0 means no saving (everything went synchronous); 0.5 is the floor
        (everything batched). Reported rather than assumed so the operator can
        see what the split actually bought on a given day.
        """
        if not self.total_calls:
            return 0.0
        billed = len(self.sync) + len(self.batched) * BATCH_DISCOUNT
        return billed / self.total_calls

    def to_dict(self) -> dict[str, object]:
        return {
            "sync": len(self.sync),
            "batched": len(self.batched),
            "deferred": len(self.deferred),
            "total_calls": self.total_calls,
            "budget_remaining": self.budget_remaining,
            "cost_multiple": round(self.cost_multiple, 4),
            "note": self.note,
        }


def _score(row: dict, score_key: str) -> float:
    try:
        return float(row.get(score_key, 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def plan_calls(
    rows: list[dict],
    *,
    calls_used_today: int,
    daily_budget: int = DAILY_CALL_BUDGET,
    sync_reserve: int = SYNC_RESERVE,
    exceptional_score: float = 90.0,
    score_key: str = "priority_score",
) -> Plan:
    """Route ``rows`` into the synchronous tier, the batch tier, or tomorrow.

    ``rows`` need not be sorted; they are ranked by ``score_key`` here so the
    budget always goes to the best candidates first regardless of the order
    selection handed them over in.

    Only candidates at or above ``exceptional_score`` are eligible for the
    synchronous tier. An unused reserve is **not** wasted: if fewer than
    ``sync_reserve`` candidates clear that bar, the leftover goes to the batch
    tier rather than shrinking the day's diligence. Nothing is promoted into
    the expensive tier to fill a quota.
    """
    remaining = max(0, daily_budget - max(0, calls_used_today))
    if remaining <= 0:
        return Plan(
            deferred=list(rows),
            budget_remaining=0,
            note="Daily call budget already spent; nothing dispatched.",
        )

    ranked = sorted(rows, key=lambda r: _score(r, score_key), reverse=True)

    sync: list[dict] = []
    if sync_reserve > 0:
        for row in ranked:
            if len(sync) >= min(sync_reserve, remaining):
                break
            if _score(row, score_key) >= exceptional_score:
                sync.append(row)

    dispatched_ids = {id(row) for row in sync}
    rest = [row for row in ranked if id(row) not in dispatched_ids]

    batch_slots = max(0, remaining - len(sync))
    batched = rest[:batch_slots]
    deferred = rest[batch_slots:]

    if not sync and batched:
        note = (
            f"No candidate scored {exceptional_score:g}+; the whole day went "
            "to the half-price batch tier."
        )
    elif deferred:
        note = (
            f"{len(deferred)} candidate(s) held for tomorrow: the "
            f"{daily_budget}/day cap was reached."
        )
    else:
        note = "Every eligible candidate fits inside today's budget."

    return Plan(
        sync=sync,
        batched=batched,
        deferred=deferred,
        budget_remaining=max(0, remaining - len(sync) - len(batched)),
        note=note,
    )


def describe_savings(plan: Plan) -> str:
    """One line an operator can read in a log without doing arithmetic."""
    if not plan.total_calls:
        return "No diligence calls dispatched."
    saved = (1.0 - plan.cost_multiple) * 100
    return (
        f"{plan.total_calls} verdict(s) for {plan.cost_multiple:.0%} of "
        f"synchronous cost ({saved:.0f}% saved): "
        f"{len(plan.sync)} immediate, {len(plan.batched)} batched."
    )
