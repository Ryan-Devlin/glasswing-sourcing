#!/usr/bin/env python3
"""Stage-by-stage survival and drop rates for the sourcing funnel.

The pipeline throws away roughly 99.2% of what CertStream hands it, but until
now nothing said *where*. This module turns the counts the pipeline already
stores into an ordered chain of stages, each reporting what entered it, what
it dropped, and what it passed on.

Two funnels, deliberately kept separate rather than chained into one number:

- **Intake** — hostnames off the CertStream feed to rows on the queue. Fed by
  the listener's live counters, because raw hostnames are never persisted.
- **Screening** — queue rows to companies a human should look at. Fed by the
  queue and classification stores.

They are not one chain because a dedup boundary sits between them: many
hostnames on the feed reduce to one registrable domain (a multi-SAN cert), and
a domain already on the queue is a repeat rather than a new row. Presenting a
single end-to-end percentage across that boundary would be arithmetic on two
different units, so the API returns both and says so.

**Backlog is not a drop.** Every stage separates ``dropped`` (decided against)
from ``pending`` (not yet reached). Collapsing the two is the specific way this
view would mislead: measured on the corpus, ~70% of queued domains are simply
unscreened, and reading that as a 70% rejection rate would send you tuning
filters that never ran.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Stage:
    """One step of the funnel: what arrived, what was thrown out, what moved on."""

    key: str
    label: str
    entered: int
    dropped: int = 0
    pending: int = 0
    note: str = ""

    @property
    def survived(self) -> int:
        """Rows that passed this stage. Never negative even on inconsistent
        inputs — the two stores are read in separate queries and can be a few
        rows apart mid-write, which must not render a negative count."""
        return max(0, self.entered - self.dropped - self.pending)

    @property
    def decided(self) -> int:
        """Rows this stage actually ruled on (excludes the backlog)."""
        return max(0, self.entered - self.pending)

    @property
    def drop_rate(self) -> float:
        """Share of *decided* rows this stage threw out, 0.0-1.0.

        Deliberately excludes ``pending`` from the denominator: a stage that
        has ruled on 10 of 1,000 rows and dropped 9 has a 90% drop rate, not a
        0.9% one. The backlog is reported on its own so it can't hide in here.
        """
        return self.dropped / self.decided if self.decided else 0.0

    @property
    def pending_rate(self) -> float:
        """Share of everything that entered which is still waiting."""
        return self.pending / self.entered if self.entered else 0.0

    def to_dict(self, *, funnel_entered: int) -> dict[str, object]:
        return {
            "key": self.key,
            "label": self.label,
            "entered": self.entered,
            "dropped": self.dropped,
            "pending": self.pending,
            "survived": self.survived,
            "drop_rate": round(self.drop_rate, 6),
            "pending_rate": round(self.pending_rate, 6),
            # Share of the funnel's own starting population still alive here,
            # so a reader can see cumulative attrition without re-multiplying
            # every stage's survival rate by hand.
            "share_of_start": round(
                self.survived / funnel_entered if funnel_entered else 0.0, 6
            ),
            "note": self.note,
        }


@dataclass(frozen=True)
class Funnel:
    name: str
    stages: list[Stage] = field(default_factory=list)
    note: str = ""

    @property
    def entered(self) -> int:
        return self.stages[0].entered if self.stages else 0

    @property
    def survived(self) -> int:
        return self.stages[-1].survived if self.stages else 0

    def to_dict(self) -> dict[str, object]:
        start = self.entered
        return {
            "name": self.name,
            "entered": start,
            "survived": self.survived,
            "overall_survival_rate": round(
                self.survived / start if start else 0.0, 6
            ),
            "note": self.note,
            "stages": [stage.to_dict(funnel_entered=start) for stage in self.stages],
        }


@dataclass(frozen=True)
class Throughput:
    """Arrival against fetch rate over one window — the capacity question.

    Whether intake outruns screening decides whether ranking is a nicety or
    the only thing that matters, and it is the one number that has to come
    from the live database rather than from a code comment. A stale local
    snapshot answers a different question about a machine nobody is running.
    """

    window_hours: int
    arrived: int
    fetched: int

    @property
    def arrival_per_min(self) -> float:
        return self.arrived / (self.window_hours * 60) if self.window_hours else 0.0

    @property
    def fetch_per_min(self) -> float:
        return self.fetched / (self.window_hours * 60) if self.window_hours else 0.0

    @property
    def overload_ratio(self) -> float:
        """Arrivals per fetch. 1.0 means screening keeps up; 4.0 means three
        of every four domains are never looked at. Zero when nothing was
        fetched, which is a different condition from keeping up and must not
        be reported as an infinite ratio."""
        return self.arrived / self.fetched if self.fetched else 0.0

    @property
    def keeping_up(self) -> bool:
        return self.fetched > 0 and self.arrived <= self.fetched

    def to_dict(self) -> dict[str, object]:
        return {
            "window_hours": self.window_hours,
            "arrived": self.arrived,
            "fetched": self.fetched,
            "arrival_per_min": round(self.arrival_per_min, 2),
            "fetch_per_min": round(self.fetch_per_min, 2),
            "overload_ratio": round(self.overload_ratio, 2),
            "keeping_up": self.keeping_up,
            # Distinguishes "screening is idle" from "screening keeps up".
            # Both show a healthy-looking backlog delta; only one is good.
            "stalled": self.fetched == 0 and self.arrived > 0,
        }


def run_funnel(
    *,
    received: int,
    unscreened: int,
    no_verdict: int,
    ghost: int,
    insufficient: int,
    rejected: int,
    candidates: int,
) -> Funnel:
    """The whole pipeline for one CertStream run, in one unit.

    Every stage counts **domains**, and the first stage is exactly the number
    of domains CertStream handed the queue during this run. Earlier versions
    split this across three tables and changed unit halfway -- hostname guards
    in one, queue rows in another -- so a reader could not reconcile the top of
    one with the bottom of the previous, and a cumulative guard tally against a
    reset hostname count produced drop rates above 100%.

    Hostname-level rejections (wrong TLD, junk names, bulk farms) happen
    *before* a domain exists, so they are not stages here at all. They are
    intake noise, reported separately as a single line rather than pretended
    into this funnel's arithmetic.

    Scoped to one run. A cumulative view cannot answer "is it working right
    now", which is the only thing this is for.
    """
    return Funnel(
        name="run",
        note=(
            "One CertStream run, counted in domains at every stage. The top is "
            "exactly what CertStream added to the queue during this run."
        ),
        stages=[
            Stage(
                key="received",
                label="Domains received from CertStream",
                entered=received,
                pending=unscreened,
                note="Pending: queued but not fetched yet. Backlog, not a rejection.",
            ),
            Stage(
                key="screened",
                label="Fetched and screened",
                entered=max(0, received - unscreened),
                dropped=no_verdict,
                note="Dropped: fetch or scrape failed, so no verdict was reached.",
            ),
            Stage(
                key="resolved",
                label="Site actually resolved",
                entered=max(0, received - unscreened - no_verdict),
                dropped=ghost,
                note="Dropped: ghost — nothing served at the domain.",
            ),
            Stage(
                key="substantive",
                label="Site had real content",
                entered=max(0, received - unscreened - no_verdict - ghost),
                dropped=insufficient,
                note="Dropped: parked, placeholder, or too thin to judge.",
            ),
            Stage(
                key="passed_rules",
                label="Survived the Python rules",
                entered=max(
                    0, received - unscreened - no_verdict - ghost - insufficient
                ),
                dropped=rejected,
                note="Dropped: hard-denied by a classifier rule or a human decision.",
            ),
            Stage(
                key="candidate",
                label="Reached the candidate pool",
                entered=max(
                    0,
                    received - unscreened - no_verdict - ghost - insufficient
                    - rejected,
                ),
                dropped=max(
                    0,
                    received - unscreened - no_verdict - ghost - insufficient
                    - rejected - candidates,
                ),
                note="Dropped: scored but not routed to review, priority, or competitor.",
            ),
        ],
    )


def biggest_losses(funnel: Funnel, limit: int = 3) -> list[dict[str, object]]:
    """The stages discarding the most rows, largest first.

    Ranked by absolute count rather than rate on purpose: a 100% drop rate on
    twelve rows is not where the funnel is actually losing its inventory, and
    ranking by rate would put it at the top every time.
    """
    ranked = sorted(
        (s for s in funnel.stages if s.dropped > 0),
        key=lambda s: s.dropped,
        reverse=True,
    )
    return [
        {
            "key": s.key,
            "label": s.label,
            "dropped": s.dropped,
            "drop_rate": round(s.drop_rate, 6),
            "note": s.note,
        }
        for s in ranked[:limit]
    ]
