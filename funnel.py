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
    """One step of the filter: what arrived, what it removed, what went on.

    IN and OUT only. There is deliberately no "waiting" column -- a domain
    that has not been reached yet simply leaves the funnel at that step, the
    same as one that was rejected, because the question this view answers is
    "what shaved the population and by how much", not "what might come back".
    Splitting the two put a third number in every row that most readers had
    to mentally add back before the arithmetic closed.
    """

    key: str
    label: str
    entered: int
    dropped: int = 0
    note: str = ""

    @property
    def survived(self) -> int:
        """Never negative: the counters behind a live funnel are read across
        several statements and can disagree by a few rows mid-write."""
        return max(0, self.entered - self.dropped)

    @property
    def drop_rate(self) -> float:
        """Share of what entered this step that this step removed."""
        return self.dropped / self.entered if self.entered else 0.0

    def to_dict(self, *, top_line: int) -> dict[str, object]:
        return {
            "key": self.key,
            "label": self.label,
            "entered": self.entered,
            "dropped": self.dropped,
            "survived": self.survived,
            "drop_rate": round(self.drop_rate, 6),
            # What this one step shaved off the original population -- the
            # number that says which steps actually matter. A step removing
            # 90% of a population that is already 0.1% of the top line is
            # doing almost nothing to the funnel.
            "shave_of_top": round(
                self.dropped / top_line if top_line else 0.0, 6
            ),
            "share_of_top": round(
                self.survived / top_line if top_line else 0.0, 6
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
        top = self.entered
        return {
            "name": self.name,
            "entered": top,
            "survived": self.survived,
            "overall_survival_rate": round(
                self.survived / top if top else 0.0, 6
            ),
            "note": self.note,
            "stages": [stage.to_dict(top_line=top) for stage in self.stages],
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


def build_funnel(
    name: str,
    top_line: int,
    steps: list[tuple[str, str, int, str]],
    *,
    note: str = "",
) -> Funnel:
    """Chain ``steps`` from ``top_line`` down, each seeing the last's survivors.

    ``steps`` is ``(key, label, dropped, note)`` in the order the filter
    actually applies them. Generic on purpose: the screening script is a long
    sequence of guards and the page should render however many there are,
    rather than a hand-maintained six that drifts out of step with the code.
    """
    stages: list[Stage] = []
    remaining = top_line
    for key, label, dropped, step_note in steps:
        removed = max(0, min(int(dropped or 0), remaining))
        stages.append(Stage(
            key=key, label=label, entered=remaining,
            dropped=removed, note=step_note,
        ))
        remaining = max(0, remaining - removed)
    return Funnel(name=name, note=note, stages=stages)


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


# The screening guards in the order route_company applies them. Each one only
# ever sees what the previous passed, which is what makes this a funnel rather
# than a set of independent tallies. Reasons not listed here are appended in
# whatever order they arrive, so a new guard shows up on the page without
# anyone editing this list -- it just sorts last until it is placed.
SCREENING_ORDER = (
    ("parked_or_for_sale", "Parked or for sale"),
    ("generic_hosting_or_registrar_placeholder", "Registrar placeholder page"),
    ("inaccessible_site", "Site would not load"),
    ("website_never_reachable_after_max_attempts", "Never reachable after retries"),
    ("website_unavailable", "Website unavailable"),
    ("predominantly_non_english", "Predominantly non-English"),
    ("confirmed_non_us", "Confirmed non-US"),
    ("consumer_b2c_product", "Consumer / B2C product"),
    ("agency_or_consultancy", "Agency or consultancy"),
    ("media_newsletter_or_aggregator", "Media, newsletter or aggregator"),
    ("hosting_or_domain_registration_business", "Hosting or domain registrar"),
    ("investor_or_venture_studio", "Investor or venture studio"),
    ("commodity_ai_wrapper_or_service", "Commodity AI wrapper"),
    ("human_ground_truth_no", "Human said no"),
    ("substantive_no_fit_evidence", "Substantive site, no thesis fit"),
    ("no_clear_enterprise_or_glasswing_fit", "No clear enterprise fit"),
)


def order_screening_reasons(counts: dict[str, int]) -> list[tuple[str, str, int]]:
    """(reason, label, count) in guard order, with unknown reasons appended.

    Keeps the page honest when the screening script grows a guard: it appears
    at the end rather than being silently dropped from the funnel, which is
    how a step can remove domains without anyone being able to see it.
    """
    placed = []
    seen = set()
    for reason, label in SCREENING_ORDER:
        if counts.get(reason):
            placed.append((reason, label, int(counts[reason])))
            seen.add(reason)
    for reason, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        if reason not in seen and count:
            placed.append((reason, reason.replace("_", " ").capitalize(), int(count)))
    return placed
