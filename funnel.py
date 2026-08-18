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


def filter_funnel(hostnames_seen: int, drops: dict[str, int]) -> Funnel:
    """What each guard removed, in the order the guards actually run.

    Each guard only ever sees what the previous ones passed, so this is a real
    funnel rather than a set of overlapping tallies -- ``motif_spam`` counts
    only hostnames that already survived the TLD check, and so on. Reporting
    it as one "filtered" number made an untracked TLD indistinguishable from a
    bulk-registration farm, which are a configuration choice and an attack on
    the certificate log respectively.
    """
    from certstream_filter import DROP_REASONS

    labels = {
        "empty": "Empty or unparseable hostname",
        "too_long": "Over the DNS length limit",
        "no_registrable_label": "Bare public suffix, nothing registrable",
        "extension_not_tracked": "TLD not in the tracked list",
        "motif_spam": "Motif spam (aiaiai, mememe)",
        "repeated_run": "Repeated-letter run (aaa, bb)",
        "machine_generated": "Machine-generated name (000091fj, x7k2mqp9)",
        "templated_bulk": "Templated bulk farm (auth-8510-system)",
    }
    stages: list[Stage] = []
    remaining = hostnames_seen
    for reason in DROP_REASONS:
        removed = int(drops.get(reason, 0) or 0)
        stages.append(Stage(
            key=reason,
            label=labels.get(reason, reason),
            entered=remaining,
            dropped=removed,
        ))
        remaining = max(0, remaining - removed)
    return Funnel(
        name="filter",
        note="Guards in evaluation order; each sees only what the previous passed.",
        stages=stages,
    )


def intake_funnel(
    *,
    hostnames_seen: int,
    hostnames_filtered: int,
    new_domains: int,
    repeat_domains: int,
) -> Funnel:
    """CertStream hostnames to newly-queued domains.

    ``hostnames_filtered`` is what ``certstream_filter.to_registrable`` refused
    (wrong TLD, bare public suffix, junk). The survivors then split into rows
    the queue had never seen (``new_domains``) and re-observations of a domain
    already queued (``repeat_domains``) — a repeat is not waste, it is a
    certificate renewal or another SAN entry for a domain already in hand.
    """
    passed_filter = max(0, hostnames_seen - hostnames_filtered)
    return Funnel(
        name="intake",
        note=(
            "Counted in hostnames, not domains: one certificate can carry many "
            "hostnames that reduce to a single registrable domain."
        ),
        stages=[
            Stage(
                key="observed",
                label="Hostnames seen on the feed",
                entered=hostnames_seen,
                dropped=hostnames_filtered,
                note="Dropped: not a tracked TLD, a bare public suffix, or junk.",
            ),
            Stage(
                key="registrable",
                label="Reduced to a tracked domain",
                entered=passed_filter,
                dropped=max(0, passed_filter - new_domains - repeat_domains),
                note="Dropped: collapsed into another hostname from the same certificate.",
            ),
            Stage(
                key="deduplicated",
                label="Not already on the queue",
                entered=new_domains + repeat_domains,
                dropped=repeat_domains,
                note="Dropped: re-observation of a domain already queued (renewal or extra SAN).",
            ),
        ],
    )


def screening_funnel(
    *,
    queued: int,
    unscreened: int,
    screened: int,
    classified: int,
    ghost: int,
    insufficient: int,
    rejected: int,
    candidates: int,
    claude_reviewed: int,
    claude_kept: int,
) -> Funnel:
    """Queue rows to companies worth a human's time.

    Each stage's ``entered`` is the previous stage's ``survived``, so the chain
    is arithmetically closed and the final survivor count is reproducible from
    the inputs.
    """
    verdict_dropped = max(0, screened - classified)
    reachable = max(0, classified - ghost)
    substantive = max(0, reachable - insufficient)
    passed_rules = max(0, substantive - rejected)

    return Funnel(
        name="screening",
        note="Counted in domains, one row per registrable domain on the queue.",
        stages=[
            Stage(
                key="queued",
                label="On the queue",
                entered=queued,
                pending=unscreened,
                note="Pending: never fetched yet. Backlog, not a rejection.",
            ),
            Stage(
                key="screened",
                label="Fetched and screened",
                entered=screened,
                dropped=verdict_dropped,
                note="Dropped: fetch or scrape failed, so no verdict was reached.",
            ),
            Stage(
                key="reachable",
                label="Site actually resolved",
                entered=classified,
                dropped=ghost,
                note="Dropped: ghost — nothing served at the domain.",
            ),
            Stage(
                key="substantive",
                label="Site had real content",
                entered=reachable,
                dropped=insufficient,
                note="Dropped: insufficient — parked, placeholder, or too thin to judge.",
            ),
            Stage(
                key="passed_rules",
                label="Survived the Python rules",
                entered=substantive,
                dropped=rejected,
                note="Dropped: hard-denied by a classifier rule or a human decision.",
            ),
            Stage(
                key="candidate",
                label="Reached the candidate pool",
                entered=passed_rules,
                dropped=max(0, passed_rules - candidates),
                note="Dropped: scored but not routed to review, priority, or competitor.",
            ),
            Stage(
                key="claude_reviewed",
                label="Claude ran diligence",
                entered=candidates,
                pending=max(0, candidates - claude_reviewed),
                note="Pending: eligible for diligence but not yet called. Backlog, not a rejection.",
            ),
            Stage(
                key="claude_kept",
                label="Claude said KEEP",
                entered=claude_reviewed,
                dropped=max(0, claude_reviewed - claude_kept),
                note="Dropped: Claude reviewed the evidence and said REJECT.",
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
