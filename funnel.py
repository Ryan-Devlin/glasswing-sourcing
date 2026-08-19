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
    # The individual reasons that add up to ``dropped``. A stage is a step of
    # the pipeline; a reason is one way of failing it. Showing reasons as
    # peers of stages is what turned a six-step funnel into a 28-row table --
    # "parked", "registrar placeholder" and "would not load" are not three
    # steps, they are three ways of failing one.
    reasons: tuple[tuple[str, str, int], ...] = ()

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
            "reasons": [
                {
                    "key": key,
                    "label": label,
                    "dropped": count,
                    # Against what entered the stage, so the reasons inside
                    # one stage sum to that stage's own drop rate.
                    "share_of_stage": round(
                        count / self.entered if self.entered else 0.0, 6
                    ),
                }
                for key, label, count in self.reasons
            ],
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
    top_line = funnel.entered
    return [
        {
            "key": s.key,
            "label": s.label,
            "dropped": s.dropped,
            "drop_rate": round(s.drop_rate, 6),
            # Against the top line, not against what reached this step: the
            # caller is ranking stages by how much of the whole population
            # each one removed, so it needs the same denominator throughout.
            "shave_of_top": round(
                s.dropped / top_line if top_line else 0.0, 6
            ),
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


def build_zone_funnel(details: dict) -> Funnel:
    """The zone-file crawl as one funnel, in domains, for the active queue.

    The top line is the day's registry delta -- the new registrations CZDS
    published -- not the whole zone. Gibberish rejection is deliberately left
    out: it is scored against the full 162M-name snapshot at import time, so
    folding it in would mix two populations and stop the top line matching the
    domains this queue actually received.

    ``details`` is :func:`zone_universe.database.diagnostics` output.
    """
    queued = int(details.get("queued_domains", 0) or 0)
    quarantined = int(details.get("low_priority_quarantined", 0) or 0)
    attempted = int(details.get("attempted", 0) or 0)

    steps: list[tuple[str, str, int, str]] = [
        (
            "already_crawled",
            "Already crawled — not a new name",
            int(details.get("skipped_existing", 0) or 0),
            "Seen and successfully fetched under an earlier queue version.",
        ),
        (
            "low_priority",
            "Below the priority ceiling — never fetched",
            quarantined,
            "Parseable, but no thesis keyword and not short or brandable, so "
            "it stays in the zone queue rather than spending a request. "
            "Recoverable: raising ZONE_UNIVERSE_MAX_CRAWL_PRIORITY to 3 puts "
            "these back in play.",
        ),
        (
            "not_reached",
            "Not reached this cycle",
            max(0, queued - quarantined - attempted),
            "Left in the queue when the cycle ended. Picked up next run.",
        ),
        (
            "unreachable",
            "Could not be reached",
            int(details.get("network_failed", 0) or 0),
            "DNS failure, timeout, block, HTTP error, or an empty response.",
        ),
        (
            "preflight_rejected",
            "Homepage read, not thesis-relevant",
            int(details.get("preflight_rejected", 0) or 0),
            "The site answered and was parsed, but the copy showed no sign of "
            "an operating technology company.",
        ),
    ]

    # Derived from the chain rather than from preflight_promising on purpose.
    # The two counters are written by different statements and drift by a few
    # rows mid-cycle -- measured 2026-08-19, promising ran 50 ahead of the
    # attempted total it is drawn from. Subtracting a drifting counter from a
    # chained one makes the funnel miss its own bottom line, so the last step
    # is whatever the chain has left over the pages actually stored.
    remaining = queued
    for _, _, dropped, _ in steps:
        remaining = max(0, remaining - max(0, int(dropped or 0)))
    steps.append((
        "no_page_stored",
        "Passed screening, no page stored",
        max(0, remaining - int(details.get("network_success", 0) or 0)),
        "Cleared the quick screen but the full crawl produced nothing.",
    ))

    return Funnel(
        name="zone",
        note=(
            "One zone-file cycle, counted in domains at every step. The top is "
            "every new domain in the day's registry delta, before any "
            "judgement."
        ),
        stages=build_funnel("zone", queued, steps).stages,
    )


# The pipeline as six steps, which is how it actually reads: find domains,
# drop the junk, drop what never served a usable page, drop what served one
# and did not fit, bucket the rest, then spend Claude on the top of that.
#
# Every screening reason belongs to exactly one step. Reasons not listed fall
# into FIT, because a new guard in route_company is far more likely to be a
# judgement about a site that loaded than a new way of failing to load.
REACHABILITY_REASONS = (
    "parked_or_for_sale",
    "generic_hosting_or_registrar_placeholder",
    "inaccessible_site",
    "website_never_reachable_after_max_attempts",
    "website_unavailable",
)


def _split_screening(counts: dict[str, int]) -> tuple[list, list]:
    """Screening reasons into (never served a page, served one and did not fit)."""
    labels = dict(SCREENING_ORDER)
    unreachable: list[tuple[str, str, int]] = []
    unfit: list[tuple[str, str, int]] = []
    for key, label, count in order_screening_reasons(counts):
        row = (key, labels.get(key, label), int(count or 0))
        (unreachable if key in REACHABILITY_REASONS else unfit).append(row)
    return unreachable, unfit


def build_daily_funnel(
    *,
    sources: dict[str, int],
    junk_dropped: int,
    junk_reasons: list[tuple[str, str, int]],
    screening_counts: dict[str, int],
    unscreened: int,
    candidates: int,
    sent_to_claude: int,
    claude_rejected: int,
    extra_no_page: list[tuple[str, str, int]] | None = None,
    extra_no_fit: list[tuple[str, str, int]] | None = None,
    note: str = "",
) -> Funnel:
    """One funnel for one day, both discovery sources, six steps.

    ``sources`` maps a discovery source to how many domains it found today.
    They are summed into the top line rather than shown as separate funnels:
    below intake the two share every step -- the same route_company, the same
    buckets, the same Claude budget -- so splitting them duplicates fourteen
    identical rows to distinguish two numbers that only differ at the top.
    """
    top_line = sum(max(0, int(n or 0)) for n in sources.values())
    unreachable, unfit = _split_screening(screening_counts)
    # The zone crawler decides reachability and relevance itself, before a
    # domain ever reaches route_company, so its outcomes never appear in
    # routing_reason. Without folding them in here they would land in "not
    # screened yet" -- reported as still in flight when they were in fact
    # rejected hours earlier.
    unreachable += list(extra_no_page or [])
    unfit += list(extra_no_fit or [])

    def total(rows: list[tuple[str, str, int]]) -> int:
        return sum(count for _, _, count in rows)

    plan: list[tuple[str, str, int, str, tuple]] = [
        (
            "junk",
            "Junk names and untracked extensions",
            junk_dropped,
            "Machine-generated or templated names, and extensions outside the "
            "tracked set. Refused on the name alone, before any request.",
            tuple(junk_reasons),
        ),
        (
            "unscreened",
            "Not screened yet",
            unscreened,
            "Queued today but no verdict yet. Not a rejection -- these can "
            "still come back as candidates.",
            (),
        ),
        (
            "no_page",
            "Never served a usable page",
            total(unreachable),
            "Parked, for sale, a registrar placeholder, or the site never "
            "loaded. Nothing to judge.",
            tuple(unreachable),
        ),
        (
            "no_fit",
            "Read the site — not a fit",
            total(unfit),
            "The site loaded and was read. These are judgements about the "
            "company, not about the page.",
            tuple(unfit),
        ),
    ]

    stages: list[Stage] = []
    remaining = top_line
    for key, label, dropped, step_note, reasons in plan:
        removed = max(0, min(int(dropped or 0), remaining))
        stages.append(Stage(
            key=key, label=label, entered=remaining, dropped=removed,
            note=step_note, reasons=reasons,
        ))
        remaining = max(0, remaining - removed)

    # The candidate pool is what survived screening. Claude is a budget
    # applied to it, so "not sent" is a step like any other -- it is the
    # single largest thing standing between a candidate and a verdict.
    # From the chain, not from the measured candidate count. The two are read
    # in separate statements and disagree by a few rows mid-write; taking the
    # measured one here makes the funnel skip rows it already accounted for.
    pool = remaining
    stages.append(Stage(
        key="not_diligenced",
        label="Candidate, not sent to Claude",
        entered=pool,
        dropped=max(0, pool - int(sent_to_claude or 0)),
        note="Reached the three buckets but the daily Claude budget did not "
             "reach them.",
    ))
    reviewed = stages[-1].survived
    stages.append(Stage(
        key="claude_reject",
        label="Claude said REJECT",
        entered=reviewed,
        dropped=max(0, min(int(claude_rejected or 0), reviewed)),
    ))

    return Funnel(name="pipeline", note=note, stages=stages)


def partition_feed(
    *,
    observed: int,
    junk_dropped: int,
    queued: int,
    unscreened: int,
    no_verdict: int,
    candidates: int,
) -> dict[str, int]:
    """Split what the feed showed us into parts that add back up to it.

    The CertStream tiles did not sum to their own top line, and could not:
    "queued after filtering" and "screening complete" are subtotals of the
    rows below them, not siblings, so adding the row of boxes double-counted
    every screened domain -- while the junk the guards removed, by far the
    largest share, had no box at all.

    Every value returned is one disjoint piece of ``observed``:

        observed = junk + repeats + unscreened + no_verdict
                   + candidates + filtered_out

    Each piece is clamped to what is left after the pieces before it, and the
    last one is whatever remains rather than its own measurement. The counts
    come from a live queue and a live classification store read in separate
    statements; taking each at face value makes the row miss its own total by
    however much they disagree at that instant.
    """
    total = max(0, int(observed or 0))
    remaining = total

    def take(value: int) -> int:
        nonlocal remaining
        part = max(0, min(int(value or 0), remaining))
        remaining -= part
        return part

    junk = take(junk_dropped)
    # Repeats are the residual of intake, not a counter: a domain already on
    # the queue is never written again, so nothing tallies it directly.
    queued_part = max(0, min(int(queued or 0), remaining))
    repeats = remaining - queued_part
    remaining = queued_part

    still_processing = take(unscreened)
    without_verdict = take(no_verdict)
    retained = take(candidates)
    filtered_out = remaining

    return {
        "observed": total,
        "junk": junk,
        "repeats": repeats,
        "still_processing": still_processing,
        "no_verdict": without_verdict,
        "candidates": retained,
        "filtered_out": filtered_out,
    }


FEED_PARTS = (
    ("junk", "Junk name or untracked extension"),
    ("repeats", "Already seen before"),
    ("still_processing", "Still screening"),
    ("no_verdict", "Finished with no verdict"),
    ("filtered_out", "Screened out"),
    ("candidates", "Candidates retained"),
)
