# Pipeline ontology

Every step a domain passes through, in the order it happens, with the unit
each step counts and where the code lives.

Two rules this document exists to keep honest:

1. **A step is a place where the population can shrink.** If it cannot remove
   anything it is not a step, it is plumbing.
2. **The unit changes exactly once**, at L1. Everything before it counts
   hostnames; everything after counts domains. Mixing the two is what made
   earlier versions of the funnel page irreconcilable — a domain appearing on
   ten certificates was charged ten times on one side of the boundary and once
   on the other.

---

## L0 — Acquisition

| | |
|---|---|
| Unit | hostnames |
| Code | `continuous_pipeline.listener` |
| Source | self-hosted CertStream `domains-only` websocket |

One frame is one certificate, carrying its full SAN list. There is no replay:
the feed is a broadcast, so anything emitted while the listener is away is
gone. That is why the listener is always-on and why a half-open socket is
treated as an outage (`STREAM_IDLE_TIMEOUT_SECONDS`, default 120s).

**Counter:** `hostnames_seen`.

---

## L1 — Reduction (the unit boundary)

| | |
|---|---|
| Unit | hostnames in, **domains out** |
| Code | `certstream_filter.reduce_hostname` |

Each hostname is reduced to its registrable domain against the Public Suffix
List: `_acme-challenge.app.exa.ai` → `exa.ai`. Multi-label suffixes are handled
properly, so `foo.com.ai` stays `foo.com.ai` rather than collapsing to the bare
suffix `com.ai`.

Hostnames that cannot become a domain at all — malformed, over the 253-char DNS
limit, or a bare public suffix — leave here as **unreducible**. They are not
part of the domain population and so are not part of any later percentage.

**Counters:** `domains_observed` (the funnel's top line), `hostnames_filtered`.

---

## L2 — Intake guards

| | |
|---|---|
| Unit | domains |
| Code | `certstream_filter.guard_registrable` |
| Order | `certstream_filter.DROP_REASONS` |

Applied **once per distinct domain**, in this order — each guard only sees what
the previous passed:

| # | Guard | Removes |
|---|---|---|
| 1 | `empty` | nothing left after normalisation |
| 2 | `too_long` | over the DNS FQDN limit |
| 3 | `no_registrable_label` | bare public suffix (`com.ai`) |
| 4 | `extension_not_tracked` | TLD outside the operator's selected set |
| 5 | `motif_spam` | a short unit repeated 3+ times (`aiaiai`) |
| 6 | `repeated_run` | one character repeated (`aaa`) |
| 7 | `machine_generated` | leading-zero counters, long digit runs, unpronounceable strings |
| 8 | `templated_bulk` | generator templates (`auth-8510-system`) |

**In practice step 4 dominates.** With two TLDs selected it removes ~99% of the
feed in one go. Everything below it operates on the ~1% remainder, which is why
their individual drop rates look large while their share of the top line is
near zero.

Guards 5–8 are deliberately conservative: dropping at intake is irreversible,
and each threshold was set by requiring **zero losses** against the domains the
pipeline had already judged real candidates. That test rejected three earlier
versions.

---

## L3 — Scoring

| | |
|---|---|
| Unit | domains (removes nothing) |
| Code | `intake_score.read_certificate` |

Not a filter — it ranks. Arrival exceeds screening capacity, so *which* domains
get fetched is decided here rather than by arrival time.

Score comes from the certificate itself: TLD prior, subdomain structure
(`app.`/`api.`/`dashboard.` means somebody shipped something), SAN breadth,
wildcard presence. A shared certificate carrying more than
`INTAKE_MAX_APEXES_PER_CERT` distinct apexes is a hosting artifact and scores to
the floor (`-50`), below even an unscored row — never fetched in practice, but
still auditable.

**Scores only move up, and are re-applied on every certificate.** A bare
registration scores low and waits; when its founders ship, the certificate that
provisions their app subdomains re-scores it and it climbs the queue by itself.
This is why there is no revisit scheduler.

**Columns:** `intake_score`, `intake_evidence`.

---

## L4 — Queue

| | |
|---|---|
| Unit | domains |
| Code | `continuous_pipeline.register_domain_event`, `claim_batch` |

`INSERT … ON CONFLICT (domain) DO NOTHING`. A domain already present is a
**repeat** — a renewal or another SAN — and increments `seen_count` instead of
creating a row. Repeats are the largest single loss between L2 and L5.

Claim order is `COALESCE(intake_score, -1) DESC, first_seen DESC`. Score first,
recency only as the tiebreak.

---

## L5 — Fetch

| | |
|---|---|
| Unit | domains |
| Code | `continuous_pipeline` screening loop, `filter.py` fetch path |

Concurrency is a closed loop on the previous batch's fetch-failure rate and RSS
(`screening_capacity.next_worker_count`), not a constant.

Failures retry up to `STREAM_MAX_ATTEMPTS` (3) with exponential backoff, then
the row is terminal. A domain that reaches a terminal state without producing a
verdict leaves the funnel as **no verdict yet** — it consumed a crawl and
produced nothing.

---

## L6 — Screening guards

| | |
|---|---|
| Unit | domains |
| Code | `filter.route_company` |
| Order | `funnel.SCREENING_ORDER` |

The long chain, in the order `route_company` applies it. Bucket in brackets.

| # | Reason | Bucket |
|---|---|---|
| 1 | `parked_or_for_sale` | reject |
| 2 | `generic_hosting_or_registrar_placeholder` | reject |
| 3 | `inaccessible_site` | ghost |
| 4 | `inaccessible_http_<status>` | ghost |
| 5 | `predominantly_non_english` | reject |
| 6 | `confirmed_non_us` | reject |
| 7 | `consumer_b2c_product` | reject |
| 8 | `agency_or_consultancy` | reject |
| 9 | `media_newsletter_or_aggregator` | reject |
| 10 | `hosting_or_domain_registration_business` | reject |
| 11 | `investor_or_venture_studio` | reject |
| 12 | `commodity_ai_wrapper_or_service` | reject |
| 13 | `verified_prelaunch_watch` | review |
| 14 | `trusted_high_conviction_product_fit` | review |
| 15 | `substantive_site_scored_for_human_review` | review |
| 16 | `substantive_site_no_glasswing_fit_signals` | screened out |
| 17 | `venture_quality_investable` | review |
| 18 | `strong_investable_candidate` | review |
| 19 | `credible_venture_candidate_missing_one_dimension` | review |
| 20 | `thesis_fit_missing_wedge_or_moat` | review |
| 21 | `insufficient_venture_quality_evidence` | screened out |

Steps 1–12 are exclusions and 13–21 are routing decisions. A reason with no
assigned position is appended to the funnel rather than dropped, so a new guard
appears on the page instead of removing domains invisibly.

**The verdict of record is `public.classifications.routing_reason`**, not the
queue's `classification` column. The queue column is a best-effort copy written
at `mark_success` time and is frequently empty; a funnel built on it counts
those rows as having survived everything.

---

## L7 — Candidate pool

| | |
|---|---|
| Unit | domains |
| Buckets | `review`, `priority`, `comp` |

What a human or Claude would look at. Everything else (`ghost`,
`insufficient`, `coming_soon`, `reject`) is terminal.

---

## L8 — Claude diligence

| | |
|---|---|
| Unit | domains |
| Code | `claude_budget.plan_calls`, `claude_batch`, `claude_classifier` |
| Budget | 100 calls/day |

Candidates are ranked and split by conviction: the top `CLAUDE_SYNC_RESERVE`
(10) scoring at or above the exceptional threshold go synchronously and are
answered in the same tick; the rest go to the Batch API at half price. 100
verdicts for ~55 calls' worth of spend.

Evidence is the stored page text — the full 12,000-character excerpt the
crawler already captured. There is no web search: it cost more per call than
the model tokens and rediscovered a page already on the row.

Outcome is `KEEP` or `REJECT`. There is no maybe.

---

## L9 — Human decision

| | |
|---|---|
| Unit | companies |
| Store | `manual_decisions`, `human_ground_truth.csv` |

The only trusted training truth. Python and Claude verdicts are never labels.

---

## Where the population actually goes

Measured, order of magnitude, per run:

```
L0  hostnames on the feed        ~90,000
L1  distinct domains              ~50,000     ← the funnel's top line
L2  survive the intake guards        ~600     ← the TLD gate takes ~99%
L4  new to the queue                 ~150     ← the rest are repeats
L6  reach a verdict                   ~45
L7  candidate pool                     ~5
L8  Claude KEEP                     0–1/day
```

The shape to keep in mind: **one step removes almost everything, and it is a
configuration choice, not a judgement.** Every guard below it is operating on a
population already three orders of magnitude smaller, so their drop rates are
loud and their effect on the funnel is small. Optimising them is worth doing
for precision, not for volume.
