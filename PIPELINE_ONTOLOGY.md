# Where domains are removed

Every point in the pipeline that can shrink the population, in the order it
happens. Nothing else is listed — if a step cannot remove a domain it is not
here.

**27 removal points.** Read down: each one only ever sees what the ones above
it passed.

Two facts that govern the whole table:

- **The unit changes exactly once, at step 1–3.** Above that line the pipeline
  counts hostnames; below it, domains. A domain appearing on ten certificates
  is one domain, not ten.
- **The verdict of record is `public.classifications.routing_reason`**, not the
  queue's `classification` column. That column is a best-effort copy and is
  frequently empty; anything built on it counts unjudged rows as survivors.

---

## Reduction — hostname never becomes a domain

`certstream_filter.reduce_hostname` · counts hostnames

| # | Removal | What goes |
|---|---|---|
| 1 | `empty` | nothing left after normalisation |
| 2 | `too_long` | over the 253-char DNS limit |
| 3 | `no_registrable_label` | a bare public suffix — `com.ai` is not a company |

Survivors are **distinct registrable domains**. This count is the funnel's top
line, and every percentage below is measured against it.

---

## Intake guards — domain refused before it is ever queued

`certstream_filter.guard_registrable` · counts domains · irreversible

| # | Removal | What goes |
|---|---|---|
| 4 | `extension_not_tracked` | TLD outside the operator's selected set |
| 5 | `motif_spam` | a short unit repeated 3+ times (`aiaiai`) |
| 6 | `repeated_run` | one character repeated (`aaa`, `bb`) |
| 7 | `machine_generated` | leading-zero counters, long digit runs, unpronounceable strings |
| 8 | `templated_bulk` | generator templates (`auth-8510-system`) |

**Step 4 removes roughly 99% of everything.** It is a configuration choice, not
a judgement. Steps 5–8 operate on the ~1% remainder, so their drop rates read
large while their effect on the funnel is near zero.

Because these drops are permanent, every threshold in 5–8 was set by requiring
**zero losses** against domains already judged real candidates. That test
rejected three earlier versions.

---

## Ranking — not fetched in practice

`intake_score.read_certificate` · counts domains · reversible

| # | Removal | What goes |
|---|---|---|
| 9 | `shared_cert` | a certificate carrying more distinct apexes than a company would hold — a hosting artifact |

The only soft removal in the table. These score to the floor (`-50`, below even
an unscored row) rather than being refused, so they are never fetched while the
row survives and the rule stays auditable. It is the one guard that could not be
safety-tested, because certificate composition was never stored.

---

## Queue — already known

`continuous_pipeline.register_domain_event` · counts domains

| # | Removal | What goes |
|---|---|---|
| 10 | `already_queued` | a renewal or another SAN for a domain the queue already holds |

Not waste — it is the same company being seen again. After step 4 this is the
largest single loss.

---

## Fetch — no verdict produced

`continuous_pipeline` screening loop · counts domains

| # | Removal | What goes |
|---|---|---|
| 11 | `no_verdict` | terminal after 3 attempts with nothing classified |

A domain that consumed a crawl and produced nothing. Distinct from *ghost*
below: ghost means the site answered and was empty; this means screening never
reached a conclusion at all.

---

## Screening guards — judged and discarded

`filter.route_company` · counts domains · in the order applied

| # | Removal | Bucket | What goes |
|---|---|---|---|
| 12 | `parked_or_for_sale` | reject | domain for sale, no company |
| 13 | `generic_hosting_or_registrar_placeholder` | reject | registrar default page |
| 14 | `inaccessible_site` | ghost | nothing served |
| 15 | `inaccessible_http_<status>` | ghost | 4xx/5xx |
| 16 | `predominantly_non_english` | reject | out of scope by language |
| 17 | `confirmed_non_us` | reject | states it is from, or sells solely to, elsewhere |
| 18 | `consumer_b2c_product` | reject | not enterprise |
| 19 | `agency_or_consultancy` | reject | services, not product |
| 20 | `media_newsletter_or_aggregator` | reject | publisher, not company |
| 21 | `hosting_or_domain_registration_business` | reject | infrastructure reseller |
| 22 | `investor_or_venture_studio` | reject | invests, does not build |
| 23 | `commodity_ai_wrapper_or_service` | reject | thin wrapper, no depth |
| 24 | `substantive_site_no_glasswing_fit_signals` | screened out | real company, wrong thesis |
| 25 | `insufficient_venture_quality_evidence` | screened out | too little evidence to judge |

Seven other `route_company` outcomes exist and are **not** here: they route a
domain onward to review rather than removing it.

A reason with no assigned position in `funnel.SCREENING_ORDER` is appended to
the funnel rather than dropped — a new guard shows up on the page instead of
removing domains invisibly.

---

## Diligence — Claude says no

`claude_classifier` · counts domains · 100 calls/day

| # | Removal | What goes |
|---|---|---|
| 26 | `claude_reject` | reviewed against the rubric and rejected |

There is no maybe. Candidates that have not been called yet are not removed —
they are waiting, and the funnel shows that as its own line.

---

## Human — the only trusted truth

`manual_decisions` · counts companies

| # | Removal | What goes |
|---|---|---|
| 27 | `human_ground_truth_no` | a person said no |

The only removal that is training truth. Python and Claude verdicts never are.

---

## Magnitude

Measured, order of magnitude, one run:

```
        distinct domains observed          ~50,000
  4  −  extension_not_tracked               ~49,400   ← 99%
5-8  −  junk name guards                        ~10
 10  −  already queued                         ~450
 11  −  no verdict                              ~100
12-25 − screening guards                         ~40
        candidate pool                            ~5
 26  −  Claude REJECT                        0–1/day
```

One step removes almost everything, and it is a setting. The other 26 share the
remaining 1%. Tuning them buys precision; only step 4 buys volume.
