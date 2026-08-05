# Code mental model

## The shortest useful model

Think of the application as a conveyor belt with two separate memory systems:

```text
CertStream
    ↓
SQLite domain queue
    ↓
Website fetch + deterministic scoring
    ↓
CSV screening history
    ↓
Optional Claude diligence
    ↓
Human/Claude training labels
    ↓
Trusted and weak model-training exports
```

SQLite remembers **what work has been attempted**. CSV files remember **what
was learned**. The dashboard reads those saved results and provides controls;
it is not the primary database.

## Module ownership

### `collector.py` — domain normalization

Takes CertStream messages, extracts hostnames, reduces them to registrable
domains, and limits them to supported extensions. It does not scrape or score.

### `continuous_pipeline.py` — orchestration

This is the long-running service. It owns:

- the CertStream WebSocket connection at `ws://localhost:8080/domains-only`;
- persistent deduplication and work state in `data/pipeline_queue.sqlite3`;
- claiming small batches from SQLite;
- calling `filter.screen_records`;
- rate-limited automatic Claude handoffs;
- runtime status in `logs/continuous_status.json`.

Its two concurrent loops are conceptually:

```text
listener loop → register new certificate domains in SQLite
worker loop   → claim queued domains, screen them, mark them complete
```

A website receives an initial fetch and one immediate slower retry. A final
failure is stored as unreachable; it does not enter a recurring retry loop.

### `filter.py` — evidence extraction and objective classification

This is the deterministic screening engine:

1. Fetch apex URL first, then fall back only when needed.
2. Parse title, description, visible text, and relevant internal links.
3. Detect positive Glasswing signals and explicit disqualifiers.
4. Calculate the 0–100 objective score.
5. Apply eligibility caps such as consumer, agency, non-U.S., mature, parked,
   or non-English.
6. Route to an internal bucket.
7. Persist batch results into the master CSV files.

The score answers: **“How much website evidence supports Glasswing fit?”**
The bucket answers: **“What should happen operationally?”** They are related
but not interchangeable.

### `claude_classifier.py` — selective external diligence

Claude is an expensive second opinion, not the primary scraper. Only eligible
high-scoring companies are sent. One lean call verifies location, funding,
stage, ownership, maturity, and enterprise fit.

- `KEEP` becomes a Silver Yes label.
- `REJECT` becomes a Silver No label.
- `MAYBE` remains unlabeled.

Hourly, daily, and batch limits live in `data/run_config.json`.

### `dashboard.py` — projection and human controls

Streamlit loads saved screening and Claude CSVs, recalculates the current
visible presentation, and renders ten tabs. It also owns:

- start/stop controls;
- manual classification overrides;
- live Yes/No and score editing;
- the active-learning review queue;
- training readiness and downloads.

Important: **Needs Review is not Yes.** Only an Investable decision, decisive
Claude result, verified portfolio membership, or explicit human label can
create a positive example. The dashboard may recompute how history is shown,
but it does not erase the historical screening rows.

### `common.py` — shared persistence primitives

Contains paths, domain normalization, CSV upserts, basket operations, label
provenance, and atomic JSON writes. If multiple modules need the same storage
rule, it belongs here.

### `training_data.py` — training-set compiler

Joins labels to the newest usable website evidence, creates deterministic
domain-level train/validation/test splits, caps input length, and writes:

- `glasswing_model_ready.csv`: trusted Gold/Silver examples only;
- `glasswing_instruction_tuning.jsonl`: balanced small-model messages;
- `glasswing_weak_labels.csv`: automatic labels kept separate;
- `manifest.json`: counts and readiness gates.

Missing-page examples are excluded from model-ready data so the model does not
learn that network failure means poor investment fit.

## Sources of truth

| Concern | Source of truth |
|---|---|
| Seen-domain deduplication and queue state | `data/pipeline_queue.sqlite3` |
| Complete screening evidence/history | `data/filtered/all_screened.csv` |
| Claude decisions | `data/llm/llm_*_master.csv` |
| Current Yes/No membership and score | `data/manual/yes_basket.csv`, `no_basket.csv` |
| Label trust and reason | `data/manual/training_label_metadata.csv` |
| Manual classification override | `data/manual/domain_overrides.csv` |
| Runtime configuration | `data/run_config.json` |
| Runtime health only | `logs/continuous_status.json` |
| Derived training artifacts | `data/training/` |

Files under `data/training/` are rebuildable outputs. Basket, provenance,
override, screening-history, SQLite, and `.env` files are not disposable.

## Classification states

```text
Investable Candidate
    Strong rule evidence; may become a weak Yes suggestion.

Needs Review
    Plausible but unresolved. Not a training label.

Coming Soon
    Credible prelaunch company with limited current evidence.

Insufficient Evidence
    Either fetched but low signal, or unreachable after the immediate retry.
    Not an investment rejection.

Hard Denied
    Requires a specific factual disqualifier or persistent human No.
```

## Label trust

```text
Gold   = explicit human decision or verified Glasswing portfolio company
Silver = decisive Claude KEEP/REJECT
Weak   = automatic deterministic suggestion
```

The baskets remain convenient two-column tables. The separate provenance
ledger determines whether a row is trusted for default training.

## Safe places to change behavior

- Change thesis vocabulary and scoring weights in `filter.py`.
- Change queue/concurrency/retry policy in `continuous_pipeline.py`.
- Change paid diligence gates and prompt in `claude_classifier.py`.
- Change display and review workflow in `dashboard.py`.
- Change trusted export format and split policy in `training_data.py`.
- Add persistence invariants to `common.py`.

Whenever routing changes, add a regression test under `tests/`. Do not
repair behavior by rewriting historical CSVs.
