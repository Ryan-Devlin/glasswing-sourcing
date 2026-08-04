# Glasswing Sourcing

Continuous CertStream discovery and fast first-pass screening for early-stage,
US-oriented B2B AI companies.

## What this folder requires

- macOS
- Docker Desktop
- Python 3
- An Anthropic API key for optional Claude diligence

## First-time setup

1. Install and open Docker Desktop.
2. Open Terminal and change into this folder.
3. Create `.env` and add the recipient's own Anthropic key:

```bash
touch .env
open -e .env
```

Add this line, save, and close the editor:

```text
ANTHROPIC_API_KEY=the_recipient_own_key
```

4. Start the local CertStream service:

```bash
docker start certstream 2>/dev/null || docker run -d \
  --name certstream \
  -p 8080:8080 \
  0rickyy0/certstream-server-go
```

## Run the app

```bash
chmod +x run_dashboard.sh diagnose.sh
./run_dashboard.sh
```

Open `http://localhost:8501`, choose the extensions and filters, then press
**Start**.

The first launch creates the local Python environment and installs the declared
packages automatically. Later launches use the same environment.

## Sending a clean copy to someone

Do not send your working `.env`, `data/`, or `logs/`. They contain credentials
or Glasswing sourcing history. Send a clean copy of the application code and
this README; each recipient should create their own `.env` and local data.

Keep the original working folder as the source of truth. Never replace or
delete its `.env` or `data/` folder during an update.

## Runtime design

- CertStream events are collapsed to registrable root domains.
- Domains already in the persistent SQLite registry are ignored.
- New domains enter a homepage-first Python screening pass.
- HTTPS root and `www` variants are attempted concurrently.
- HTTP fallbacks are attempted only when HTTPS produces no readable page.
- Results are saved under `data/filtered`.
- Claude review is optional and disabled by default.

## Human investment-training baskets

The result tabs contain an editable **Add or edit training basket labels**
table. Check Yes or No, enter a human rating from 0–100, and save. No company
dropdown is required.

- `data/manual/yes_basket.csv` contains only `domain_url,rating`.
- `data/manual/no_basket.csv` contains only `domain_url,rating`.

The Yes Basket and No Basket tabs support direct score editing, removing rows,
moving companies between baskets, and CSV downloads. A domain can appear in
only one basket. A rating of zero is valid and is useful for obvious No cases.

On dashboard startup, verified company domains from Glasswing's official
portfolio are added to the Yes Basket at 100 as ground-truth positives. The
seed only fills missing labels: it never overwrites a rating or basket choice
you have edited manually. Cards that link only to an acquirer/social profile or
provide no company website are excluded to protect training-data quality.

Python and Claude classifications never become training truth automatically.
Only explicit human actions and verified Glasswing portfolio positives enter
the protected Yes/No training set. A Python Hard Denial removes expendable
crawl payloads when safe, but it does not manufacture a human No label.

Every visible company has a deterministic 0-100 Glasswing-fit score calculated
from its extracted website HTML rather than its dashboard tab. The scorer uses
the page title, metadata, and stored homepage text to measure Glasswing thesis
alignment, enterprise buyers, product specificity, technical and workflow
depth, business value, defensibility, and explicit negative signals. Needs
Review requires a genuine score of at least 55; no display floor rewrites it.

Usable website text always receives a small evidence-coverage score, even when
the company has no Glasswing alignment; zero is reserved for pages with no
usable extracted text. An Investable classification is a review suggestion,
not a training label. Needs Review and every other Python bucket remain
unlabeled until a person makes a decision. Manual moves transfer the displayed
score, write the append-only movement ledger, and update the protected human
training set.

The objective score includes investment eligibility, not only thematic fit.
Predominantly non-English sites are capped at 5; confirmed non-US companies at
10; consumer/media products at 15; agencies at 20; commodity AI services at
25; and mature or disclosed-funded companies at 45. Parked and generic
placeholder pages are capped at 1 and 3. The dashboard exposes the same capped
score used by routing and basket transfers.

Optional Claude diligence remains a secondary product signal. KEEP, REJECT,
and MAYBE can help prioritize review, but none enter Yes/No training data
without an explicit human action. Confirmed Glasswing portfolio ground truth
and manual labels are protected from automatic movement and payload cleanup.

Yes Basket membership is an explicit Investable decision. No Basket membership
is an explicit Hard Denied decision. Human/portfolio provenance is tracked in
`data/manual/training_label_metadata.csv`; automatic records are excluded from
the model-ready export.

The **Training Data → Label Next** queue prioritizes likely positives,
borderline companies, and disagreements between rule, learned, and optional
Claude signals. This concentrates human review where it improves the model
most.

Every manual move creates a timestamped, checksummed, human-only snapshot under
`data/manual/snapshots`. The **System Health** top tab validates and restores
those snapshots, reports database/index integrity and model reversal metrics,
and controls all local workers through a bounded-restart supervisor.

The application is local-first but cloud-portable. Configure storage roots
with `.env.example`. It is deployed today on Railway as four services --
`web` (this dashboard) plus `pipeline-worker`, `ai-worker`, and
`zone-worker`, each running its own crawler continuously -- backed by a
shared PostgreSQL instance (classification store, pipeline queue, `.AI`
search, Zone Universe, each in its own schema) and S3-compatible object
storage for manual snapshots and new HTML archives. Every backend is
selected purely by environment variable presence -- local SQLite/local
files remain the default with no configuration required. See
`CLOUD_MIGRATION.md` for exact status and `CUTOVER_RUNBOOK.md` for how each
piece was turned on.

For a code-level map, see `CODE_MENTAL_MODEL.md`.

## Recommended settings

- Extensions: `.ai`, `.app`, `.dev`
- Filter preset: Balanced Glasswing
- Throughput: Fast
- Automatic Claude diligence: Off

## Troubleshooting

```bash
./diagnose.sh
```

The dashboard also displays the latest startup error when the background
process exits unexpectedly.

Common checks:

```bash
docker ps --filter name=certstream
```


## Visible bucket reconciliation fix

Every successfully screened website is now represented exactly once in Investable, Coming Soon, Needs Review, or Hard Denied. Internal `screened_out` rows are visible under Hard Denied with their filter reason, and `comp` rows are visible under Needs Review.


## Classification quality correction

- Credible borderline companies route to **Needs Review**. Low scores, sparse
  copy, inaccessible sites, and incomplete evidence route to **Insufficient
  Evidence**, never Hard Denied.
- Hard Denied requires an explicit factual disqualifier such as a parked domain, confirmed non-US location, consumer product, agency/services business, non-English page, media property, or commodity wrapper.
- Historical `screened_out` records are reclassified dynamically: explicit
  disqualifiers remain Hard Denied, credible borderline companies become
  Needs Review, and unresolved rows remain Insufficient Evidence.
- Plausible enterprise companies can surface as Investable at a slightly lower first-pass threshold, while manual decisions remain final.
- A conservative enterprise rescue gate moves concise, strongly relevant B2B
  AI, security, infrastructure, and vertical-workflow companies into Needs
  Review without bypassing explicit disqualifiers.
- Fit scoring prioritizes concrete Glasswing thesis, buyer, product, technical,
  workflow, and important-problem intersections. Generic AI services, job
  boards, coupon pages, portfolios, and commodity outreach tools are capped.


## Rescore saved crawler content

When the screening rules change, update every locally saved `.AI`, `.app`, and
`.com` website without contacting the websites again:

```bash
./run_corpus_rescore.sh
```

The job is checkpointed, resumable, and deliberately throttled with small
batches and pauses. If a large zone-file import is active, it waits quietly
and starts automatically after that import commits. It only rewrites the
canonical classification rows during each small batch. Automated results never
write Yes/No training labels; human bucket moves remain final authority.

Check progress at any time:

```bash
.venv/bin/python reclassify_corpus.py status
tail -f data/logs/cached_corpus_rescore.log
```

For a small validation pass that saves nothing:

```bash
.venv/bin/python reclassify_corpus.py run --limit 25 --dry-run
```


## Evidence-oriented page selection

Both website crawlers use the same internal-link policy. Navigation links are
discovered before navigation/footer text is removed, then ranked for the
evidence needed by the Glasswing screen:

1. technical depth: research, technology, architecture, security, whitepapers,
   developer documentation, and APIs;
2. product wedge: product, platform, solutions, use cases, and industries;
3. company quality: founders, leadership, team, About, and company pages;
4. traction: customers and case studies.

Legal, login, checkout, and generic blog pages are excluded. First-party
documentation subdomains are allowed, unrelated domains are not, and a failed
preferred page no longer consumes the page budget—the crawler tries the next
ranked evidence page.

The crawl itself is staged. Every reachable domain starts with one homepage:

- obvious parked, for-sale, prelaunch, hosting-placeholder, explicit
  non-English, and low-value sites stop after that one retained page;
- ambiguous but plausible companies receive one evidence-page request;
- strong company, product, or technical signals receive up to two;
- every retained homepage still flows through the normal Glasswing classifier,
  even when enrichment stops.

The dashboards report `Homepage sufficient` and `Evidence enriched` separately
so request savings and evidence depth remain visible.


## Human ground truth and rejection cleanup

`data/manual/human_ground_truth.csv` is the canonical training ledger. It
contains only explicit human Yes/No decisions and verified Glasswing portfolio
positives. Python and LLM classifications may suggest a bucket, but cannot
label their own output as training truth.

An automatically Hard Denied domain keeps only its immutable source-inventory
entry and a minimal queue deduplication marker. Its classification, searchable
page, HTML payload, retry/registration data, and CSV exports are deleted. Run
the complete maintenance pass with:

```bash
.venv/bin/python data_maintenance.py clean-all
```

Every explicit human move also creates a checksummed, human-only snapshot under
`data/manual/snapshots`. Worker controls, database/index checks, model feedback,
snapshot validation, and restore are available on the top-level **System
Health** page. The local automatic-recovery supervisor uses bounded exponential
backoff and never starts unless processing is explicitly requested.

Cloud migration boundaries and the recommended Railway service layout are
documented in `CLOUD_MIGRATION.md`.
