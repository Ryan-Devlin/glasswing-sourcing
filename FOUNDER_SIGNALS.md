# Founder Signals

Founder Signals is a native module in the Glasswing sourcing dashboard. It
monitors permitted public evidence for credible signs that an exceptional
technical person may be forming a company. Scores are explainable rankings,
not probabilities or claims about private behavior.

## Architecture

- `founder_signals.py` owns additive schema migration, talent intake,
  conservative deduplication, GitHub identity resolution, official GitHub API
  collection, permanent event storage, activity aggregation, signals, scoring,
  analyst workflow, settings, and synthetic demonstrations.
- `web_api.py` exposes the module through the existing FastAPI service and the
  existing signed admin session. Reads are company-wide; all mutations require
  admin authentication.
- `webapp/src/main.jsx` renders five views inside the existing React shell:
  Priority Signals, Talent Universe, Activity Monitor, Identity Review, and
  Settings.
- The FastAPI lifespan scheduler checks hourly and starts collection only when
  the configured interval has elapsed. `founder_signals_worker.py` remains an
  optional dedicated-worker entrypoint for higher-volume deployments.
- `railway.founder-signals.json` is the optional dedicated Railway worker
  configuration. No separate database is created.

The startup migration adds `founder_*`, `github_identity_candidates`, and
`verified_github_identities` tables with foreign keys, idempotency constraints,
indexes, provenance, historical scores, job logs, and collection errors. It
does not alter or delete existing sourcing records.

## Configuration

Set a fine-grained, read-only GitHub token in every process that performs
identity resolution or collection:

```bash
GITHUB_TOKEN=github_pat_...
```

The token is never returned by the API or displayed by the UI. The Settings
view only reports whether it is configured. Target companies, universities,
laboratories, technical categories, weights, alert threshold, collection
frequency, people-per-run limit, and automatic identity threshold are stored
in the canonical database and editable by an administrator.

Start a local collector with:

```bash
python founder_signals_worker.py
```

Or trigger Discovery, Collection, and Score Recalculation from Activity
Monitor. Collection jobs are idempotent, partially successful, retryable, and
log row-level errors without aborting unrelated people.

## Talent intake and discovery

Talent Universe accepts `.csv` and `.xlsx`. Recognized headings include Full
Name/Name, Employer/Company, Title, School/University, GitHub, Website,
LinkedIn, Location, and Category. List values can be separated with commas,
semicolons, or pipes. Original source URLs and extraction methods are retained.

Discovery uses GitHub's official user-search/profile APIs against the target
organizations configured in Settings. Discovered matches start inactive and
in `needs_research`; they are not silently monitored. Direct LinkedIn scraping
is not implemented or permitted.

Candidate deduplication uses verified GitHub usernames, personal domains, and
name plus employer/school combinations. Ambiguous records are never merged.

## Identity resolution

Identity scores use explainable evidence: verified cross-links, name,
employer, school, personal site, location, and supplied username. Conflicting
employer/site evidence caps automatic confidence. A single score of 90+ without
contradictions may be verified automatically; 70–89 and multiple plausible
accounts go to Identity Review; lower scores remain unresolved. Analysts can
approve, reject, or enter another public username.

## Collection, metrics, and scoring

Only GitHub's official API is used. Events are keyed by GitHub event ID,
paginated, permanently stored, and safe to ingest repeatedly. The module tracks
7-, 30-, 90-, and 180-day activity, previous 30 days, a rolling six-month
baseline, active days, repositories, pushes, pull requests, issues, releases,
forks, organizations, repositories, and acceleration. Sparse histories are
explicitly marked insufficient.

Signals preserve underlying dates, evidence, source URLs, confidence, rule
version, and analyst disposition. Current rules include meaningful activity
acceleration, new personal repositories, new organizations, external domains,
commercial/customer-discovery language, and founding-team hiring language.
Weak signals never independently create a high-confidence alert.

Overall priority combines configurable Talent, Founder Transition, Glasswing
Fit, Identity, and Signal Confidence scores. Signal contributions decay
exponentially with age and related recent signals receive a bounded combination
bonus. Every calculation writes a historical score snapshot and explanation.

## Synthetic data

In Settings, choose **Load synthetic demo data**. Eight clearly labeled records
cover stable activity, dormancy break, organization creation, commercialization,
ambiguous identity, false positive, and high-confidence formation behavior.
The seed is idempotent and never resembles real monitoring data.

## Tests

```bash
PYTHONPATH=. .venv/bin/pytest -q tests/test_founder_signals.py
npm --prefix webapp run build
```

External calls are mocked. The suite covers migration, import, deduplication,
identity confidence/ambiguity, pagination, event idempotency, sparse baselines,
acceleration, commercialization signals, explainable scores, analyst audit,
settings, and synthetic seed behavior.

## Data restrictions and limitations

- Public professional information only; no sensitive-characteristic inference.
- No unauthorized LinkedIn scraping. LinkedIn URLs are reference links only.
- GitHub public events have limited upstream history. Daily collection builds
  the durable baseline prospectively.
- Private repository work is invisible; absence of public activity must never
  be interpreted as inactivity.
- GitHub discovery is deliberately conservative. Conference, OpenAlex, ORCID,
  Semantic Scholar, and university connectors can be added behind the same
  provenance interface, subject to their terms and rate limits.
- Email/digest delivery is not activated by this module. It records dashboard
  alerts and can later feed the existing digest architecture without contacting
  real recipients during implementation.
