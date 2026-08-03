# Cloud migration status

The application is local-first by default and cloud-portable by design: every
cloud backend is selected purely by an environment variable's presence, never
by a code branch that has to be flipped for a release. Local SQLite/local-file
mode is never a one-way removal -- it keeps working with zero configuration.

## Live today

- **Deployed on Railway** (`web` service, project `glasswing-sourcing`) with a
  persistent volume for local-only data (pipeline queue, `.AI`/Zone Universe
  SQLite databases and HTML archives). GitHub-connected auto-deploy on push
  to `main`, with `railway up`/`railway redeploy` as a manual fallback.
- **Classification store on PostgreSQL.** `classification_store.py`,
  `training_truth.py`, `rejection_cleanup.py`, `model_evaluation.py`, and
  `health_monitor.py` all run against either SQLite or Postgres through
  `db_backend.py`, chosen by whether `DATABASE_URL` is set. Verified against a
  live Postgres instance, not just local tests. Local SQLite remains the
  default when `DATABASE_URL` is unset.
- **Manual snapshots on S3-compatible object storage.** `manual_truth_backup.py`
  writes/reads/lists/restores checksummed manual-decision snapshots through
  `object_storage.py`, chosen by whether `S3_BUCKET` is set. Works with any
  S3-compatible provider (`S3_ENDPOINT_URL`) -- Cloudflare R2 is the intended
  provider, selected for its free egress, but the bucket has not been created
  yet (blocked on a one-time "enable R2" step in the Cloudflare dashboard, not
  a code dependency). Local files under `GLASSWING_MANUAL_SNAPSHOT_ROOT` are
  the default until `S3_BUCKET` is set.
- **On-demand Postgres backups** (`backup_database.py`): a logical backup
  (one CSV per table + manifest + sha256 sidecar) via Postgres's native COPY
  -- no `pg_dump` binary needed, so it runs identically on Railway, in CI, or
  a developer's Mac. Written through the same local-file-or-S3 backend as
  manual snapshots. Run manually (`python backup_database.py`) or wire to a
  schedule later; Railway's own managed Postgres backups are a separate,
  paid-plan feature this does not replace -- check Railway's dashboard for
  that.
- **CI on every push/PR** (`.github/workflows/ci.yml`): compile-check plus the
  full test suite, including an opt-in Postgres parity test that runs for
  real against an ephemeral `postgres:16` service container. Dependabot
  (`.github/dependabot.yml`) watches all three pip manifests and GitHub
  Actions weekly.
- **`GLASSWING_RUNTIME_MODE=cloud`** disables local-process controls (starting
  crawlers, killing workers) in the Streamlit UI, since Railway's own restart
  policy owns worker lifecycle for whatever is deployed. Set on the `web`
  service.

See `.env.example` for every variable and its default (unset = local mode).

- **CertStream pipeline queue has a tested Postgres dual-backend**
  (`continuous_pipeline.py`), built and verified against the real live
  database the same way the classification store was. Deliberately **not**
  wired into any running process yet: `continuous_pipeline.py` isn't
  imported by anything the deployed `web` service runs today (only by
  tests, and by its own local-only `__main__` entrypoint, which cloud mode
  already disables), so this is pure groundwork with zero live effect. The
  one piece kept working differently on purpose: `claim_batch()`'s
  domain-priority ordering used SQLite's `ATTACH DATABASE` to cheaply JOIN
  against the (still-local) ai_search/zone_universe corpora; Postgres has no
  ATTACH equivalent, so the Postgres path instead pulls a bounded candidate
  window (`min(size * 10, 2000)` pending rows) and re-ranks it in Python
  against those same local SQLite files looked up by domain -- correct at
  realistic batch sizes, but not a byte-for-byte behavioral clone at extreme
  queue depth.

- **HTML archives have a tested S3 dual-backend**, but the real archived
  corpus (years of crawled pages) has **not been migrated into it** -- both
  `html_archive.py` modules (ai_search, zone_universe) now write one S3
  object per page when `S3_BUCKET` is set, reusing the exact same
  `ArchivePointer` shape (path/offset/length/codec) the local sharded format
  already used, so no schema change was needed. `read_html()` tells old
  local pointers apart from new S3 ones by shape alone (local paths are
  always absolute; S3 keys deliberately aren't), so old and new rows can
  coexist in the same corpus indefinitely -- there's no forced cutover
  moment. Bulk-migrating the real existing archive (real money, real
  bandwidth, and R2 itself isn't enabled yet) is a separate, deliberate,
  explicitly-confirmed action, not something to run opportunistically.
- **`.AI` search (ai_search subproject) has a tested Postgres dual-backend**,
  gated on its own `AI_SEARCH_DATABASE_URL` (never the classification
  store's `DATABASE_URL`). Covers the full schema (domains/pages/
  crawl_attempts/shortlists/watch_queries/query_discoveries/html_blobs) plus
  FTS5 -> Postgres full-text search: a weighted `search_vector` tsvector
  column (domain/title/description/headings/body, weighted A/B/C/D
  approximating the original bm25 column weights) replaces the FTS5
  `page_search` virtual table; `compile_query_postgres()` reproduces
  `compile_query()`'s exact phrase/AND/NOT grammar (including "unquoted
  multi-word input is one exact phrase") using parameterized
  `phraseto_tsquery`/`plainto_tsquery` combined with tsquery's `&&`/`!!`
  operators, rather than a hand-built query string; `ts_rank_cd` replaces
  `bm25()` (note: opposite sort direction -- higher is more relevant);
  `ts_headline` replaces `snippet()`. Verified against the real live
  database: phrase-vs-word matching, negation, filters, and ranking order
  all confirmed correct, not just "runs without an exception."
  **Important fix found by that live verification:** every Postgres-backed
  piece (classification store, pipeline queue, ai_search) now runs in its
  own Postgres *schema* (`public`, `pipeline`, `ai_search` respectively, via
  `CREATE SCHEMA IF NOT EXISTS` + `SET search_path`) -- Railway only
  provides one Postgres instance today, so without this, same-named tables
  (`domains`, `pages`, ...) across subsystems would collide the moment two
  of them ever pointed at the same physical database, which is the actual
  likely real-world setup. Also fixed in the same pass: `continuous_pipeline.py`
  had been (harmlessly, since nothing live calls it) gated on the
  classification store's own `DATABASE_URL` -- given its own
  `PIPELINE_DATABASE_URL` instead, so that variable can never be
  misread elsewhere as "the pipeline queue is on Postgres too."
  Not yet done for ai_search: `screenable_domains()` (worker-only, still
  SQLite-only ATTACH) and a handful of worker-side `.fetchone()[0]`-style
  bugs in `crawler.py`/`maintenance.py`/`worker.py` -- all on the crawler
  worker's path, not the live dashboard's, and inert today the same way
  `continuous_pipeline.py` is.

## Not yet done

- **Still one Railway service**, not the four-service split below. There is
  no `pipeline-worker`/`ai-worker`/`zone-worker` split running today --
  CertStream discovery and crawling are not currently running in the cloud.
- **Zone Universe still runs entirely on SQLite+FTS5**, unmigrated. Same
  shape of work as ai_search above (schema + tsvector search), not yet
  started.
- **ai_search's crawler-worker-only code paths** (see above) still assume
  SQLite; only the parts reachable from the live dashboard were verified.

## Target service split (when the multi-worker split happens)

Separate Railway services from the same repository:

1. `web`: Streamlit dashboard only (live today).
2. `pipeline-worker`: certificate/new-domain screening.
3. `ai-worker`: `.AI` discovery and crawling.
4. `zone-worker`: `.COM`/`.APP` queue crawling.
5. managed PostgreSQL: classifications, decisions, queues, health heartbeats
   (live today).
6. S3-compatible object storage: HTML packs and manual snapshots (manual
   snapshots live today; HTML packs not yet designed).

## Portable service modules

- `manual_truth_backup.py` owns checksummed human-only snapshots and restore.
- `runtime_control.py` owns desired worker state and local process operations.
- `health_monitor.py` owns read-only readiness checks.
- `model_evaluation.py` owns frozen-version human-feedback metrics.
- `training_data.py` owns the active-learning `Label Next` queue.
- `db_backend.py` / `object_storage.py` own the SQLite-or-Postgres and
  local-file-or-S3 selection, respectively.

## Remaining migration sequence

1. ~~Put current data/log paths on a persistent Railway volume.~~ Done.
2. ~~Replace the SQLite classification store with a PostgreSQL-or-SQLite
   dual backend.~~ Done.
3. ~~Move manual snapshots to object storage (dual backend).~~ Done, pending
   R2 bucket creation.
4. ~~Design and build a dual object-storage backend for HTML archives.~~
   Done. Bulk-migrating the real existing archive into it is separate and
   still pending (needs R2 enabled, and its own explicit go-ahead given the
   cost/bandwidth involved).
5. ~~Add a Postgres-or-SQLite dual backend for the pipeline queue.~~ Done,
   verified live; not wired to anything running today.
6. ~~Migrate `.AI` search (FTS5 -> tsvector/ts_rank_cd) to Postgres.~~ Done
   for the dashboard-reachable code paths; the crawler worker's own paths
   (`screenable_domains`, `crawler.py`, `maintenance.py`, `worker.py`) still
   assume SQLite.
7. Migrate Zone Universe search (FTS5 -> tsvector/ts_rank_cd) to Postgres --
   same shape of work as step 6, not yet started.
8. Run the web and three workers as separate services.
9. Keep `human_ground_truth.csv` as an export, not the cloud source of truth
   (already true today -- Postgres holds the live table).
