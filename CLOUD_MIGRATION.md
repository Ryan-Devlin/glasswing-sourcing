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

## Not yet done

- **Still one Railway service**, not the four-service split below. There is
  no `pipeline-worker`/`ai-worker`/`zone-worker` split running today --
  CertStream discovery and crawling are not currently running in the cloud.
- **`.AI` search and Zone Universe still run on SQLite+FTS5**, unmigrated to
  Postgres. Full-text search behavior doesn't move to Postgres for free, so
  this needs its own design pass (`tsvector`/`pg_trgm` or similar), not a
  drop-in swap.
- **HTML archives are not on object storage.** They're stored as sharded,
  append-only files addressed by byte offset + length -- incompatible with
  S3's immutable-object model without redesigning the storage format
  (one-object-per-page) and touching the schema and crawler code. Deliberately
  scoped out as a separate decision rather than rushed.

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
4. Design and migrate HTML archives to object storage.
5. Migrate `.AI`/Zone Universe search databases to Postgres (or another
   full-text-capable cloud store).
6. Run the web and three workers as separate services.
7. Keep `human_ground_truth.csv` as an export, not the cloud source of truth
   (already true today -- Postgres holds the live table).
