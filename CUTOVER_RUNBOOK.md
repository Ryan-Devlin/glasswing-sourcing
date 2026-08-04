# Cutover runbook

This is the exact, concrete sequence used to switch each piece of
`CLOUD_MIGRATION.md` on for real. Kept as a reference for how the current
live state was reached and as a template for any future subsystem --
not a pending checklist. Nothing here should run without deciding to run
it -- these are real, mostly one-way actions on real data and real money.

Current state (updated 2026-08-04): four Railway services (`web`,
`pipeline-worker`, `ai-worker`, `zone-worker`) plus one managed `Postgres`
service, project `glasswing-sourcing`. All four databases (classification
store, pipeline queue, ai_search, zone_universe) are on Postgres and all
three workers are running continuously in the cloud. See Part 1 and Part 2
below for how each step was done.

## Part 1: database + archive cutover

Turns on the Postgres backend for one of {pipeline queue, ai_search,
zone_universe} and/or the S3 backend for HTML archives, for real data.

### Per-database cutover (repeat per database)

1. **Confirm the target Postgres is reachable and the schema is what you
   expect.** For an existing Railway Postgres, get a connection string
   without printing it:
   ```
   railway run --service Postgres -- printenv DATABASE_PUBLIC_URL > /tmp/pg.txt
   ```
2. **Dry-run the matching migration script locally** to confirm the row
   counts match what you expect from the real local database:
   ```
   PIPELINE_DATABASE_URL="$(cat /tmp/pg.txt)" python migrate_pipeline_to_postgres.py
   AI_SEARCH_DATABASE_URL="$(cat /tmp/pg.txt)" python migrate_ai_search_to_postgres.py
   ZONE_UNIVERSE_DATABASE_URL="$(cat /tmp/pg.txt)" python migrate_zone_universe_to_postgres.py
   ```
3. **Run the real copy** (`--execute`). This only ever adds rows
   (`ON CONFLICT DO NOTHING`) -- it never deletes or modifies the local
   SQLite file, so it's safe to re-run if interrupted. It will show you the
   target host/database and ask you to type `yes` before writing anything
   (pass `--yes` to skip that prompt for a scripted/automated run):
   ```
   PIPELINE_DATABASE_URL="$(cat /tmp/pg.txt)" python migrate_pipeline_to_postgres.py --execute
   ```
   `ai_search`/`zone_universe`'s versions take longer (search_vector is
   recomputed per page after the copy) and print progress to stderr as they go.
4. **Verify** row counts in Postgres match the dry-run counts, and spot-check
   a few real rows/searches before touching the live deployment at all.
5. **Only once verified**, set the matching env var on the `web` service in
   Railway (dashboard, or `railway variable set NAME=VALUE --service web` --
   never pass the value as a bare CLI arg if it contains a password; prefer
   the dashboard or `--stdin` for anything sensitive) and redeploy
   (`railway redeploy` or push a commit) so the running container picks up
   the change. Confirm via `railway deployment list --json` and by using
   the affected feature in the live app.
6. **Do not delete the local SQLite file after cutover.** Local mode stays
   the fallback if `DATABASE_URL`-style variables are ever unset again --
   that's the whole point of the dual-backend design. Only remove local
   files as a deliberate, separate decision once you're confident in the
   cloud copy.

### HTML archive cutover (S3 / R2)

1. Finish enabling R2 in the Cloudflare dashboard (blocked as of
   2026-08-03 -- `wrangler r2 bucket create` fails with error code 10042
   until this is done manually).
2. `npx wrangler r2 bucket create glasswing-sourcing-archives`.
3. Create an R2 API token (Cloudflare dashboard -> R2 -> Manage API Tokens),
   scoped to that bucket only.
4. Set on the `web` service: `S3_BUCKET`, `S3_ENDPOINT_URL` (R2's
   account-specific endpoint), `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`.
   Leave `S3_REGION` at its default (`auto`).
5. Redeploy. From this point on, **newly crawled pages** archive to R2;
   already-archived local pages keep working unchanged (`read_html()`
   tells old and new pointers apart by path shape). There is no migration
   script for bulk-moving the existing local archive into R2 -- that's a
   separate decision (real bandwidth/storage cost) this session
   deliberately did not build tooling for, since it wasn't asked for and
   isn't required for new data to start landing in R2 immediately.

## Part 2: multi-service split (done, live)

Splits `pipeline`/`ai-worker`/`zone-worker` into their own Railway services
instead of running as local-only processes controlled from the Streamlit
UI. All three exist and are running continuously in the cloud today
(created via `railway_worker_setup.sh` + each service's own
`railway.<name>-worker.json` config-as-code file). Kept below as the exact
steps taken, for reference.

**This runs real, continuous crawling and CertStream ingestion in the cloud
around the clock**, including ongoing Anthropic API spend for Claude
diligence calls -- that's a live fact now, not a future decision.

Per new worker service:

1. Complete Part 1 for the database(s) that worker's crawler needs
   (`ai-worker` needs `AI_SEARCH_DATABASE_URL`; `zone-worker` needs
   `ZONE_UNIVERSE_DATABASE_URL`; `pipeline-worker` needs
   `PIPELINE_DATABASE_URL`). A worker cannot safely run against local
   SQLite from a separate Railway service -- there's no shared volume.
2. `railway service create <name>` in the same project (or via dashboard),
   linked to the same GitHub repo.
3. Set that service's start command:
   - `pipeline-worker`: `python continuous_pipeline.py`
   - `ai-worker`: `python -m ai_search.worker` (needs
     `PYTHONPATH=/app/ai-domain-search-engine/src:/app`)
   - `zone-worker`: `python -m zone_universe.worker` (needs
     `PYTHONPATH=/app/zone-universe/src:/app/ai-domain-search-engine/src:/app`)
4. Copy the relevant env vars from `web` (the database URL(s) from step 1,
   plus any crawler-specific settings in `.env.example`) onto the new
   service. Set `GLASSWING_RUNTIME_MODE=cloud` there too.
5. Deploy and watch `railway logs --service <name>` closely before trusting
   it unattended. Confirm the worker is actually writing to Postgres (row
   counts increasing) and not silently falling back to a local file inside
   its own ephemeral container (which would vanish on every redeploy).
6. Update `web`'s dashboard pages to reflect that a remote worker, not a
   local subprocess, now owns this crawler -- `runtime_control.py`'s
   local-process start/stop controls should stay disabled for it in cloud
   mode (already true via `GLASSWING_RUNTIME_MODE=cloud`, but worth
   re-confirming once a real worker exists to disable controls *for*).
