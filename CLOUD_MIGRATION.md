# Cloud migration boundary

The application remains local-first, but new operational code is separated so
the UI does not depend directly on local PID files or manual CSV rewrites.

## Portable services

- `manual_truth_backup.py` owns checksummed human-only snapshots and restore.
- `runtime_control.py` owns desired worker state and local process operations.
- `health_monitor.py` owns read-only readiness checks.
- `model_evaluation.py` owns frozen-version human-feedback metrics.
- `training_data.py` owns the active-learning `Label Next` queue.

## Railway deployment shape

Use separate Railway services from the same repository:

1. `web`: Streamlit dashboard only.
2. `pipeline-worker`: certificate/new-domain screening.
3. `ai-worker`: `.AI` discovery and crawling.
4. `zone-worker`: `.COM/.APP` queue crawling.
5. managed PostgreSQL: classifications, decisions, queues, health heartbeats.
6. S3-compatible object storage: HTML packs and manual snapshots.

Set `GLASSWING_RUNTIME_MODE=cloud` for the web service. Railway restart policies
replace `worker_supervisor.py`; the System Health UI remains the observability
surface but does not attempt to spawn or signal sibling containers.

## Migration sequence

1. Put current data/log paths on a persistent Railway volume using the
   variables in `.env.example`.
2. Move HTML archives and manual snapshots to object storage.
3. Replace the SQLite implementations behind the service modules with
   PostgreSQL repositories.
4. Run the web and three workers as separate services.
5. Keep `human_ground_truth.csv` as an export, not the cloud source of truth.

