# Launch instructions

This build is designed to replace the application code while preserving your existing `data` folder and `.env`.

1. Stop the old dashboard and pipeline.
2. Back up `data` and `.env`.
3. Replace the project folder.
4. Restore `data` and `.env`.
5. Confirm Docker Desktop and the `certstream` container are running.
6. Run `./run_dashboard.sh`.
