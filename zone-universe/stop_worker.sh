#!/bin/zsh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
PID_FILE="logs/worker.pid"
STOP_FILE="logs/worker.stop"

if [[ ! -f "$PID_FILE" ]]; then
  echo "Zone Universe worker is not running."
  exit 0
fi

WORKER_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
touch "$STOP_FILE"
if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
  kill -TERM "$WORKER_PID"
  for _ in {1..20}; do
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  echo "Stopped Zone Universe worker (PID $WORKER_PID)."
else
  echo "Removed stale Zone Universe worker PID."
fi
rm -f "$PID_FILE"
