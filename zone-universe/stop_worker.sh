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
  for _ in {1..120}; do
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  if kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "Pause requested; worker $WORKER_PID is still finishing its current safe batch."
    echo "Its PID/checkpoint was preserved. Run this command again to check it."
    exit 0
  fi
  echo "Stopped Zone Universe worker (PID $WORKER_PID)."
else
  echo "Removed stale Zone Universe worker PID."
fi
rm -f "$PID_FILE"
