#!/bin/zsh
set -e
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
PID_FILE="logs/worker.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "Worker is not running."
  exit 0
fi
PID="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  echo "Stopped worker (PID $PID)."
else
  echo "Removed stale worker PID."
fi
rm -f "$PID_FILE"

