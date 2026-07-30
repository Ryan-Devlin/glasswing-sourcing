#!/bin/zsh
set -e
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
mkdir -p logs

PID_FILE="logs/worker.pid"
if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    echo "Discovery/crawl worker already running (PID $PID)."
    exit 0
  fi
fi

nohup .venv/bin/python -m ai_search.worker \
  >> logs/worker.log 2>&1 &
echo $! > "$PID_FILE"
echo "Started discovery/crawl worker (PID $!)."

