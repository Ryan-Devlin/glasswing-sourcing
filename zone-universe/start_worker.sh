#!/bin/zsh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
mkdir -p logs data/zonefiles data/queues

PYTHON="../.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "Missing parent project virtual environment: $PYTHON"
  exit 1
fi

PID_FILE="logs/worker.pid"
if [[ -f "$PID_FILE" ]]; then
  WORKER_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "Zone Universe worker already running (PID $WORKER_PID)."
    exit 0
  fi
fi

rm -f logs/worker.stop logs/worker.pid
export PYTHONPATH="$PROJECT_DIR/..:$PROJECT_DIR/src:$PROJECT_DIR/../ai-domain-search-engine/src"
nohup "$PYTHON" -m zone_universe.worker >> logs/worker.log 2>&1 &
echo $! > "$PID_FILE"
echo "Started Zone Universe worker (PID $!)."
