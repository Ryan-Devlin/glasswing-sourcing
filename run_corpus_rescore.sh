#!/bin/zsh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$PROJECT_DIR/.venv/bin/python"
LOG_DIR="$PROJECT_DIR/data/logs"
PID_FILE="$PROJECT_DIR/data/reclassification/cached_corpus.pid"
LOG_FILE="$LOG_DIR/cached_corpus_rescore.log"

mkdir -p "$LOG_DIR" "$(dirname "$PID_FILE")"

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(tr -dc '0-9' < "$PID_FILE")"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "Cached-corpus rescore is already running (PID $EXISTING_PID)."
    exit 0
  fi
fi

nohup "$PYTHON" "$PROJECT_DIR/reclassify_corpus.py" run \
  --scope all \
  --batch-size 50 \
  --pause-seconds 0.5 \
  --wait-for-zone-import \
  >> "$LOG_FILE" 2>&1 &

RESCORE_PID=$!
echo "$RESCORE_PID" > "$PID_FILE"
echo "Started low-priority cached-corpus rescore (PID $RESCORE_PID)."
echo "Log: $LOG_FILE"
echo "Status: $PYTHON $PROJECT_DIR/reclassify_corpus.py status"
