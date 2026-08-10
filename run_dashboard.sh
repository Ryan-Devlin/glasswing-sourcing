#!/bin/zsh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo ""
echo "Glasswing Sourcing"
echo "Project: $PROJECT_DIR"
echo ""

if [[ ! -f ".env" ]]; then
  echo "ERROR: .env is missing."
  echo "Create it with: touch .env && open -e .env"
  exit 1
fi

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Creating the project virtual environment..."
  rm -rf .venv
  python3 -m venv .venv
fi

PYTHON="$PROJECT_DIR/.venv/bin/python"

if ! "$PYTHON" -c "import fastapi, uvicorn, pandas, requests, websockets, anthropic, dotenv" >/dev/null 2>&1; then
  echo "Installing or repairing project packages..."
  "$PYTHON" -m pip install --upgrade pip
  "$PYTHON" -m pip install -r requirements.txt
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "ERROR: Node.js/npm is required to build the React interface."
  exit 1
fi

if [[ ! -d "webapp/node_modules" ]]; then
  echo "Installing web interface packages..."
  (cd webapp && npm ci)
fi

echo "Building the web interface..."
(cd webapp && npm run build)

if ! "$PYTHON" -c "from dotenv import load_dotenv; import os; load_dotenv(); raise SystemExit(0 if os.getenv('ANTHROPIC_API_KEY','').strip() else 1)" >/dev/null 2>&1; then
  echo "WARNING: ANTHROPIC_API_KEY is missing from .env."
  echo "Python screening will work, but Claude review will not."
fi

mkdir -p logs data/filtered data/llm data/manual data/raw

if ! "$PYTHON" -m py_compile \
  web_api.py \
  main.py dashboard.py pages/ai_search.py pages/zone_universe.py \
  pages/system_health.py continuous_pipeline.py filter.py collector.py \
  common.py claude_classifier.py diligence_logic.py \
  classification_store.py manual_truth_backup.py model_evaluation.py \
  runtime_control.py worker_supervisor.py health_monitor.py training_data.py; then
  echo "ERROR: Core Python files failed the startup compile check."
  exit 1
fi

if [[ -f "logs/continuous_pipeline.pid" ]]; then
  OLD_PID="$(cat logs/continuous_pipeline.pid 2>/dev/null || true)"
  if [[ -z "$OLD_PID" ]] || ! kill -0 "$OLD_PID" 2>/dev/null; then
    rm -f logs/continuous_pipeline.pid logs/continuous_pipeline.stop
  fi
fi

echo "Starting Glasswing Sourcing at http://localhost:8501"
exec "$PYTHON" -m uvicorn web_api:app --host 127.0.0.1 --port 8501
