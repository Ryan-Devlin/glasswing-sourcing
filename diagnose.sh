#!/bin/zsh

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo "=== Glasswing Sourcing Diagnostics ==="
echo "Project: $PROJECT_DIR"
echo ""

echo "[Files]"
[[ -f .env ]] && echo ".env: present" || echo ".env: MISSING"
[[ -x .venv/bin/python ]] && echo ".venv: present" || echo ".venv: MISSING"
[[ -f requirements.txt ]] && echo "requirements.txt: present" || echo "requirements.txt: MISSING"
echo ""

echo "[Python]"
if [[ -x .venv/bin/python ]]; then
  .venv/bin/python --version
  .venv/bin/python -m py_compile dashboard.py continuous_pipeline.py filter.py && echo "Core compile: OK"
  .venv/bin/python -c "import streamlit, pandas, aiohttp, websockets, anthropic, dotenv; print('Core imports: OK')" 2>&1
else
  echo "Project Python unavailable. Run ./run_dashboard.sh once."
fi
echo ""

echo "[Docker]"
docker info >/dev/null 2>&1 && echo "Docker: running" || echo "Docker: NOT running"
docker ps -a --filter name=certstream --format 'CertStream container: {{.Status}}' 2>/dev/null
echo ""

echo "[Processes]"
pgrep -af "continuous_pipeline.py|streamlit run dashboard.py" 2>/dev/null || echo "No pipeline/dashboard processes found."
echo ""

echo "[Live status]"
cat logs/continuous_status.json 2>/dev/null || echo "No continuous status file yet."
echo ""

echo "[Latest pipeline log]"
tail -n 80 logs/continuous_pipeline.log 2>/dev/null || echo "No pipeline log yet."
