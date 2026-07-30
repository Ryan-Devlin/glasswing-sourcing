#!/bin/zsh
set -e
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  python3 -m venv .venv
fi

.venv/bin/python -m pip install -q -r requirements.txt
"$PROJECT_DIR/start_worker.sh"
exec .venv/bin/python -m streamlit run app.py --server.port 8502
