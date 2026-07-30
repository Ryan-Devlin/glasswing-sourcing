#!/bin/zsh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

PYTHON="../.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "Missing parent project virtual environment: $PYTHON"
  exit 1
fi

export PYTHONPATH="$PROJECT_DIR/..:$PROJECT_DIR/src:$PROJECT_DIR/../ai-domain-search-engine/src"
exec "$PYTHON" -m zone_universe.cli "$@"
