#!/bin/zsh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

exec .venv/bin/python -c \
  "from runtime_control import ensure_supervisor; print(f'Supervisor PID {ensure_supervisor()}')"

