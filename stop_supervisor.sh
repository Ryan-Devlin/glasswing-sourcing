#!/bin/zsh
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

exec .venv/bin/python -c \
  "from runtime_control import request_services,stop_supervisor; request_services({\"pipeline\":False,\"ai_crawler\":False,\"zone_crawler\":False},requested_by=\"stop_supervisor.sh\"); stop_supervisor(); print(\"Supervisor stop requested.\")"
