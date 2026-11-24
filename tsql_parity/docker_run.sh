#!/usr/bin/env bash
# Run the parity harness in a Linux container (for machines without a working Spark:
# Spark on Windows needs winutils). SQL Server comes from tsql_parity/docker-compose.yml.
#   OLIST_CSV_DIR=<folder> bash tsql_parity/docker_run.sh [pytest|run]
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
mode="${1:-run}"
case "$mode" in
  run) cmd="uv run --no-sync python -m tsql_parity.run --csv-dir /data" ;;
  pytest) cmd="uv run --no-sync pytest tsql_parity -q -p no:cacheprovider" ;;
  *) echo "usage: $0 [run|pytest]" >&2; exit 2 ;;
esac
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "$repo:/work" -v "${OLIST_CSV_DIR:?set OLIST_CSV_DIR}:/data:ro" \
  -e MSSQL_HOST=host.docker.internal -e UV_PROJECT_ENVIRONMENT=/opt/venv \
  -w /work python:3.12-bookworm bash -c "
    apt-get update -qq && apt-get install -y -qq default-jre-headless >/dev/null &&
    pip install -q uv && uv sync --only-group tsql -q && $cmd"
