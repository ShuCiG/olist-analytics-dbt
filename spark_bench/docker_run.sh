#!/usr/bin/env bash
# Run the Spark benchmark in a Linux container (Spark on Windows needs winutils).
#   OLIST_CSV_DIR=<folder> bash spark_bench/docker_run.sh [bench.py args, e.g. --scale 50 --only skew]
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "$repo:/work" -v "${OLIST_CSV_DIR:?set OLIST_CSV_DIR}:/data:ro" \
  -e UV_PROJECT_ENVIRONMENT=/opt/venv \
  -w /work python:3.12-bookworm bash -c "
    apt-get update -qq && apt-get install -y -qq default-jre-headless >/dev/null &&
    pip install -q uv && uv sync --only-group tsql -q &&
    uv run --no-sync python -m spark_bench.bench --csv-dir /data $*"
