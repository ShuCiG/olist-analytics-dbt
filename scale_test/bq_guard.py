"""Cost guard for the BigQuery scale test (#250): the bytes a dbt layer would bill, before it runs.

    uv run python scale_test/bq_guard.py --select path:models/staging --max-gib 5

Compiles the selected nodes (models and their tests) for the `ci` target, sends every
compiled query to BigQuery as a dry run (free, nothing is read or written) and adds up
what a real run would bill: bytes processed, at least 10 MiB per query (BigQuery's
minimum). Exits 1 above --max-gib, or when a query cannot be estimated (an upstream table
that does not exist yet): run the layers in order, estimating each after the one before it
is built. The dataset comes from DBT_CI_SCHEMA (and DBT_RAW_SCHEMA for the raw tables).
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRANSFORM = ROOT / "transform"
MIN_BILLED = 10 * 1024 * 1024
GIB = 1024**3


def compile_nodes(select: str) -> dict[str, str]:
    """Compiled SQL of the selected nodes and their tests: node path -> SQL."""
    compiled = TRANSFORM / "target" / "compiled"
    shutil.rmtree(compiled, ignore_errors=True)
    subprocess.run(
        ["uv", "run", "dbt", "compile", "--target", "ci", "--select", select, "--exclude", "package:elementary"],
        cwd=TRANSFORM,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return {str(p.relative_to(compiled)): p.read_text(encoding="utf-8") for p in sorted(compiled.rglob("*.sql"))}


def estimate(queries: dict[str, str]) -> tuple[dict[str, int], dict[str, str]]:
    """Billed bytes per query, and the queries BigQuery refused to estimate."""
    from google.cloud import bigquery

    client = bigquery.Client(project=os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315"), location="EU")
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    billed: dict[str, int] = {}
    failed: dict[str, str] = {}
    for name, sql in queries.items():
        try:
            billed[name] = max(MIN_BILLED, client.query(sql, job_config=config).total_bytes_processed or 0)
        except Exception as error:
            failed[name] = str(error).splitlines()[0][:160]
    return billed, failed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--select", required=True, help="dbt selector of the layer, e.g. path:models/staging")
    parser.add_argument("--max-gib", type=float, required=True, help="refuse above this many GiB billed")
    args = parser.parse_args()

    queries = compile_nodes(args.select)
    billed, failed = estimate(queries)
    total = sum(billed.values())
    for name, size in sorted(billed.items(), key=lambda item: -item[1])[:10]:
        print(f"{size / GIB:8.3f} GiB  {name}")
    print(f"{len(billed)} queries estimated, {total / GIB:.2f} GiB billed (limit {args.max_gib} GiB)")
    for name, message in failed.items():
        print(f"NOT ESTIMATED {name}: {message}")
    if failed or total > args.max_gib * GIB:
        sys.exit(1)


if __name__ == "__main__":
    main()
