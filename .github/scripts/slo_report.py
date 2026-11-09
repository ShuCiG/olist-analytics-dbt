"""SLO report of the data product (D111): reads the daily events, computes SLIs, error
budget and burn rate, writes the page for GitHub Pages and the alert list.

Events:
- freshness and quality: `marts.fct_slo_daily` (dbt model on Elementary's run history),
  from BigQuery prod, or from a local DuckDB file for a check on a laptop;
- deploy availability: the CI/CD workflow's runs on main (`gh run list`), completed ones
  that succeeded or failed (cancelled and skipped runs are no events).

Targets and windows: data-product/slo.yaml.

    uv run python .github/scripts/slo_report.py --out _site/slo                      # CD
    uv run python .github/scripts/slo_report.py --out out --duckdb data/olist.duckdb \
        --runs runs.json                                                             # local

Writes <out>/index.html and <out>/data.json. In GitHub Actions it also sets the step
output `alerts` (JSON list of the SLOs that call for an email, for job `notify-slo`).
"""

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alerting import slo  # noqa: E402

WORKFLOW = "ci-cd.yml"
FACT = "marts.fct_slo_daily"


def load_site():
    """data-product/ has a hyphen: load slo_site.py by path."""
    path = ROOT / "data-product" / "slo_site.py"
    spec = importlib.util.spec_from_file_location("slo_site", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def model_rows_bigquery(since: date) -> list[tuple[date, str, int, int]]:
    from google.cloud import bigquery

    project = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
    client = bigquery.Client(project=project, location="EU")
    query = (
        f"select slo_date, slo, good_events, total_events from `{project}.{FACT}` "
        "where slo_date >= @since"
    )
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("since", "DATE", since)])
    return [(r.slo_date, r.slo, r.good_events, r.total_events) for r in client.query(query, job_config=config).result()]


def model_rows_duckdb(path: str, since: date) -> list[tuple[date, str, int, int]]:
    import duckdb

    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(
            f"select slo_date, slo, good_events, total_events from {FACT} where slo_date >= ?", [since]
        ).fetchall()
    finally:
        con.close()


def deploy_runs(since: date) -> list[dict[str, Any]]:
    out = subprocess.run(
        ["gh", "run", "list", "--workflow", WORKFLOW, "--branch", "main", "--status", "completed",
         "--created", f">={since.isoformat()}", "--limit", "500", "--json", "conclusion,createdAt,url"],
        check=True, capture_output=True, text=True,
    )
    return json.loads(out.stdout)


def deploy_events(runs: list[dict[str, Any]]) -> slo.Daily:
    """Daily (good, total) of the completed runs that succeeded or failed."""
    days: dict[date, list[int]] = {}
    for run in runs:
        if run["conclusion"] not in ("success", "failure"):
            continue
        day = datetime.fromisoformat(run["createdAt"].replace("Z", "+00:00")).astimezone(timezone.utc).date()
        counts = days.setdefault(day, [0, 0])
        counts[0] += run["conclusion"] == "success"
        counts[1] += 1
    return [(day, good, total) for day, (good, total) in sorted(days.items())]


def build_report(
    config: dict[str, Any], rows: list[tuple[date, str, int, int]], runs: list[dict[str, Any]], today: date, now: str
) -> dict[str, Any]:
    events: dict[str, slo.Daily] = {key: [] for key in config["slos"]}
    for day, key, good, total in rows:
        if key in events:
            events[key].append((day, int(good), int(total)))
    events["deploy_availability"] = deploy_events(runs)
    statuses = [
        slo.evaluate(
            key, meta["title"], float(meta["target"]), events[key], today,
            window_days=config["window_days"], burn_days=config["burn_window_days"], burn_alert=config["burn_alert"],
        )
        for key, meta in config["slos"].items()
    ]
    return {
        "generated_at": now,
        "today": today.isoformat(),
        "slos": [s.as_dict() for s in statuses],
        "alerts": [s.as_dict(daily=False) for s in slo.alerts(statuses)],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--duckdb", help="read the model from this DuckDB file instead of BigQuery prod")
    parser.add_argument("--runs", type=Path, help="gh run list JSON instead of calling gh (local check)")
    parser.add_argument("--today", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    args = parser.parse_args()

    config = yaml.safe_load((ROOT / "data-product" / "slo.yaml").read_text(encoding="utf-8"))
    since = args.today - timedelta(days=config["window_days"] - 1)
    rows = model_rows_duckdb(args.duckdb, since) if args.duckdb else model_rows_bigquery(since)
    runs = json.loads(args.runs.read_text(encoding="utf-8")) if args.runs else deploy_runs(since)

    report = build_report(config, rows, runs, args.today, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "data.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    (args.out / "index.html").write_text(load_site().render(report, config), encoding="utf-8")

    for s in report["slos"]:
        print(slo.describe(s))
    flagged = report["alerts"]
    print(f"{len(flagged)} SLO(s) call for an email")
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as f:
            f.write(f"alerts={json.dumps(flagged, separators=(',', ':'))}\n")


if __name__ == "__main__":
    main()
