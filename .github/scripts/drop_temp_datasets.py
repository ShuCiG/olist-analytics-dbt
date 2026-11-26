"""Drop throwaway BigQuery datasets: drills, scale tests, leftover CI datasets (#343, D137).

Dry run by default; `--apply` drops. A dataset is dropped only if it passes three guards:
- its name matches one of RULES (dr254_raw, scale10x_marts, ci_pr_12_34_marts) and no
  protected pattern (the layers, airflow_*, elementary),
- it is older than the rule's minimum age (BigQuery refuses to drop a dataset in its first
  7 days after creation, so drills and scale tests wait 8; a CI dataset has 1 day: CI drops
  it itself, so one left behind is a failed cleanup),
- it carries no label `keep`.
GCP checks the name again (janitor.tf): the account can't delete a dataset outside the
prefixes dr*, scale*, ci_pr_*, whatever this script asks for.

Usage: python .github/scripts/drop_temp_datasets.py [--apply] [--project ID]
Env:   Application Default Credentials (google-github-actions/auth in CI)
       GITHUB_STEP_SUMMARY  the report is appended to it when set
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT = "olist-analytics-dbt-472315"

# (name pattern, minimum age in days)
RULES: list[tuple[re.Pattern[str], int]] = [
    (re.compile(r"^dr\d+[a-z]*_[a-z0-9_]+$"), 8),  # disaster recovery drills (dr254_raw, dr254b_marts)
    (re.compile(r"^scale\d+x[a-z0-9_]*$"), 8),  # scale tests (#250)
    (re.compile(r"^ci_pr_\d+_\d+(_[a-z0-9_]+)?$"), 1),  # PR CI datasets a failed cleanup left behind
]

# Never dropped, even if a rule matched: the long-lived datasets (infra/datasets.tf)
PROTECTED = re.compile(r"^(airflow_)?(raw|raw_staging|staging|intermediate|marts|seeds|snapshots|elementary)$")

KEEP_LABEL = "keep"


@dataclass
class DatasetInfo:
    dataset_id: str
    created: datetime
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class Plan:
    drop: list[DatasetInfo] = field(default_factory=list)
    keep: list[tuple[DatasetInfo, str]] = field(default_factory=list)  # (dataset, why)


def min_age_days(name: str) -> int | None:
    """The rule's minimum age for a name, or None when no rule covers it."""
    if PROTECTED.match(name):
        return None
    for pattern, days in RULES:
        if pattern.match(name):
            return days
    return None


def plan(datasets: list[DatasetInfo], now: datetime) -> Plan:
    out = Plan()
    for ds in sorted(datasets, key=lambda d: d.dataset_id):
        days = min_age_days(ds.dataset_id)
        if days is None:
            # Datasets no rule covers are not reported one by one: they are most of the project
            continue
        age = now - ds.created
        if KEEP_LABEL in ds.labels:
            out.keep.append((ds, f"label {KEEP_LABEL}"))
        elif age < timedelta(days=days):
            out.keep.append((ds, f"{age.days} d old, drops after {days} d"))
        else:
            out.drop.append(ds)
    return out


def report(p: Plan, apply: bool, failed: dict[str, str], now: datetime) -> str:
    lines = [f"## Throwaway BigQuery datasets ({'apply' if apply else 'dry run'}, {now:%Y-%m-%d %H:%M} UTC)", ""]
    verb = "Dropped" if apply else "Would drop"
    lines.append(f"{verb}: {len(p.drop) - len(failed) if apply else len(p.drop)}")
    for ds in p.drop:
        status = f" FAILED: {failed[ds.dataset_id]}" if ds.dataset_id in failed else ""
        lines.append(f"- `{ds.dataset_id}` (created {ds.created:%Y-%m-%d %H:%M} UTC){status}")
    lines.append(f"\nKept: {len(p.keep)}")
    for ds, why in p.keep:
        lines.append(f"- `{ds.dataset_id}`: {why}")
    return "\n".join(lines) + "\n"


def list_datasets(project: str) -> list[DatasetInfo]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    out: list[DatasetInfo] = []
    for item in client.list_datasets():
        if min_age_days(item.dataset_id) is None:
            continue  # no table or label call for the datasets that are not candidates
        ds = client.get_dataset(item.reference)
        assert ds.created is not None
        out.append(DatasetInfo(ds.dataset_id, ds.created, dict(ds.labels or {})))
    return out


def drop(project: str, p: Plan) -> dict[str, str]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    failed: dict[str, str] = {}
    for ds in p.drop:
        try:
            client.delete_dataset(f"{project}.{ds.dataset_id}", delete_contents=True, not_found_ok=True)
        except Exception as exc:  # report every dataset, fail at the end
            failed[ds.dataset_id] = str(exc).splitlines()[0][:200]
    return failed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="drop (default: list only)")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    now = datetime.now(UTC)
    p = plan(list_datasets(args.project), now)
    failed = drop(args.project, p) if args.apply else {}
    text = report(p, args.apply, failed, now)
    print(text)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as f:
            f.write(text)
    if failed:
        print(f"::error::{len(failed)} dataset(s) could not be dropped")
        sys.exit(1)


if __name__ == "__main__":
    main()
