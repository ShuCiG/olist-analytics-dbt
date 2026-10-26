"""BigQuery bytes billed by one CI run, as the Markdown of a PR comment (D72).

Every dbt query of the run carries the job label `github_run` = `<run id>-<attempt>`
(macros/query_comment.sql). This sums the run's top-level jobs from
`INFORMATION_SCHEMA.JOBS_BY_USER` (olist-ci's own jobs: `bigquery.jobs.list` comes
with roles/bigquery.user) per dbt node, and sets each node next to its last prod
build from the cost mart `marts.fct_dbt_job_costs` (D71), which olist-ci can read.

Usage: python .github/scripts/ci_cost.py <project id> <github_run label> <out.md> [run url]
Env:   Application Default Credentials (google-github-actions/auth in CI)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# First line of the comment: the comment job finds and updates it on the next push
MARKER = "<!-- bigquery-cost -->"
TOP = 15
MIB = 1024 * 1024

CI_JOBS_SQL = """
select
    (select l.value from unnest(labels) as l where l.key = 'dbt_resource_type') as resource_type,
    (select l.value from unnest(labels) as l where l.key = 'dbt_node') as node,
    count(*) as jobs,
    countif(state != 'DONE') as unfinished,
    sum(coalesce(total_bytes_billed, 0)) as bytes_billed
from `region-eu`.INFORMATION_SCHEMA.JOBS_BY_USER
where
    creation_time >= timestamp_sub(current_timestamp(), interval 1 day)
    and parent_job_id is null
    and exists (select 1 from unnest(labels) as l where l.key = 'github_run' and l.value = @run)
group by resource_type, node
"""

# Per node, the jobs of the latest prod invocation that built or tested it
PROD_SQL = """
with per_invocation as (
    select
        dbt_resource_type as resource_type,
        dbt_node as node,
        dbt_invocation_id,
        max(created_at) as built_at,
        count(*) as jobs,
        sum(bytes_billed) as bytes_billed
    from `{project}.marts.fct_dbt_job_costs`
    where dbt_target = 'prod' and dbt_node in unnest(@nodes)
    group by resource_type, node, dbt_invocation_id
)
select resource_type, node, jobs, bytes_billed
from per_invocation
qualify row_number() over (partition by resource_type, node order by built_at desc) = 1
"""


@dataclass
class NodeCost:
    resource_type: str | None  # None: run-level queries (hooks, introspection)
    node: str | None
    jobs: int
    bytes_billed: int
    unfinished: int = 0


def size(n: int) -> str:
    return f"{n / 1024 / MIB:.2f} GiB" if n >= 1024 * MIB else f"{n / MIB:.0f} MiB"


def render(ci: list[NodeCost], prod: dict[tuple[str, str], NodeCost], run_url: str = "") -> str:
    """Comment body: total, the TOP most expensive nodes, each next to its last prod build."""
    total = sum(c.bytes_billed for c in ci)
    jobs = sum(c.jobs for c in ci)
    run = f" ([CI run]({run_url}))" if run_url else ""
    lines = [MARKER, "### BigQuery cost of this PR's CI build", ""]
    if not ci:
        lines.append(f"No dbt jobs found for this run{run}.")
        return "\n".join(lines) + "\n"
    nodes = [c for c in ci if c.node]
    matched = [prod[(c.resource_type or "", c.node or "")] for c in nodes if (c.resource_type or "", c.node or "") in prod]
    lines.append(f"**{size(total)} billed** in {jobs} jobs{run}.")
    if matched:
        lines.append(
            f"The {len(matched)} of {len(nodes)} nodes that prod built before cost "
            f"{size(sum(p.bytes_billed for p in matched))} in their last prod build."
        )
    unfinished = sum(c.unfinished for c in ci)
    if unfinished:
        lines.append(f"{unfinished} jobs were still running: their bytes are not counted yet.")
    lines += ["", "| Node | Type | Jobs | Billed | Last prod build |", "|---|---|--:|--:|--:|"]
    ranked = sorted(ci, key=lambda c: (-c.bytes_billed, c.node or ""))
    for c in ranked[:TOP]:
        p = prod.get((c.resource_type or "", c.node or ""))
        name = f"`{c.node}`" if c.node else "run-level (hooks, introspection)"
        last = size(p.bytes_billed) if p else "–"
        lines.append(f"| {name} | {c.resource_type or 'run'} | {c.jobs} | {size(c.bytes_billed)} | {last} |")
    rest = ranked[TOP:]
    if rest:
        lines.append(f"| {len(rest)} more | | {sum(c.jobs for c in rest)} | {size(sum(c.bytes_billed for c in rest))} | |")
    lines += [
        "",
        "<sub>CI datasets start empty: incremental models build in full here, while prod "
        "merges new rows. BigQuery bills at least 10 MiB per table a query reads. "
        "Last prod build: the node's jobs in the latest prod run that built or tested it; "
        "– when prod has not built it since the job labels (D71). "
        "Updated on every push; this summary's own two queries are not counted.</sub>",
    ]
    return "\n".join(lines) + "\n"


def read_ci(client: Any, run: str) -> list[NodeCost]:
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("run", "STRING", run)])
    rows = client.query(CI_JOBS_SQL, job_config=config).result()
    return [NodeCost(r.resource_type, r.node, r.jobs, r.bytes_billed, r.unfinished) for r in rows]


def read_prod(client: Any, project: str, nodes: list[str]) -> dict[tuple[str, str], NodeCost]:
    from google.cloud import bigquery

    if not nodes:
        return {}
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("nodes", "STRING", nodes)])
    rows = client.query(PROD_SQL.format(project=project), job_config=config).result()
    return {(r.resource_type, r.node): NodeCost(r.resource_type, r.node, r.jobs, r.bytes_billed) for r in rows}


def main(argv: list[str]) -> int:
    if len(argv) not in (4, 5):
        print(__doc__, file=sys.stderr)
        return 2
    project, run, out = argv[1:4]
    run_url = argv[4] if len(argv) == 5 else ""
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location="EU")
    ci = read_ci(client, run)
    prod = read_prod(client, project, sorted({c.node for c in ci if c.node}))
    body = render(ci, prod, run_url)
    Path(out).write_text(body, encoding="utf-8")
    print(body)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
