"""Render the status page (/status/ on GitHub Pages, D125) from the summary that
alerting/status.py builds: the last build's totals, the known warnings with the
decision that explains each, source freshness, data contract checks and the SLOs.

Stdlib only; one self-contained HTML file (inline CSS), like the SLO page.
"""

import html
from typing import Any

REPO = "https://github.com/ShuCiG/olist-analytics-dbt"

OVERALL = {
    "healthy": ("ok", "All checks pass"),
    "warnings": ("warn", "Passing, with known warnings"),
    "failing": ("bad", "Checks failing"),
}
SLO_STATE = {"ok": "ok", "burning": "bad", "exhausted": "bad", "no data": "none"}


def e(value: Any) -> str:
    return html.escape(str(value))


def decision_link(decision: str) -> str:
    if not decision:
        return '<span class="state bad">unexplained</span>'
    return f'<a href="{REPO}/blob/main/docs/DECISIONS.md" title="docs/DECISIONS.md, entry {e(decision)}">{e(decision)}</a>'


def totals_tiles(totals: dict[str, int]) -> str:
    tiles = [("pass", "ok"), ("warn", "warn"), ("error", "bad"), ("skip", "none"), ("no-op", "none")]
    return "".join(
        f'<div class="tile {cls if totals[key] else "none"}"><div class="big">{totals[key]}</div>'
        f'<div class="muted">{key.upper()}</div></div>'
        for key, cls in tiles
    )


def findings_table(findings: list[dict[str, Any]]) -> str:
    if not findings:
        return '<p class="muted">No warnings or errors in the last build.</p>'
    rows = "".join(
        f'<tr><td><span class="state {"bad" if f["outcome"] == "error" else "warn"}">{e(f["outcome"])}</span></td>'
        f'<td>{e(f["description"] or f["name"])}<div class="muted"><code>{e(f["name"][:70])}</code></div></td>'
        f'<td class="num">{"" if f["failures"] is None else f"{f["failures"]:,}"} {e(f["unit"])}</td>'
        f'<td>{decision_link(f["decision"])}</td></tr>'
        for f in findings
    )
    return (
        "<table><thead><tr><th></th><th>What it means</th><th>Found</th><th>Decision</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def contracts_table(contracts: list[dict[str, Any]]) -> str:
    if not contracts:
        return '<p class="muted">No contract results in this build.</p>'
    rows = ""
    for c in contracts:
        cls = "ok" if c["result"] == "passed" else "bad"
        failed = "".join(f"<div class=\"muted\">{e(name)}</div>" for name in c["failed_checks"])
        rows += (
            f'<tr><td><code>{e(c["id"])}</code> {e(c["version"])}</td>'
            f'<td><span class="state {cls}">{e(c["result"])}</span></td>'
            f'<td class="num">{c["passed"]} of {c["total"]}</td><td>{failed}</td></tr>'
        )
    return (
        "<table><thead><tr><th>Contract</th><th>Result</th><th>Checks passed</th><th>Failed</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def freshness_line(freshness: dict[str, Any]) -> str:
    totals = freshness["totals"]
    if not totals:
        return '<p class="muted">No freshness result in this build.</p>'
    counts = ", ".join(f"{n} {k}" for k, n in sorted(totals.items()))
    late = "".join(
        f'<li><code>{e(f["source"])}</code>: {e(f["outcome"])}, last load {e(f["max_loaded_at"])} ({f["age_hours"]} h ago)</li>'
        for f in freshness["findings"]
    )
    return f"<p>Sources: {e(counts)}.</p>" + (f"<ul>{late}</ul>" if late else "")


def slo_cards(slos: list[dict[str, Any]]) -> str:
    if not slos:
        return '<p class="muted">The SLO report is not part of this build.</p>'
    rows = ""
    for s in slos:
        sli = "n/a" if s["sli"] is None else f"{s['sli'] * 100:.1f}%"
        left = "n/a" if s["budget_left"] is None else f"{s['budget_left'] * 100:.0f}%"
        burn = "n/a" if s["burn_rate"] is None else f"{s['burn_rate']:.1f}x"
        rows += (
            f'<tr><td>{e(s["title"])}</td><td><span class="state {SLO_STATE[s["state"]]}">{e(s["state"])}</span></td>'
            f'<td class="num">{sli} (target {s["target"] * 100:.0f}%)</td><td class="num">{left}</td>'
            f'<td class="num">{burn}</td></tr>'
        )
    return (
        "<table><thead><tr><th>SLO</th><th>State</th><th>SLI, window</th><th>Budget left</th><th>Burn rate</th>"
        f'</tr></thead><tbody>{rows}</tbody></table><p class="muted">Details and history: <a href="../slo/">/slo/</a>.</p>'
    )


def render(status: dict[str, Any]) -> str:
    """`status`: the structure of alerting.status.build_status."""
    cls, label = OVERALL[status["overall"]]
    totals = status["totals"]
    sha = status.get("sha", "")
    commit = f'<a href="{REPO}/commit/{e(sha)}"><code>{e(sha[:7])}</code></a>' if sha else "unknown commit"
    run = f' · <a href="{e(status["run_url"])}">CI run</a>' if status.get("run_url") else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Status · Olist commerce</title>
<style>
:root {{ --bg:#fff; --fg:#1d232b; --muted:#5d6875; --line:#dde2e8; --card:#f6f8fa; --accent:#2f6fde;
  --good:#2da44e; --bad:#cf222e; --warn:#9a6700; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#14181d; --fg:#e4e8ed; --muted:#9aa5b1; --line:#2c333b;
  --card:#1b2027; --accent:#7aa7ff; --good:#3fb950; --bad:#f85149; --warn:#d29922; }} }}
body {{ margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 system-ui, sans-serif; }}
main {{ max-width:1100px; margin:0 auto; padding:24px 16px 48px; }}
a {{ color:var(--accent); }} h1 {{ margin:0 0 4px; }} h2 {{ margin-top:32px; }}
.muted {{ color:var(--muted); font-size:13px; }} .meta {{ color:var(--muted); }}
.banner {{ border:1px solid var(--line); border-left-width:6px; border-radius:8px; background:var(--card);
  padding:12px 16px; margin:16px 0; font-size:18px; font-weight:600; }}
.banner.ok {{ border-left-color:var(--good); }} .banner.warn {{ border-left-color:var(--warn); }}
.banner.bad {{ border-left-color:var(--bad); }}
.tiles {{ display:flex; flex-wrap:wrap; gap:12px; }}
.tile {{ background:var(--card); border:1px solid var(--line); border-radius:8px; padding:8px 16px; min-width:90px; }}
.tile.ok .big {{ color:var(--good); }} .tile.warn .big {{ color:var(--warn); }} .tile.bad .big {{ color:var(--bad); }}
.big {{ font-size:26px; font-weight:600; }}
table {{ border-collapse:collapse; width:100%; }} th, td {{ text-align:left; padding:6px 10px; border-bottom:1px solid var(--line); vertical-align:top; }}
th {{ color:var(--muted); font-size:13px; font-weight:500; }} td.num {{ white-space:nowrap; }}
.state {{ border:1px solid var(--line); border-radius:999px; padding:1px 10px; font-size:12px; white-space:nowrap; }}
.state.ok {{ color:var(--good); border-color:var(--good); }} .state.warn {{ color:var(--warn); border-color:var(--warn); }}
.state.bad {{ color:var(--bad); border-color:var(--bad); }}
code {{ font-size:13px; background:var(--card); padding:0 3px; border-radius:3px; }}
</style></head>
<body><main>
<p class="meta"><a href="../">dbt docs</a> · <a href="../data-product/">data product</a> · <a href="../slo/">SLOs</a> · <a href="../elementary/">Elementary report</a> · status</p>
<h1>Data status</h1>
<p>One page for the state of the Olist marts after the last production deploy: the tests of the full
<code>dbt build</code>, the known warnings with the decision that explains each, source freshness,
the data contract checks and the SLOs.</p>
<div class="banner {cls}">{e(label)}</div>
<p class="meta">Build of {e(status['built_at'][:19].replace('T', ' '))} UTC, {commit}{run} · dbt {e(status['dbt_version'])} ·
{status['elapsed_seconds']} s</p>
<div class="tiles">{totals_tiles(totals)}<div class="tile none"><div class="big">{totals['total']}</div><div class="muted">TOTAL</div></div></div>
<p class="muted">The numbers are the end-of-run line of <code>dbt build</code> (models, seeds, snapshots, tests and unit tests)
on a fresh DuckDB ingest of the same commit: the full project, not the slim prod build.</p>
<h2>Known warnings</h2>
{findings_table(status['findings'])}
<p class="muted">Warnings are source defects kept visible on purpose (<code>severity: warn</code>), never hidden.
A warning without a decision would show as <i>unexplained</i>.</p>
<h2>Source freshness</h2>
{freshness_line(status['freshness'])}
<h2>Data contracts</h2>
{contracts_table(status['contracts'])}
<h2>SLOs</h2>
{slo_cards(status['slos'])}
<p class="muted">Generated by the production deploy by
<a href="{REPO}/blob/main/.github/scripts/status_report.py">.github/scripts/status_report.py</a>.</p>
</main></body></html>
"""
