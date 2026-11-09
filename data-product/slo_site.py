"""Render the SLO page (/slo/ on GitHub Pages, D111) from the report that
.github/scripts/slo_report.py computes: one card per SLO with the SLI, the error
budget left, the burn rate and the daily history of the last 28 days.

Stdlib only; the page is one self-contained HTML file (inline CSS and SVG).
"""

import html
from typing import Any

PRODUCT_URL = "../data-product/"
ELEMENTARY_URL = "../elementary/"


def e(value: Any) -> str:
    return html.escape(str(value))


def percent(share: float | None, digits: int = 1) -> str:
    return "n/a" if share is None else f"{share * 100:.{digits}f}%"


def bars(daily: list[dict[str, Any]]) -> str:
    """Inline SVG, one bar per day: height = share of good events, full red-tinted
    bar when a day had bad events, grey stub when it had no events."""
    width, height, gap = 14, 48, 3
    parts = []
    for i, d in enumerate(daily):
        x = i * (width + gap)
        total, good = d["total"], d["good"]
        if not total:
            parts.append(
                f'<rect x="{x}" y="{height - 3}" width="{width}" height="3" class="none">'
                f"<title>{e(d['day'])}: no events</title></rect>"
            )
            continue
        share = good / total
        bar = max(3.0, share * height)
        cls = "good" if good == total else "bad"
        parts.append(
            f'<rect x="{x}" y="{height - bar:.1f}" width="{width}" height="{bar:.1f}" class="{cls}">'
            f"<title>{e(d['day'])}: {good} good of {total} ({percent(share)})</title></rect>"
        )
    chart_width = len(daily) * (width + gap) - gap
    return (
        f'<svg viewBox="0 0 {chart_width} {height}" width="100%" height="{height}" role="img" '
        f'preserveAspectRatio="none" aria-label="Share of good events per day, last {len(daily)} days">'
        + "".join(parts) + "</svg>"
    )


STATE_LABEL = {
    "ok": ("ok", "within budget"),
    "burning": ("burning", "burning too fast"),
    "exhausted": ("exhausted", "budget spent"),
    "no data": ("nodata", "no data"),
}


def card(s: dict[str, Any], slo_meta: dict[str, Any]) -> str:
    cls, label = STATE_LABEL[s["state"]]
    burn = "n/a" if s["burn_rate"] is None else f"{s['burn_rate']:.1f}x"
    left = "n/a" if s["budget_left"] is None else percent(s["budget_left"], 0)
    return f"""<section class="card">
<div class="head"><h3>{e(s['title'])}</h3><span class="state {cls}">{e(label)}</span></div>
<p class="muted">{e(slo_meta['definition'])}</p>
<div class="stats">
  <div><div class="big">{percent(s['sli'])}</div><div class="muted">SLI, {s['window_days']} days<br>target {percent(s['target'], 0)}</div></div>
  <div><div class="big">{left}</div><div class="muted">error budget left<br>{s['bad']} bad of {s['allowed_bad']:.1f} allowed</div></div>
  <div><div class="big">{burn}</div><div class="muted">burn rate, last {s['burn_days']} days<br>{s['burn_total'] - s['burn_good']} bad of {s['burn_total']}</div></div>
  <div><div class="big">{s['total']}</div><div class="muted">events ({e(slo_meta['event'])}s)</div></div>
</div>
{bars(s['daily'])}
<div class="axis muted"><span>{e(s['daily'][0]['day'])}</span><span>{e(s['daily'][-1]['day'])}</span></div>
</section>"""


def render(report: dict[str, Any], config: dict[str, Any]) -> str:
    """`report`: slo_report's data (generated_at, today, slos); `config`: data-product/slo.yaml."""
    cards = "".join(card(s, config["slos"][s["key"]]) for s in report["slos"])
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data SLOs · Olist commerce</title>
<style>
:root {{ --bg:#fff; --fg:#1d232b; --muted:#5d6875; --line:#dde2e8; --card:#f6f8fa; --accent:#2f6fde;
  --good:#2da44e; --bad:#cf222e; --warn:#9a6700; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#14181d; --fg:#e4e8ed; --muted:#9aa5b1; --line:#2c333b;
  --card:#1b2027; --accent:#7aa7ff; --good:#3fb950; --bad:#f85149; --warn:#d29922; }} }}
body {{ margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 system-ui, sans-serif; }}
main {{ max-width:1100px; margin:0 auto; padding:24px 16px 48px; }}
a {{ color:var(--accent); }} h1 {{ margin:0 0 4px; }} h3 {{ margin:0; font-size:17px; }}
.muted {{ color:var(--muted); font-size:13px; }} .meta {{ color:var(--muted); }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr)); gap:16px; margin-top:20px; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:8px; padding:14px 16px; }}
.head {{ display:flex; justify-content:space-between; align-items:center; gap:8px; }}
.state {{ border:1px solid var(--line); border-radius:999px; padding:1px 10px; font-size:12px; white-space:nowrap; }}
.state.ok {{ color:var(--good); border-color:var(--good); }}
.state.burning, .state.exhausted {{ color:var(--bad); border-color:var(--bad); }}
.stats {{ display:grid; grid-template-columns:1fr 1fr; gap:10px 12px; margin:10px 0 12px; }}
.big {{ font-size:22px; font-weight:600; }}
svg rect.good {{ fill:var(--good); }} svg rect.bad {{ fill:var(--bad); }} svg rect.none {{ fill:var(--line); }}
.axis {{ display:flex; justify-content:space-between; margin-top:2px; }}
.note {{ border-left:3px solid var(--warn); background:var(--card); padding:8px 12px; margin:16px 0; border-radius:0 6px 6px 0; }}
code {{ font-size:13px; background:var(--card); padding:0 3px; border-radius:3px; }}
</style></head>
<body><main>
<p class="meta"><a href="../">dbt docs</a> · <a href="{PRODUCT_URL}">data product</a> · <a href="{ELEMENTARY_URL}">Elementary report</a> · SLOs</p>
<h1>Data SLOs</h1>
<p>How reliable the Olist marts have been over the last {config['window_days']} days (to {e(report['today'])}):
three objectives, the error budget each one leaves and how fast it is being spent. The targets are
part of the <a href="{PRODUCT_URL}">data product descriptor</a> (output ports <code>marts</code> and <code>metrics</code>).</p>
<div class="note"><b>Thin history.</b> Production builds on every merge to <code>main</code>, a few times a day at most, and
the source is a static dump (D18). A window holds tens of events, not thousands: one bad event moves a
percentage visibly, and a quiet day has no event at all (grey stub). Read the trend, not the decimals.</div>
<div class="grid">{cards}</div>
<h2>Definitions</h2>
<ul>
<li><b>Error budget</b>: the bad events the target allows in the window, <code>(1 - target) × events</code>.
Left = 1 − bad ÷ allowed; below 0 the budget is overspent.</li>
<li><b>Burn rate</b>: the bad share over the last {config['burn_window_days']} days divided by the allowed share
(<code>1 - target</code>). 1x spends the budget exactly over the window.</li>
<li><b>Alert</b>: an email to the alert list when the burn rate reaches {config['burn_alert']:g}x or the window's budget is spent.</li>
<li>Freshness and quality come from Elementary's run history (dbt model <code>fct_slo_daily</code>);
deploy availability from the GitHub Actions runs of the CI/CD workflow on <code>main</code>.</li>
</ul>
<p class="muted">Generated {e(report['generated_at'])} by the production deploy from
<a href="https://github.com/ShuCiG/olist-analytics-dbt/blob/main/data-product/slo.yaml">data-product/slo.yaml</a>.</p>
</main></body></html>
"""
