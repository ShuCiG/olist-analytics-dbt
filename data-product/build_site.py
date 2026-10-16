"""Build the data product's page: an overview generated from the descriptor, the
consumer agreements and the contracts, plus the contracts' HTML catalog.

CD publishes it next to the dbt docs (GitHub Pages, /data-product/):

    uv run python data-product/build_site.py <output-dir>
"""

import html
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

PRODUCT_DIR = Path(__file__).resolve().parent
DOCS_URL = "../"  # the dbt docs are the site root


def load(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def e(value: Any) -> str:
    return html.escape(str(value))


def text(value: Any) -> str:
    """Escaped prose, `code` spans as <code>."""
    return re.sub(r"`([^`]+)`", r"<code></code>", e(value))


def build_catalog(out: Path) -> None:
    """`datacontract catalog`: one HTML page per contract, under catalog/contracts/."""
    exe = shutil.which("datacontract", path=str(Path(sys.executable).parent))
    assert exe, f"datacontract not found next to {sys.executable}"
    # Run from data-product/: the pattern matches recursively (contracts/*.odcs.yaml)
    subprocess.run(
        [exe, "catalog", "--files", "*.odcs.yaml", "--output", str(out / "catalog")],
        cwd=PRODUCT_DIR, check=True, capture_output=True,
    )


def contract_link(contract_id: str, contracts: dict[str, dict[str, Any]], files: dict[str, str]) -> str:
    c = contracts[contract_id]
    return f'<a href="catalog/contracts/{files[contract_id]}">{e(contract_id)} v{e(c["version"])}</a>'


def card(title: str, body: str, kind: str) -> str:
    return f'<div class="card {kind}"><div class="kind">{e(kind)}</div><h3>{title}</h3>{body}</div>'


def render(product: dict[str, Any], contracts: dict[str, dict[str, Any]], files: dict[str, str],
           agreements: list[dict[str, Any]]) -> str:
    owner = next(m for m in product["team"]["members"] if m.get("role") == "Product Owner")
    support = product["support"][0]
    desc = product["description"]

    inputs = "".join(
        card(e(p["name"]), f"<p>contract {contract_link(p['contractId'], contracts, files)}</p>"
             f"<p class=\"muted\">{text(contracts[p['contractId']]['description']['purpose'])}</p>", "input port")
        for p in product["inputPorts"]
    )
    outputs = "".join(
        card(e(p["name"]),
             (f"<p>contract {contract_link(p['contractId'], contracts, files)}</p>" if "contractId" in p
              else f"<p><a href=\"{e(p['authoritativeDefinitions'][0]['url'])}\">definitions</a></p>")
             + f"<p class=\"muted\">{text(p['description'])}</p>", "output port")
        for p in product["outputPorts"]
    )
    consumers = "".join(
        card(f'<a href="{e(a["consumer"]["url"])}">{e(a["name"])}</a>',
             f"<p>port <b>{e(a['provider']['outputPort'])}</b>, contract {e(a['provider']['contractId'])} "
             f"v{e(a['provider']['contractVersion'])}, server {e(a['provider']['server'])}</p>"
             f"<p>{sum(len(c) for c in a['uses'].values())} columns of {len(a['uses'])} tables"
             + (f", {len(a['metrics']['names'])} metrics" if a.get("metrics") else "") + "</p>"
             f"<p class=\"muted\">{text(a['purpose'])}</p>", "consumer")
        for a in agreements
    )
    rows = "".join(
        f"<tr><td>{contract_link(cid, contracts, files)}</td><td>{e(c['name'])}</td>"
        f"<td>{len(c['schema'])}</td><td>{e(', '.join(s['server'] for s in c['servers']))}</td>"
        f"<td>{e(', '.join(f'{s['property']} {s['value']}{s.get('unit', '')}' for s in c.get('slaProperties', [])))}</td></tr>"
        for cid, c in contracts.items()
    )
    management = "".join(
        f"<li><b>{e(m['name'])}</b> ({e(m['content'])}): "
        + (f'<a href="{e(m["url"])}">{e(m["url"])}</a> ' if "url" in m else "")
        + f"{text(m.get('description', ''))}</li>"
        for m in product["managementPorts"]
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(product['name'])} · data product</title>
<style>
:root {{ --bg:#fff; --fg:#1d232b; --muted:#5d6875; --line:#dde2e8; --card:#f6f8fa; --accent:#2f6fde; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#14181d; --fg:#e4e8ed; --muted:#9aa5b1; --line:#2c333b; --card:#1b2027; --accent:#7aa7ff; }} }}
body {{ margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 system-ui, sans-serif; }}
main {{ max-width:1100px; margin:0 auto; padding:24px 16px 48px; }}
a {{ color:var(--accent); }} h1 {{ margin:0 0 4px; }} h2 {{ margin-top:32px; border-bottom:1px solid var(--line); padding-bottom:4px; }}
.meta, .muted {{ color:var(--muted); }} .badge {{ border:1px solid var(--line); border-radius:999px; padding:1px 8px; margin-right:6px; }}
.flow {{ display:grid; grid-template-columns:repeat(3, 1fr); gap:16px; align-items:start; }}
.col > h4 {{ margin:0 0 8px; color:var(--muted); font-weight:600; text-transform:uppercase; font-size:12px; letter-spacing:.06em; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:8px; padding:12px 14px; margin-bottom:12px; }}
.card h3 {{ margin:0 0 6px; font-size:16px; }} .card p {{ margin:4px 0; }}
.kind {{ font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); }}
table {{ border-collapse:collapse; width:100%; }} td, th {{ border-bottom:1px solid var(--line); padding:6px 8px; text-align:left; vertical-align:top; }}
.scroll {{ overflow-x:auto; }} code {{ font-size:13px; background:var(--card); padding:0 3px; border-radius:3px; }}
@media (max-width:760px) {{ .flow {{ grid-template-columns:1fr; }} }}
</style></head>
<body><main>
<p class="meta"><a href="{DOCS_URL}">dbt docs</a> · data product</p>
<h1>{e(product['name'])}</h1>
<p class="meta"><span class="badge">{e(product['status'])}</span><span class="badge">v{e(product['version'])}</span>
<span class="badge">domain {e(product['domain'])}</span><span class="badge">{e(product['type'])}</span>
Product Owner <b>{e(owner['name'])}</b> · support: <a href="{e(support['url'])}">{e(support['channel'])}</a></p>
<p>{text(desc['purpose'])}</p>
<p class="muted"><b>Limitations.</b> {text(desc['limitations'])}</p>
<p class="muted"><b>Usage.</b> {text(desc['usage'])}</p>

<h2>Ports</h2>
<div class="flow">
  <div class="col"><h4>Input</h4>{inputs}</div>
  <div class="col"><h4>Output</h4>{outputs}</div>
  <div class="col"><h4>Consumers</h4>{consumers}</div>
</div>

<h2>Data contracts</h2>
<p class="muted">ODCS v3.1, tested on the data in CI (datacontract-cli). Full schema, quality rules and servers on each page.</p>
<div class="scroll"><table><tr><th>Contract</th><th>Name</th><th>Tables</th><th>Servers</th><th>SLA</th></tr>{rows}</table></div>

<h2>Management</h2>
<ul>{management}</ul>
<p class="muted">Generated from <a href="https://github.com/ShuCiG/olist-analytics-dbt/tree/main/data-product">data-product/</a>
(descriptor: Open Data Product Standard v1.1).</p>
</main></body></html>
"""


def build(out: Path) -> None:
    out = out.resolve()  # the catalog runs from data-product/, not from the caller's directory
    out.mkdir(parents=True, exist_ok=True)
    build_catalog(out)
    contract_paths = sorted((PRODUCT_DIR / "contracts").glob("*.odcs.yaml"))
    contracts = {load(p)["id"]: load(p) for p in contract_paths}
    files = {load(p)["id"]: p.name.replace(".yaml", ".html") for p in contract_paths}
    agreements = [load(p) for p in sorted((PRODUCT_DIR / "consumers").glob("*.yaml"))]
    page = render(load(PRODUCT_DIR / "olist-commerce.odps.yaml"), contracts, files, agreements)
    (out / "index.html").write_text(page, encoding="utf-8")


if __name__ == "__main__":
    build(Path(sys.argv[1]))
