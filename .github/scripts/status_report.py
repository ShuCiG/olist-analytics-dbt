"""Status page of the data product (D125): reads the files of the full DuckDB `dbt build`
(CI job `dbt build`, artifact `status-inputs`), the data contract results and the SLO
report, writes <out>/index.html and <out>/data.json for GitHub Pages (/status/).

    uv run python .github/scripts/status_report.py --inputs status-inputs --out _site/status \
        --slo _site/slo/data.json

Inputs folder: run_results.json, manifest.json, optional sources.json and
contract-*.json (`datacontract ci --output-format json`). Commit and run URL come from
the GitHub Actions environment (GITHUB_SHA, GITHUB_SERVER_URL, GITHUB_REPOSITORY, GITHUB_RUN_ID).
"""

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alerting import status  # noqa: E402


def load_site():
    """data-product/ has a hyphen: load status_site.py by path."""
    path = ROOT / "data-product" / "status_site.py"
    spec = importlib.util.spec_from_file_location("status_site", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def collect(inputs: Path, slo_path: Path | None, env: dict[str, str]) -> dict[str, Any]:
    run_results = read_json(inputs / "run_results.json")
    manifest = read_json(inputs / "manifest.json")
    sources = read_json(inputs / "sources.json") if (inputs / "sources.json").exists() else None
    contracts = [status.contract(read_json(p)) for p in sorted(inputs.glob("contract-*.json"))]
    slos = read_json(slo_path)["slos"] if slo_path and slo_path.exists() else []
    meta: dict[str, Any] = {"sha": env.get("GITHUB_SHA", "")}
    if env.get("GITHUB_RUN_ID"):
        meta["run_url"] = f"{env['GITHUB_SERVER_URL']}/{env['GITHUB_REPOSITORY']}/actions/runs/{env['GITHUB_RUN_ID']}"
    return status.build_status(run_results, manifest, contracts, sources, slos, meta)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--slo", type=Path, help="data.json of the SLO report (slo_report.py)")
    args = parser.parse_args()

    report = collect(args.inputs, args.slo, dict(os.environ))
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "data.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    (args.out / "index.html").write_text(load_site().render(report), encoding="utf-8")

    t = report["totals"]
    print(
        f"Status: {report['overall']}. dbt build PASS={t['pass']} WARN={t['warn']} ERROR={t['error']} "
        f"SKIP={t['skip']} NO-OP={t['no-op']} TOTAL={t['total']}; {len(report['findings'])} finding(s), "
        f"{len(report['contracts'])} contract(s)"
    )


if __name__ == "__main__":
    main()
