"""Run the CI checks that need no cloud, locally, before a push (D108).

Each check is a step of `.github/workflows/ci-cd.yml`, kept here with the command
exactly as CI writes it; `tests/test_ci_local.py` fails when the two drift apart.
Checks run in parallel lanes; the first failure stops the lanes from starting
anything new, and its output is printed with the CI job it stands for.

    uv run python .github/scripts/ci_local.py               # all checks (pre-push hook)
    uv run python .github/scripts/ci_local.py --only evaluator
    uv run python .github/scripts/ci_local.py --list

Not mirrored (CI only): the dbt builds on DuckDB, BigQuery and Databricks (fresh
ingest, cloud credentials), fault injection (~14-20 min), the Airflow image, and
`terraform validate` / `terraform test` (init would rewrite infra/.terraform, which
holds the owner's GCS backend).
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Check:
    key: str
    job: str  # job id in ci-cd.yml
    step: str  # step name in that job
    run: str  # one line of the step's `run`, exactly as in ci-cd.yml
    # Checks in one lane run one after another, lanes in parallel. Lane "dbt" = the
    # checks that write transform/target (sqlfluff's dbt templater, `dbt parse`)
    lane: str
    cwd: str = "."  # the step's working directory, relative to the repo root
    ci_only_args: tuple[str, ...] = ()  # dropped locally (CI output format)
    tool: str = ""  # binary that may be missing locally: the check is then skipped, loudly
    needs_dbt_packages: bool = False
    env: dict[str, str] = field(default_factory=dict)  # the job's env, as in ci-cd.yml


CHECKS: tuple[Check, ...] = (
    Check(
        "sqlfluff",
        "sqlfluff-lint",
        "sqlfluff lint",
        "uv run sqlfluff lint transform/models transform/tests --format github-annotation-native",
        lane="dbt",
        ci_only_args=("--format", "github-annotation-native"),
        needs_dbt_packages=True,
    ),
    Check(
        "data-product",
        "dbt-build",
        "Data product checks",
        "uv run pytest data-product -q",
        lane="dbt",
        needs_dbt_packages=True,
    ),
    Check("pytest", "python-tests", "pytest", "uv run pytest -q", lane="pytest"),
    Check("pyright", "pyright", "pyright", "uv run pyright", lane="light"),
    Check(
        "audit-reports",
        "python-tests",
        "No audit reports in git",
        "uv run python .claude/skills/audit/audit_tools.py check",
        lane="light",
    ),
    Check(
        "contract-versioning",
        "python-tests",
        "Data contract versioning (vs main)",
        "uv run python data-product/check_compatibility.py origin/main",
        lane="light",
    ),
    Check(
        "terraform-fmt",
        "terraform",
        "terraform fmt",
        "terraform fmt -check -recursive -diff",
        lane="light",
        cwd="infra",
        tool="terraform",
    ),
    Check(
        "evaluator",
        "dbt-project-evaluator",
        "dbt build (evaluator models + rule tests)",
        "uv run dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions",
        lane="evaluator",  # own target dir, see run_check
        cwd="transform",
        needs_dbt_packages=True,
        env={"DBT_PROJECT_EVALUATOR": "true"},
    ),
)


def local_args(check: Check) -> list[str]:
    """The CI command without its CI-only arguments."""
    args = shlex.split(check.run)
    drop = list(check.ci_only_args)
    for i in range(len(args) - len(drop) + 1):
        if drop and args[i : i + len(drop)] == drop:
            return args[:i] + args[i + len(drop) :]
    return args


def _run(args: list[str], cwd: Path, env: dict[str, str], out: list[str]) -> int:
    exe = shutil.which(args[0]) or args[0]  # Windows: finds uv.exe, terraform.exe
    proc = subprocess.run(
        [exe, *args[1:]],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out.append(proc.stdout)
    return proc.returncode


def run_check(check: Check, scratch: Path, out: list[str]) -> int | None:
    """Exit code, or None when skipped. Output goes to `out`."""
    if check.tool and not shutil.which(check.tool):
        return None
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", **check.env}
    if check.key == "evaluator":
        # A throwaway DuckDB file and target dir, as on the runner: the dev
        # database and transform/target (partial parse, manifest) stay untouched
        env["DBT_DUCKDB_PATH"] = str(scratch / "evaluator.duckdb")
        env["DBT_TARGET_PATH"] = str(scratch / "evaluator-target")
        env["DBT_LOG_PATH"] = str(scratch / "evaluator-logs")
    if check.key == "contract-versioning":
        # CI fetches main first; offline, compare with the last fetched main
        if _run(["git", "fetch", "--no-tags", "--quiet", "origin", "main"], ROOT, env, out):
            out.append("git fetch failed: comparing with the last fetched origin/main\n")
    return _run(local_args(check), ROOT / check.cwd, env, out)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="+", choices=[c.key for c in CHECKS], help="run these checks only")
    parser.add_argument("--list", action="store_true", help="print the checks and the CI steps they mirror")
    opts = parser.parse_args(argv)

    checks = [c for c in CHECKS if not opts.only or c.key in opts.only]
    if opts.list:
        for c in checks:
            print(f"{c.key:20} lane {c.lane:10} CI job {c.job!r}, step {c.step!r}: {' '.join(local_args(c))}")
        return 0

    if any(c.needs_dbt_packages for c in checks) and not (ROOT / "transform" / "dbt_packages").is_dir():
        print("transform/dbt_packages missing: running `dbt deps` (as CI does)", flush=True)
        if code := subprocess.run([shutil.which("uv") or "uv", "run", "dbt", "deps"], cwd=ROOT / "transform").returncode:
            return code

    lanes: dict[str, list[Check]] = {}
    for c in checks:
        lanes.setdefault(c.lane, []).append(c)
    failed: list[tuple[Check, int, str]] = []
    stop = threading.Event()
    lock = threading.Lock()
    started = time.monotonic()

    with tempfile.TemporaryDirectory(prefix="ci-local-") as tmp:

        def lane(items: list[Check]) -> None:
            for c in items:
                if stop.is_set():
                    return
                t, out = time.monotonic(), []
                code = run_check(c, Path(tmp), out)
                with lock:
                    if code is None:
                        print(f"SKIPPED {c.key}: `{c.tool}` not on PATH (CI still runs it)", flush=True)
                    elif code:
                        failed.append((c, code, "".join(out)))
                        stop.set()
                    else:
                        print(f"ok      {c.key} ({time.monotonic() - t:.0f} s)", flush=True)

        print(f"ci-local: {len(checks)} checks in {len(lanes)} lanes", flush=True)
        threads = [threading.Thread(target=lane, args=(items,)) for items in lanes.values()]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

    for c, code, output in failed:
        print(f"\n{'=' * 70}\n{c.key}: output\n{'=' * 70}\n{output.rstrip()}")
        print(
            f"\nFAILED: {c.key}, which CI runs as job {c.job!r}, step {c.step!r} (exit {code}).\n"
            "Fix it, or skip on purpose with `git push --no-verify`."
        )
    if failed:
        return failed[0][1]
    print(f"ci-local: {len(checks)} checks passed in {time.monotonic() - started:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
