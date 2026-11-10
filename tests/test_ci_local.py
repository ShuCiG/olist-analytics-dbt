"""ci_local.py runs the same commands as the CI steps it stands for (D108)."""

import importlib.util
import re
import shlex
from pathlib import Path

import duckdb
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci-cd.yml"
PRE_COMMIT = ROOT / ".pre-commit-config.yaml"

_spec = importlib.util.spec_from_file_location("ci_local", ROOT / ".github" / "scripts" / "ci_local.py")
assert _spec and _spec.loader
ci_local = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ci_local)

# Jobs mirrored whole: each of their `run` steps is a check or listed here with the reason
NOT_MIRRORED = {
    "sqlfluff-lint": {"Install dependencies", "dbt deps (templater needs package macros)"},
    "pyright": {"Install dependencies"},
    "python-tests": {"Install dependencies"},
    "dbt-project-evaluator": {"Install dependencies", "dbt deps"},
    # init would rewrite infra/.terraform, which holds the owner's GCS backend
    "terraform": {
        "Provider cache directory",
        "terraform validate",
        "terraform test",
        "terraform validate + test (snowflake)",
    },
}


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def _step(job: dict, name: str) -> dict:
    steps = {s.get("name"): s for s in job["steps"]}
    assert name in steps, f"CI step {name!r} not found: rename it in ci_local.py too"
    return steps[name]


@pytest.mark.parametrize("check", ci_local.CHECKS, ids=lambda c: c.key)
def test_check_runs_the_ci_command(check) -> None:
    jobs = _jobs()
    assert check.job in jobs, f"CI job {check.job!r} not found"
    job = jobs[check.job]
    step = _step(job, check.step)
    lines = [line.strip() for line in step["run"].splitlines()]
    assert check.run in lines, f"CI step {check.step!r} now runs {step['run']!r}: update ci_local.py"
    cwd = step.get("working-directory") or job.get("defaults", {}).get("run", {}).get("working-directory", ".")
    assert check.cwd == cwd
    for key, value in check.env.items():
        assert str(job.get("env", {}).get(key)) == value, f"job env {key} differs from ci_local.py"


@pytest.mark.parametrize("job_id", sorted(NOT_MIRRORED))
def test_every_step_of_a_mirrored_job_is_covered(job_id: str) -> None:
    mirrored = {c.step for c in ci_local.CHECKS if c.job == job_id}
    steps = {s["name"] for s in _jobs()[job_id]["steps"] if "run" in s}
    new = steps - mirrored - NOT_MIRRORED[job_id]
    assert not new, f"new CI step in {job_id!r}: add it to ci_local.py, or to NOT_MIRRORED with the reason: {sorted(new)}"


def test_ci_only_args_exist_and_are_dropped() -> None:
    for check in ci_local.CHECKS:
        args = ci_local.local_args(check)
        if check.ci_only_args:
            assert len(args) == len(shlex.split(check.run)) - len(check.ci_only_args), check.key
        elif not check.subset:
            assert args == shlex.split(check.run)
    sqlfluff = next(c for c in ci_local.CHECKS if c.key == "sqlfluff")
    assert ci_local.local_args(sqlfluff) == ["uv", "run", "sqlfluff", "lint", "transform/models", "transform/tests"]


def test_hooks_call_the_script() -> None:
    config = yaml.safe_load(PRE_COMMIT.read_text(encoding="utf-8"))
    hooks = {h["id"]: h for repo in config["repos"] for h in repo["hooks"]}
    assert "pre-push" in config["default_install_hook_types"]
    assert config["default_stages"] == ["pre-commit"], "other hooks must not rerun on push"
    for hook_id in ("trailing-whitespace", "end-of-file-fixer", "check-added-large-files"):
        assert hooks[hook_id]["stages"] == ["pre-commit"], f"{hook_id}: its manifest stages include pre-push"

    push = hooks["ci-local"]
    assert push["stages"] == ["pre-push"] and push["always_run"] and not push["pass_filenames"]
    assert push["entry"] == "uv run python .github/scripts/ci_local.py"

    evaluator = hooks["dbt-project-evaluator"]
    assert evaluator["entry"] == "uv run python .github/scripts/ci_local.py --only evaluator"
    pattern = evaluator["files"]
    for path in (
        "transform/models/marts/fct_orders.sql",
        "transform/models/marts/_marts.yml",
        "transform/tests/assert_x.sql",
        "transform/seeds/dbt_project_evaluator_exceptions.csv",
        "transform/dbt_project.yml",
        "transform/packages.yml",
    ):
        assert re.search(pattern, path), path
    for path in ("transform/macros/x.sql", "transform/seeds/other.csv", "ingestion/pipeline.py", "README.md"):
        assert not re.search(pattern, path), path


def test_subset_lies_inside_the_ci_path() -> None:
    # A narrowed check runs files the CI step also runs, nothing else
    for check in (c for c in ci_local.CHECKS if c.subset or c.subset_of):
        ci_args = shlex.split(check.run)
        assert check.subset and check.subset_of in ci_args, check.key
        for path in check.subset:
            assert path.startswith(check.subset_of.rstrip("/") + "/"), path
            assert (ROOT / path).is_file(), path
        i = ci_args.index(check.subset_of)
        assert ci_local.local_args(check) == [*ci_args[:i], *check.subset, *ci_args[i + 1 :]]
    consumer = next(c for c in ci_local.CHECKS if c.key == "dashboard-consumer")
    assert ci_local.local_args(consumer) == ["uv", "run", "pytest", "dashboard/tests/test_consumer.py", "-q"]


def test_dashboard_check_skips_without_built_marts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = next(c for c in ci_local.CHECKS if c.key == "dashboard-consumer")
    db = tmp_path / "olist.duckdb"
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db))
    monkeypatch.delenv("MARTS_DATASET", raising=False)
    assert "no DuckDB file" in (ci_local.skip_reason(consumer) or "")

    tables = sorted(yaml.safe_load(ci_local.AGREEMENT.read_text(encoding="utf-8"))["uses"])
    with duckdb.connect(str(db)) as con:
        con.execute("create schema marts")
        for name in tables[:-1]:
            con.execute(f"create table marts.{name} (x int)")
    assert f"has no marts.{tables[-1]}" in (ci_local.skip_reason(consumer) or "")

    with duckdb.connect(str(db)) as con:
        con.execute(f"create table marts.{tables[-1]} (x int)")
    assert ci_local.skip_reason(consumer) is None
