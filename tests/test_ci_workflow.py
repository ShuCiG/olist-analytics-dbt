"""CI workflow guards: every job is watched by the failure email, a broken alert setup
fails loudly, and the prod key never reaches a PR job."""

import importlib.util
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci-cd.yml"

_spec = importlib.util.spec_from_file_location("notify_failure", ROOT / ".github" / "scripts" / "notify_failure.py")
assert _spec and _spec.loader
notify_failure = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(notify_failure)


_spec = importlib.util.spec_from_file_location("changed_paths", ROOT / ".github" / "scripts" / "changed_paths.py")
assert _spec and _spec.loader
changed_paths = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(changed_paths)


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def test_notify_failure_needs_every_job() -> None:
    jobs = _jobs()
    watched = set(jobs["notify-failure"]["needs"])
    missing = set(jobs) - watched - {"notify-failure"}
    assert not missing, f"add to notify-failure `needs`, or its failures send no email: {sorted(missing)}"
    assert watched <= set(jobs), f"unknown jobs in notify-failure `needs`: {sorted(watched - set(jobs))}"


def test_elementary_report_does_not_block_deploy() -> None:
    steps = {s.get("id"): s for s in _jobs()["deploy-prod"]["steps"]}
    assert steps["edr"]["continue-on-error"] is True


def _secrets(job: dict) -> set[str]:
    return set(re.findall(r"secrets\.(\w+)", yaml.safe_dump(job)))


MAIN_ONLY = "github.ref == 'refs/heads/main'"


def test_prod_key_only_in_main_jobs_behind_environment() -> None:
    jobs = _jobs()
    holders = {name for name, job in jobs.items() if "GCP_SA_KEY" in _secrets(job)}
    assert holders == {"deploy-prod"}
    environment = jobs["deploy-prod"]["environment"]
    assert (environment["name"] if isinstance(environment, dict) else environment) == "prod"
    assert MAIN_ONLY in jobs["deploy-prod"]["if"]


def test_pr_jobs_use_ci_key_only() -> None:
    for name, job in _jobs().items():
        if MAIN_ONLY in str(job.get("if", "")):
            continue
        assert "GCP_SA_KEY" not in _secrets(job), f"{name} runs on PRs: use GCP_SA_KEY_CI"
    assert "GCP_SA_KEY_CI" in _secrets(_jobs()["dbt-build-bigquery"])


def test_notify_failure_runs_base_branch_code_on_prs() -> None:
    checkout = _jobs()["notify-failure"]["steps"][0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"]["ref"].startswith("${{ github.event.pull_request.base.sha")


@pytest.mark.parametrize(
    ("paths", "build"),
    [
        (["README.md", "docs/DECISIONS.md", "airflow/README.md"], False),
        ([".github/ISSUE_TEMPLATE/task.yml", ".github/pull_request_template.md"], False),
        # dbt docs blocks: dbt docs reads them
        (["transform/models/_docs.md"], True),
        (["README.md", "transform/models/marts/fct_orders.sql"], True),
        (["docs/RUNNING.md", ".github/workflows/ci-cd.yml"], True),
        (["uv.lock"], True),
        ([], True),
    ],
)
def test_needs_build(paths: list[str], build: bool) -> None:
    assert changed_paths.needs_build(paths) is build


# Jobs that cost BigQuery bytes or minutes of image builds; they and their
# dependants skip when `changes` says docs only
GATED = {"dbt-project-evaluator", "airflow-dags", "dbt-build", "dbt-build-bigquery", "deploy-prod"}


def test_expensive_jobs_skip_docs_only_changes() -> None:
    jobs = _jobs()
    for name in GATED:
        needs = jobs[name]["needs"]
        assert "changes" in ([needs] if isinstance(needs, str) else needs), name
        assert "needs.changes.outputs.code == 'true'" in jobs[name]["if"], name
    for name in ("deploy-docs", "publish-airflow-image"):
        assert jobs[name]["needs"] == "deploy-prod"


def test_required_checks_are_not_path_filtered() -> None:
    # A workflow-level paths filter never starts the run: required checks hang on "Expected"
    on = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))[True]  # YAML 1.1 reads `on` as True
    for event in ("pull_request", "push"):
        assert not {"paths", "paths-ignore"} & set(on[event]), event


GOOD_URI ="smtp://user%40gmail.com:pw@smtp.gmail.com:587?disable_ssl=true"


@pytest.mark.parametrize(
    ("to", "uri", "reason"),
    [
        ([], GOOD_URI, "ALERT_EMAILS"),
        (["a@example.com"], "", "not set"),
        # Secret pasted with its .env name: no host, smtplib would never connect
        (["a@example.com"], f"SMTP_CONN={GOOD_URI}", "not an smtp://"),
        (["a@example.com"], "user:pw@smtp.gmail.com", "not an smtp://"),
    ],
)
def test_config_error(to: list[str], uri: str, reason: str) -> None:
    error = notify_failure.config_error(to, uri)
    assert error and reason in error


def test_config_ok() -> None:
    assert notify_failure.config_error(["a@example.com"], GOOD_URI) is None


def test_main_exits_1_without_secret(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    monkeypatch.setenv("ALERT_EMAILS", "a@example.com")
    monkeypatch.delenv("SMTP_CONN", raising=False)
    with pytest.raises(SystemExit) as exc:
        notify_failure.main()
    assert exc.value.code == 1
    assert "::error::" in capsys.readouterr().out
