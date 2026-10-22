"""CI workflow guards: every job is watched by the failure email, a broken alert setup
fails loudly, and the prod identity never reaches a PR job."""

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


def test_prod_elementary_step_keeps_history_and_a_full_fallback() -> None:
    steps = [s.get("name", "") for s in _jobs()["deploy-prod"]["steps"]]
    step = _jobs()["deploy-prod"]["steps"][steps.index("dbt run (Elementary tables, target prod)")]
    assert steps.index("Download prod manifest (last prod deploy)") < steps.index(step["name"])
    assert "--full-refresh" not in step["run"], "Elementary's tables hold the run and test history (D38)"
    assert "--select package:elementary\n" in step["run"], "no prod manifest: all of Elementary's models must run"


def _secrets(job: dict) -> set[str]:
    return set(re.findall(r"secrets\.(\w+)", yaml.safe_dump(job)))


MAIN_ONLY = "github.ref == 'refs/heads/main'"
PROD_SA = "dbt-runner@"
CI_SA = "olist-ci@"


def _gcp_auth(job: dict) -> list[dict]:
    return [s for s in job.get("steps", []) if s.get("uses", "").startswith("google-github-actions/auth@")]


def test_no_gcp_key_secrets() -> None:
    # Keys were replaced by Workload Identity Federation (D57)
    for name, job in _jobs().items():
        assert not {s for s in _secrets(job) if s.startswith("GCP_SA_KEY")}, f"{name} reads a GCP key secret"
        for step in _gcp_auth(job):
            assert "credentials_json" not in step["with"], f"{name}: authenticate with workload_identity_provider"


def test_gcp_auth_uses_federation() -> None:
    jobs = _jobs()
    users = {name for name, job in jobs.items() if _gcp_auth(job)}
    assert users == {"dbt-build-bigquery", "deploy-prod"}
    for name in users:
        (step,) = _gcp_auth(jobs[name])
        assert step["with"]["workload_identity_provider"] == "${{ env.GCP_WIF_PROVIDER }}"
        assert jobs[name]["permissions"].get("id-token") == "write", f"{name} needs id-token: write for OIDC"


def test_prod_identity_only_in_main_job_behind_environment() -> None:
    jobs = _jobs()
    holders = {name for name, job in jobs.items() if PROD_SA in yaml.safe_dump(job)}
    assert holders == {"deploy-prod"}
    environment = jobs["deploy-prod"]["environment"]
    # GCP trusts dbt-runner only for the prod environment's OIDC subject (infra/wif.tf)
    assert (environment["name"] if isinstance(environment, dict) else environment) == "prod"
    assert MAIN_ONLY in jobs["deploy-prod"]["if"]


def test_pr_jobs_use_ci_identity_only() -> None:
    for name, job in _jobs().items():
        if MAIN_ONLY in str(job.get("if", "")):
            continue
        assert PROD_SA not in yaml.safe_dump(job), f"{name} runs on PRs: use olist-ci"
    (step,) = _gcp_auth(_jobs()["dbt-build-bigquery"])
    assert step["with"]["service_account"].startswith(CI_SA)


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


DATABRICKS = ROOT / ".github" / "workflows" / "databricks.yml"


def _databricks() -> dict:
    return yaml.safe_load(DATABRICKS.read_text(encoding="utf-8"))


def test_databricks_runs_on_main_only() -> None:
    # The service principal's secret is in environment databricks-ci (main only, D58):
    # no pull_request trigger, so PR code never runs next to it
    workflow = _databricks()
    triggers = workflow[True]  # PyYAML reads the `on:` key as boolean True
    assert set(triggers) == {"schedule", "workflow_dispatch"}
    job = workflow["jobs"]["dbt-build-databricks"]
    assert job["environment"] == "databricks-ci"
    assert MAIN_ONLY in job["if"]
    holders = {name for name, j in workflow["jobs"].items() if "DATABRICKS_CLIENT_SECRET" in _secrets(j)}
    assert holders == {"dbt-build-databricks"}


def test_databricks_cleans_up_and_alerts() -> None:
    jobs = _databricks()["jobs"]
    steps = {s.get("name"): s for s in jobs["dbt-build-databricks"]["steps"]}
    assert steps["Drop CI schemas"]["if"] == "always()"
    assert steps["Drop guard refuses non-CI schemas"]["env"]["DBT_CI_SCHEMA"] == "marts_guard_check"
    assert jobs["dbt-build-databricks"]["env"]["DBT_CI_SCHEMA"].startswith("ci_")
    assert set(jobs["notify-failure"]["needs"]) == set(jobs) - {"notify-failure"}


DRIFT = ROOT / ".github" / "workflows" / "drift.yml"


def test_drift_check_is_read_only_and_alerts() -> None:
    # terraform-plan is trusted only for environment drift-check from main (infra/wif.tf, D60)
    workflow = yaml.safe_load(DRIFT.read_text(encoding="utf-8"))
    assert set(workflow[True]) == {"schedule", "workflow_dispatch"}
    jobs = workflow["jobs"]
    job = jobs["terraform-plan"]
    assert job["environment"] == "drift-check"
    assert MAIN_ONLY in job["if"]
    (auth,) = _gcp_auth(job)
    assert auth["with"]["service_account"].startswith("terraform-plan@")
    assert auth["with"]["workload_identity_provider"] == "${{ env.GCP_WIF_PROVIDER }}"
    assert workflow["env"]["GCP_WIF_PROVIDER"] == yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["env"]["GCP_WIF_PROVIDER"]
    for name, j in jobs.items():
        assert PROD_SA not in yaml.safe_dump(j), f"{name}: the drift check must not use the prod writer"
    # The IAM comparison (D62) runs as the same read-only account, in the same environment
    iam = jobs["iam-bindings"]
    assert iam["environment"] == "drift-check" and MAIN_ONLY in iam["if"]
    (auth,) = _gcp_auth(iam)
    assert auth["with"]["service_account"].startswith("terraform-plan@")
    assert "apply" not in yaml.safe_dump(iam)
    plan = next(s["run"] for s in job["steps"] if s.get("name") == "terraform plan")
    # Read-only run: no state lock, drift (exit code 2) fails the job, nothing applied
    assert "-lock=false" in plan and "-detailed-exitcode" in plan and 'exit "$code"' in plan
    assert "apply" not in yaml.safe_dump(job)
    assert set(jobs["notify-failure"]["needs"]) == set(jobs) - {"notify-failure"}


def test_schema_email_after_prod_load_without_the_prod_job_holding_smtp() -> None:
    # New source columns in prod raw (D61): own job, keeps the deploy green
    jobs = _jobs()
    ingest = next(s for s in jobs["deploy-prod"]["steps"] if s.get("id") == "ingest")
    assert "new_columns" in ingest["run"] and "$GITHUB_OUTPUT" in ingest["run"]
    assert jobs["deploy-prod"]["outputs"]["new_columns"] == "${{ steps.ingest.outputs.new_columns }}"
    assert "SMTP_CONN" not in _secrets(jobs["deploy-prod"])
    job = jobs["notify-schema"]
    assert job["needs"] == "deploy-prod"
    assert job["if"].startswith("always()") and "!= '{}'" in job["if"]
    assert "id-token" not in job["permissions"]
    (step,) = [s for s in job["steps"] if s.get("run")]
    # Column names reach the script through env, never inlined into `run`
    assert "${{" not in step["run"]
    assert step["env"]["NEW_COLUMNS"] == "${{ needs.deploy-prod.outputs.new_columns }}"


def _notify_schema(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(ROOT / ".github" / "scripts"))  # its `from notify_failure import`
    spec = importlib.util.spec_from_file_location("notify_schema", ROOT / ".github" / "scripts" / "notify_schema.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(("new_columns", "sent"), [('{"orders": ["order_channel"]}', True), ("{}", False)])
def test_notify_schema_sends_only_for_new_columns(
    monkeypatch: pytest.MonkeyPatch, new_columns: str, sent: bool
) -> None:
    module = _notify_schema(monkeypatch)
    calls: list[str] = []
    monkeypatch.setattr(module, "send", lambda _uri, _to, subject, _html: calls.append(subject))
    for name, value in {
        "NEW_COLUMNS": new_columns,
        "SMTP_CONN": "smtp://u:p@smtp.example.com:587?disable_ssl=true",
        "ALERT_EMAILS": "a@example.com",
        "GITHUB_REPOSITORY": "o/r",
        "GITHUB_RUN_ID": "1",
    }.items():
        monkeypatch.setenv(name, value)
    module.main()
    assert calls == (["[olist prod] SCHEMA: 1 new source column in orders"] if sent else [])
