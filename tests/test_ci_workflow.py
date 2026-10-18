"""CI failure alerting: every job is watched, and a broken alert setup fails loudly."""

import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci-cd.yml"

_spec = importlib.util.spec_from_file_location("notify_failure", ROOT / ".github" / "scripts" / "notify_failure.py")
assert _spec and _spec.loader
notify_failure = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(notify_failure)


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


GOOD_URI = "smtp://user%40gmail.com:pw@smtp.gmail.com:587?disable_ssl=true"


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
