"""Data SLOs (D111): error budget and burn rate arithmetic, the report and page built
from daily events, the burn-rate email, and the targets agreeing everywhere (slo.yaml,
the ODPS descriptor, the workflow wiring)."""

import importlib.util
import json
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from alerting import slo

ROOT = Path(__file__).resolve().parents[1]
TODAY = date(2026, 11, 10)
CONFIG = yaml.safe_load((ROOT / "data-product" / "slo.yaml").read_text(encoding="utf-8"))


def load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def status(events: slo.Daily, target: float = 0.99, **kwargs: float) -> slo.Status:
    return slo.evaluate("quality", "Quality", target, events, TODAY, **kwargs)  # type: ignore[arg-type]


def days_ago(n: int) -> date:
    return TODAY - timedelta(days=n)


def test_sli_budget_and_burn_rate() -> None:
    # 200 events in the window, 2 bad: allowed 0.01 * 200 = 2.0 -> budget exactly spent, not over
    # last 7 days: 20 events, 1 bad -> bad share 5% / allowed 1% = burn rate 5x
    s = status([(days_ago(20), 150, 150), (days_ago(3), 29, 30), (days_ago(1), 19, 20)])
    assert (s.good, s.total, s.bad) == (198, 200, 2)
    assert s.sli == pytest.approx(0.99)
    assert s.allowed_bad == pytest.approx(2.0)
    assert s.budget_left == pytest.approx(0.0)
    assert s.burn_rate == pytest.approx((2 / 50) / 0.01)
    assert s.state == "burning"


def test_events_outside_the_window_do_not_count() -> None:
    s = status([(days_ago(28), 0, 10), (days_ago(27), 5, 5), (TODAY + timedelta(days=1), 0, 3)])
    assert (s.good, s.total) == (5, 5)  # day 28 ago is the 29th day back; tomorrow is not in yet
    assert len(s.daily) == 28 and s.daily[0]["day"] == days_ago(27).isoformat() and s.daily[-1]["day"] == TODAY.isoformat()


def test_ok_when_bad_events_stay_within_budget_and_burn_is_slow() -> None:
    s = status([(days_ago(i), 10, 10) for i in range(1, 27)] + [(days_ago(15), 9, 10)], target=0.9)
    assert s.state == "ok"
    assert s.burn_rate == 0  # events in the last 7 days, none bad


def test_exhausted_when_bad_events_exceed_the_allowance() -> None:
    # 3 bad of 100 against 1 allowed; the last 7 days are clean, so it is not burning but spent
    s = status([(days_ago(20), 97, 100)])
    assert s.state == "exhausted" and s.budget_left is not None and s.budget_left < 0
    assert s.burn_total == 0 and s.burn_rate is None


def test_no_data_is_not_a_perfect_score() -> None:
    s = status([])
    assert s.state == "no data" and s.sli is None and s.burn_rate is None and s.budget_left is None
    assert slo.alerts([s]) == []


def test_burn_rate_none_when_the_short_window_is_empty_but_the_long_one_is_not() -> None:
    s = status([(days_ago(20), 100, 100)])
    assert s.burn_rate is None and s.state == "ok"


def test_alert_threshold_is_inclusive() -> None:
    # 2 bad of 100 in the last 7 days at 99%: 2% / 1% = exactly 2x
    s = status([(days_ago(2), 98, 100)], burn_alert=2.0)
    assert s.burn_rate == pytest.approx(2.0) and s.state == "exhausted"  # 2 bad > 1 allowed as well
    only_burning = status([(days_ago(20), 1000, 1000), (days_ago(2), 98, 100)], burn_alert=2.0)
    assert only_burning.state == "burning"  # allowed 11, bad 2: within budget, burn 2x


@pytest.mark.parametrize("target", [0, 1, 1.5])
def test_target_must_leave_a_budget(target: float) -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        status([], target=target)


def test_same_day_events_are_summed() -> None:
    s = status([(days_ago(1), 1, 1), (days_ago(1), 0, 1)])
    assert (s.good, s.total) == (1, 2)
    assert [d for d in s.daily if d["total"]] == [{"day": days_ago(1).isoformat(), "good": 1, "total": 2}]


def test_email_lists_only_flagged_slos() -> None:
    burning = status([(days_ago(20), 1000, 1000), (days_ago(2), 98, 100)])
    healthy = status([(days_ago(2), 100, 100)])
    flagged = [s.as_dict(daily=False) for s in slo.alerts([burning, healthy])]
    assert len(flagged) == 1 and "daily" not in flagged[0]
    subject, html = slo.email(flagged, "[olist prod]", [("SLO page", "https://example.com/slo/")], "footer") or ("", "")
    assert subject == "[olist prod] SLO: Error budget burning: quality"
    assert "2.0x" in html and "target 99%" in html and "https://example.com/slo/" in html
    assert slo.email([], "[olist prod]", [], "") is None


# Report, page, notifier ---------------------------------------------------------------------

REPORT = load_module(ROOT / ".github" / "scripts" / "slo_report.py", "slo_report")
SITE = load_module(ROOT / "data-product" / "slo_site.py", "slo_site")


def runs(*items: tuple[str, str]) -> list[dict[str, str]]:
    return [{"conclusion": c, "createdAt": at, "url": "https://example.com"} for c, at in items]


def test_deploy_events_count_only_finished_successes_and_failures() -> None:
    events = REPORT.deploy_events(runs(
        ("success", "2026-11-09T08:00:00Z"), ("failure", "2026-11-09T09:00:00Z"),
        ("cancelled", "2026-11-09T10:00:00Z"), ("skipped", "2026-11-09T11:00:00Z"), ("success", "2026-11-08T23:30:00Z"),
    ))
    assert events == [(date(2026, 11, 8), 1, 1), (date(2026, 11, 9), 1, 2)]


def make_report() -> dict:
    rows = [
        (days_ago(2), "quality", 5, 6), (days_ago(1), "quality", 3, 3),
        (days_ago(1), "freshness", 1, 1), (days_ago(40), "quality", 0, 9),  # outside the window
        (days_ago(1), "other", 1, 1),  # not a configured SLO: ignored
    ]
    return REPORT.build_report(CONFIG, rows, runs(("success", "2026-11-09T08:00:00Z")), TODAY, "2026-11-10 06:00 UTC")


def test_report_has_every_configured_slo_and_the_flagged_ones() -> None:
    report = make_report()
    assert [s["key"] for s in report["slos"]] == list(CONFIG["slos"])
    by_key = {s["key"]: s for s in report["slos"]}
    assert (by_key["quality"]["good"], by_key["quality"]["total"]) == (8, 9)
    assert by_key["deploy_availability"]["total"] == 1
    assert [a["key"] for a in report["alerts"]] == ["quality"]  # 1 bad of 9 against 0.09 allowed
    assert all("daily" not in a for a in report["alerts"])
    json.dumps(report)  # serializable for data.json and the job output


def test_page_shows_every_slo_the_caveat_and_stays_escaped() -> None:
    page = SITE.render(make_report(), CONFIG)
    for meta in CONFIG["slos"].values():
        assert meta["title"] in page
    assert "Thin history" in page and "<svg" in page
    assert "2026-11-10 06:00 UTC" in page


def test_notify_slo_sends_only_when_flagged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.syspath_prepend(str(ROOT / ".github" / "scripts"))  # its `from notify_failure import`
    module = load_module(ROOT / ".github" / "scripts" / "notify_slo.py", "notify_slo")
    calls: list[str] = []
    monkeypatch.setattr(module, "send", lambda _uri, _to, subject, _html: calls.append(subject))
    for name, value in {
        "SMTP_CONN": "smtp://u:p@smtp.example.com:587?disable_ssl=true", "ALERT_EMAILS": "a@example.com",
        "GITHUB_REPOSITORY": "o/r", "GITHUB_RUN_ID": "1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SLO_ALERTS", "[]")
    module.main()
    assert calls == []
    monkeypatch.setenv("SLO_ALERTS", json.dumps(make_report()["alerts"]))
    module.main()
    assert calls == ["[olist prod] SLO: Error budget burning: quality"]


# The targets agree everywhere -----------------------------------------------------------------

DESCRIPTOR_PROPERTIES = {
    "freshness": "sloFreshness", "quality": "sloQuality", "deploy_availability": "sloDeployAvailability",
}


def test_descriptor_lists_the_targets_of_slo_yaml_on_every_output_port() -> None:
    product = yaml.safe_load((ROOT / "data-product" / "olist-commerce.odps.yaml").read_text(encoding="utf-8"))
    for port in product["outputPorts"]:
        props = {p["property"]: p["value"] for p in port.get("customProperties", [])}
        assert props["sloWindowDays"] == CONFIG["window_days"], port["name"]
        for key, prop in DESCRIPTOR_PROPERTIES.items():
            assert props[prop] == CONFIG["slos"][key]["target"], (port["name"], prop)
    assert any(m["name"] == "slo" and m["url"].endswith("/slo/") for m in product["managementPorts"])


def test_every_slo_has_a_definition_and_the_model_counts_the_dbt_ones() -> None:
    assert set(CONFIG["slos"]) == set(DESCRIPTOR_PROPERTIES)
    for meta in CONFIG["slos"].values():
        assert 0 < meta["target"] < 1 and meta["definition"] and meta["event"]
    model = (ROOT / "transform" / "models" / "marts" / "platform" / "fct_slo_daily.sql").read_text(encoding="utf-8")
    for key in ("quality", "freshness"):
        assert f"'{key}' as slo" in model


def test_prod_deploy_builds_the_model_every_time_and_mails_through_its_own_job() -> None:
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "ci-cd.yml").read_text(encoding="utf-8"))
    deploy, notify = workflow["jobs"]["deploy-prod"], workflow["jobs"]["notify-slo"]
    steps = {s.get("name"): s for s in deploy["steps"]}
    assert "fct_slo_daily" in steps["dbt build (job costs, target prod)"]["run"]
    assert "fct_slo_daily" in steps["dbt build (target prod)"]["run"].split("args=(", 1)[1].split(")", 1)[0]
    assert steps["SLO report"]["continue-on-error"] is True
    assert deploy["outputs"]["slo_alerts"] == "${{ steps.slo.outputs.alerts }}"
    assert "SMTP_CONN" not in json.dumps(deploy)
    assert notify["needs"] == "deploy-prod" and "id-token" not in notify["permissions"]
    (send,) = [s for s in notify["steps"] if s.get("run")]
    assert "${{" not in send["run"] and send["env"]["SLO_ALERTS"] == "${{ needs.deploy-prod.outputs.slo_alerts }}"
