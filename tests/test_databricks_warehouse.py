"""Databricks warehouse probe (.github/scripts/databricks_warehouse.py, D78): which
answers mean "wait", "go" or "our error"."""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("databricks_warehouse", ROOT / ".github" / "scripts" / "databricks_warehouse.py")
assert _spec and _spec.loader
dbx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dbx)

CANT_CREATE = "Cannot create the resource, please try again later."
# Weekly run 42572093544 (2026-11-09): the statement probe's answer, then the warehouse
# start endpoint's answer for the same blocked workspace (#262)
BARE_400 = {"error_code": "BAD_REQUEST", "message": "The request could not be processed by the warehouse."}
START_INACTIVE = {
    "error_code": "BAD_REQUEST",
    "message": CANT_CREATE,
    "details": [{
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "DENY_NEW_AND_EXISTING_RESOURCES",
        "domain": "resource-gatekeeper",
        "metadata": {"denyReason": "INACTIVE"},
    }],
}


def _statement(state: str, message: str | None = None) -> dict[str, Any]:
    status: dict[str, Any] = {"state": state}
    if message:
        status["error"] = {"error_code": "BAD_REQUEST", "message": message}
    return {"statement_id": "s1", "status": status}


@pytest.mark.parametrize(
    ("http_status", "body", "outcome"),
    [
        (200, _statement("SUCCEEDED"), dbx.READY),
        # The first scheduled run's error, as an HTTP answer and as a failed statement
        (400, {"error_code": "BAD_REQUEST", "message": CANT_CREATE}, dbx.RETRY),
        (200, _statement("FAILED", CANT_CREATE), dbx.RETRY),
        # Still starting after the wait timeout (cancelled by on_wait_timeout)
        (200, _statement("PENDING"), dbx.RETRY),
        (200, _statement("CANCELED"), dbx.RETRY),
        (503, {"error_code": "TEMPORARILY_UNAVAILABLE", "message": "try later"}, dbx.RETRY),
        (429, {}, dbx.RETRY),
        # Ours: credentials, grants, a wrong warehouse id, a broken statement
        (401, {"error_code": "UNAUTHENTICATED", "message": "Invalid token"}, dbx.ERROR),
        (403, {"error_code": "PERMISSION_DENIED", "message": "no CAN_USE"}, dbx.ERROR),
        (404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "warehouse not found"}, dbx.ERROR),
        (200, _statement("FAILED", "PARSE_SYNTAX_ERROR"), dbx.ERROR),
    ],
)
def test_classify(http_status: int, body: dict[str, Any], outcome: str) -> None:
    assert dbx.classify(http_status, body)[0] == outcome


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (START_INACTIVE, "INACTIVE"),
        ({"error_code": "BAD_REQUEST", "message": CANT_CREATE}, None),
        ({}, None),
        ({"error_code": "PERMISSION_DENIED", "message": "no CAN_MANAGE", "details": []}, None),
    ],
)
def test_deny_reason(body: dict[str, Any], reason: str | None) -> None:
    assert dbx.deny_reason(body) == reason


def test_block_check_only_on_answers_the_gatekeeper_gives() -> None:
    assert dbx.needs_block_check(*dbx.classify(400, BARE_400))
    assert dbx.needs_block_check(*dbx.classify(400, {"error_code": "BAD_REQUEST", "message": CANT_CREATE}))
    assert not dbx.needs_block_check(*dbx.classify(200, _statement("SUCCEEDED")))
    assert not dbx.needs_block_check(*dbx.classify(200, _statement("PENDING")))
    assert not dbx.needs_block_check(*dbx.classify(403, {"error_code": "PERMISSION_DENIED", "message": "x"}))


def test_delays_stay_within_the_deadline() -> None:
    pauses = dbx.delays()
    assert pauses[:6] == [30] * 6 and set(pauses[6:]) == {120}
    assert sum(pauses) <= dbx.DEADLINE


def _run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, answers: list[tuple[str, str]],
         block: str | None = None) -> dict[str, str]:
    monkeypatch.setenv("DATABRICKS_HOST", "dbc-1.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "out"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    seen: list[str] = []

    def probe(token: str, warehouse_id: str) -> tuple[str, str]:
        seen.append(warehouse_id)
        return answers.pop(0) if answers else (dbx.RETRY, CANT_CREATE)

    starts: list[str] = []

    def block_reason(token: str, warehouse_id: str) -> str | None:
        starts.append(warehouse_id)
        return block

    monkeypatch.setattr(dbx, "probe", probe)
    monkeypatch.setattr(dbx, "block_reason", block_reason)
    monkeypatch.setattr(dbx.time, "sleep", lambda s: None)
    try:
        dbx.main()
    finally:
        assert set(seen) == {"abc123"}
        # The start endpoint is asked at most once a run
        assert starts in ([], ["abc123"])
    return {"out": (tmp_path / "out").read_text(), "attempts": str(len(seen)), "starts": str(len(starts))}


def test_main_ready_after_retries(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    result = _run(monkeypatch, tmp_path, [(dbx.RETRY, CANT_CREATE), (dbx.READY, "SUCCEEDED")])
    assert result == {"out": "available=true\n", "attempts": "2", "starts": "1"}
    assert not (tmp_path / "summary").exists()


def test_main_unavailable_is_a_skip_not_a_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    result = _run(monkeypatch, tmp_path, [])
    assert result["out"] == "available=false\n"
    assert int(result["attempts"]) == len(dbx.delays()) + 1
    assert "Databricks unavailable" in (tmp_path / "summary").read_text()


def test_main_fails_on_our_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="HTTP 403"):
        _run(monkeypatch, tmp_path, [(dbx.ERROR, "HTTP 403 PERMISSION_DENIED: no CAN_USE")])
    assert (tmp_path / "out").read_text() == "available=false\n"


@pytest.mark.parametrize("first", [dbx.classify(400, BARE_400), (dbx.RETRY, CANT_CREATE)])
def test_main_fails_at_once_when_workspace_is_inactive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, first: tuple[str, str]
) -> None:
    with pytest.raises(SystemExit, match="blocked for inactivity.*sign in to the workspace UI"):
        _run(monkeypatch, tmp_path, [first], block="INACTIVE")
    assert (tmp_path / "out").read_text() == "available=false\n"
    assert not (tmp_path / "summary").exists()


def test_main_bare_400_without_block_fails_as_before(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="HTTP 400 BAD_REQUEST"):
        _run(monkeypatch, tmp_path, [dbx.classify(400, BARE_400)])


def test_main_ready_needs_no_start_call(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    result = _run(monkeypatch, tmp_path, [(dbx.READY, "SUCCEEDED")])
    assert result["starts"] == "0"
