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


def test_delays_stay_within_the_deadline() -> None:
    pauses = dbx.delays()
    assert pauses[:6] == [30] * 6 and set(pauses[6:]) == {120}
    assert sum(pauses) <= dbx.DEADLINE


def _run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, answers: list[tuple[str, str]]) -> dict[str, str]:
    monkeypatch.setenv("DATABRICKS_HOST", "dbc-1.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "out"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    seen: list[str] = []

    def probe(token: str, warehouse_id: str) -> tuple[str, str]:
        seen.append(warehouse_id)
        return answers.pop(0) if answers else (dbx.RETRY, CANT_CREATE)

    monkeypatch.setattr(dbx, "probe", probe)
    monkeypatch.setattr(dbx.time, "sleep", lambda s: None)
    dbx.main()
    assert set(seen) == {"abc123"}
    return {"out": (tmp_path / "out").read_text(), "attempts": str(len(seen))}


def test_main_ready_after_retries(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    result = _run(monkeypatch, tmp_path, [(dbx.RETRY, CANT_CREATE), (dbx.READY, "SUCCEEDED")])
    assert result == {"out": "available=true\n", "attempts": "2"}
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
