"""check_new_sources.py: a raw source table prod `raw` lacks blocks the push (#353)."""

import importlib.util
import io
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("check_new_sources", ROOT / ".github" / "scripts" / "check_new_sources.py")
assert _spec and _spec.loader
cns = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cns)

RAW = """
version: 2
sources:
  - name: olist
    schema: "{{ env_var('DBT_RAW_SCHEMA', 'raw') }}"
    tables:
      - name: orders
      - name: weather_daily
  - name: bigquery
    schema: region-eu
    tables:
      - name: jobs
"""
OLD = RAW.replace("      - name: weather_daily\n", "")


def test_raw_tables_reads_only_sources_that_default_to_raw() -> None:
    assert cns.raw_tables(RAW) == {"orders", "weather_daily"}
    assert cns.raw_tables("version: 2\n") == set()
    plain = "sources:\n  - name: raw\n    tables:\n      - name: t\n"
    assert cns.raw_tables(plain) == {"t"}


def test_new_tables_against_the_base() -> None:
    path = "transform/models/staging/olist/_olist__sources.yml"
    assert cns.new_tables({path: OLD}, {path: RAW}) == {"weather_daily"}
    assert cns.new_tables({path: RAW}, {path: RAW}) == set()
    assert cns.new_tables({path: None}, {path: RAW}) == {"orders", "weather_daily"}  # new file


def run(monkeypatch: pytest.MonkeyPatch, ask) -> tuple[int, str, str]:
    path = "transform/models/staging/olist/_olist__sources.yml"
    monkeypatch.setattr(cns, "git", lambda *a: type("R", (), {"returncode": 0, "stdout": OLD})())
    monkeypatch.setattr(cns, "source_files", lambda: {path: RAW})
    out, err = io.StringIO(), io.StringIO()
    return cns.check("origin/main", ask, out, err), out.getvalue(), err.getvalue()


def test_missing_in_prod_blocks_with_the_load_command(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[set[str]] = []

    def ask(tables: set[str]) -> list[str]:
        asked.append(tables)
        return sorted(tables)

    code, _, err = run(monkeypatch, ask)
    assert code == 1
    assert asked == [{"weather_daily"}]  # only the new table is asked
    assert "raw.weather_daily" in err and cns.LOAD_COMMAND in err


def test_loaded_in_prod_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    code, out, _ = run(monkeypatch, lambda tables: [])
    assert code == 0 and "exist in prod raw" in out


def test_unreadable_prod_warns_and_does_not_block(monkeypatch: pytest.MonkeyPatch) -> None:
    def ask(tables: set[str]) -> list[str]:
        raise cns.Unreadable("DefaultCredentialsError: no ADC")

    code, _, err = run(monkeypatch, ask)
    assert code == 0 and "WARNING" in err and "weather_daily" in err


def test_no_new_table_never_asks_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cns, "git", lambda *a: type("R", (), {"returncode": 0, "stdout": RAW})())
    monkeypatch.setattr(cns, "source_files", lambda: {"p.yml": RAW})

    def ask(tables: set[str]) -> list[str]:
        raise AssertionError("prod must not be asked")

    assert cns.check("origin/main", ask, io.StringIO(), io.StringIO()) == 0


def test_no_base_ref_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cns, "git", lambda *a: type("R", (), {"returncode": 1, "stdout": ""})())
    err = io.StringIO()
    assert cns.check("origin/nope", lambda t: ["x"], io.StringIO(), err) == 0
    assert "skipped" in err.getvalue()
