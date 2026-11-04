"""Fault injection (D100): which data defects the pipeline's checks catch, on DuckDB.

Not part of `uv run pytest` (testpaths): ~100 s of baseline plus a dbt build per
scenario. CI job `fault injection`; locally `uv run pytest fault_injection`.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from fault_injection.runner import ROOT, prepare_baseline, run_scenario
from fault_injection.scenarios import SCENARIOS, Scenario

WORKERS = min(4, os.cpu_count() or 1)


@pytest.fixture(scope="session")
def outcomes(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, tuple[list[str], object]]:
    """All selected scenarios at once, in parallel; `-k` runs a few."""
    selected = [
        item.callspec.params["scenario"]  # type: ignore[attr-defined]
        for item in request.session.items
        if "scenario" in getattr(getattr(item, "callspec", None), "params", {})
    ]
    root = tmp_path_factory.mktemp("faults")
    baseline = root / "baseline"
    prepare_baseline(baseline)
    with ThreadPoolExecutor(WORKERS) as pool:
        # Short work dirs: dbt's compiled paths are long, Windows limits them
        runs = pool.map(lambda s: run_scenario(baseline, root / str(SCENARIOS.index(s)), s), selected)
        return {s.name: outcome for s, outcome in zip(selected, runs, strict=True)}


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.name)
def test_expected_reaction(scenario: Scenario, outcomes: dict[str, tuple[list[str], object]]) -> None:
    reactions, pinned = outcomes[scenario.name]
    for expected in scenario.expected:
        assert any(r.startswith(expected) for r in reactions), f"nothing like {expected!r} in {reactions}"
    if not scenario.expected:
        assert reactions == []
    if scenario.pinned:
        assert pinned == scenario.pinned[1]


def test_every_defect_class_is_in_the_readme_table() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    missing = [s.defect for s in SCENARIOS if f"| {s.defect} |" not in readme]
    assert not missing
