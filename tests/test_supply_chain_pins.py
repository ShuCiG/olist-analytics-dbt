"""Third-party code in CI and the Airflow image is pinned to content, not to a movable tag."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def _uses() -> list[tuple[str, str]]:
    return [
        (path.name, ref)
        for path in WORKFLOWS
        for ref in re.findall(r"^\s*-?\s*uses:\s*(\S+)", path.read_text(encoding="utf-8"), re.M)
    ]


def test_workflows_found() -> None:
    assert _uses(), "no `uses:` found: the pin check would pass vacuously"


@pytest.mark.parametrize(("file", "ref"), _uses())
def test_actions_pinned_to_commit_sha(file: str, ref: str) -> None:
    # Local actions (./...) are part of this repo
    assert ref.startswith("./") or re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", ref), (
        f"{file}: pin {ref} to a full commit SHA (keep the version in a comment)"
    )


def test_airflow_base_images_pinned_by_digest() -> None:
    dockerfile = (ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    images = re.findall(r"^FROM\s+(\S+)", dockerfile, re.M) + re.findall(r"--from=(\S+)", dockerfile)
    assert images
    for image in images:
        assert re.search(r"@sha256:[0-9a-f]{64}$", image), f"pin {image} by digest"


def test_edr_installed_from_hashed_lock() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci-cd.yml").read_text(encoding="utf-8")
    assert "--require-hashes" in workflow and ".github/edr-requirements.txt" in workflow
    assert "uvx" not in workflow, "uvx resolves dependencies at run time"
    lock = (ROOT / ".github" / "edr-requirements.txt").read_text(encoding="utf-8")
    packages = re.findall(r"^([A-Za-z0-9_.-]+)==", lock, re.M)
    assert packages and lock.count("--hash=sha256:") >= len(packages)


def test_airflow_edr_installed_from_hashed_lock() -> None:
    dockerfile = (ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "--require-hashes" in dockerfile and ".github/edr-requirements.txt" in dockerfile


def test_edr_lock_matches_project_dbt_versions() -> None:
    # The Airflow report opens a DuckDB file the project's duckdb wrote: a newer
    # storage version can't be read by an older duckdb (and dbt should match too)
    lock = (ROOT / ".github" / "edr-requirements.txt").read_text(encoding="utf-8")
    edr = dict(re.findall(r"^([A-Za-z0-9_.-]+)==(\S+)", lock, re.M))
    uv_lock = (ROOT / "uv.lock").read_text(encoding="utf-8")
    project = dict(re.findall(r'^name = "([^"]+)"\nversion = "([^"]+)"', uv_lock, re.M))
    for package in ("dbt-core", "dbt-bigquery", "dbt-duckdb", "duckdb"):
        assert edr[package] == project[package], f"{package}: edr {edr[package]}, uv.lock {project[package]}"
