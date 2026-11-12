"""airflow/reset_dbt_cache.py: partial parse caches dropped only when dbt files change (D114)."""

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("reset_dbt_cache", ROOT / "airflow" / "reset_dbt_cache.py")
assert _spec and _spec.loader
reset_dbt_cache = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reset_dbt_cache)


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "transform"
    (project / "models" / "marts").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: olist\n", encoding="utf-8")
    (project / "models" / "marts" / "_marts__models.yml").write_text("models: []\n", encoding="utf-8")
    return project


def _caches(cache: Path) -> list[Path]:
    paths = [
        cache / "cosmos" / "olist_elt_duckdb__dbt" / "partial_parse.msgpack",
        cache / "elementary_duckdb" / "partial_parse.msgpack",
        cache / "metric_drivers_bigquery" / "partial_parse.msgpack",
    ]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"cache")
    return paths


def test_first_run_without_stamp_drops_caches(tmp_path: Path) -> None:
    # Caches of unknown origin (written before this script existed) can be stale
    project, cache = _project(tmp_path), tmp_path / "dbt-cache"
    caches = _caches(cache)
    assert reset_dbt_cache.reset(project, cache) == sorted(caches)
    assert not any(p.exists() for p in caches)
    assert (cache / reset_dbt_cache.STAMP).read_text(encoding="utf-8").strip() == reset_dbt_cache.project_hash(project)


def test_unchanged_project_keeps_caches(tmp_path: Path) -> None:
    project, cache = _project(tmp_path), tmp_path / "dbt-cache"
    reset_dbt_cache.reset(project, cache)
    caches = _caches(cache)
    assert reset_dbt_cache.reset(project, cache) == []
    assert all(p.exists() for p in caches)


def test_yaml_edit_drops_caches(tmp_path: Path) -> None:
    project, cache = _project(tmp_path), tmp_path / "dbt-cache"
    reset_dbt_cache.reset(project, cache)
    caches = _caches(cache)
    (project / "models" / "marts" / "_marts__models.yml").write_text("models: [] # edit\n", encoding="utf-8")
    assert len(reset_dbt_cache.reset(project, cache)) == len(caches)
    assert not any(p.exists() for p in caches)


def test_rename_changes_hash(tmp_path: Path) -> None:
    project = _project(tmp_path)
    before = reset_dbt_cache.project_hash(project)
    (project / "models" / "marts" / "_marts__models.yml").rename(project / "models" / "marts" / "_other.yml")
    assert reset_dbt_cache.project_hash(project) != before


def test_generated_files_ignored(tmp_path: Path) -> None:
    # dbt writes these on every run: they must not drop the caches
    project = _project(tmp_path)
    before = reset_dbt_cache.project_hash(project)
    for rel in [
        "target/manifest.json",
        "target_airflow/manifest.json",
        "target_elementary_duckdb/partial_parse.msgpack",
        "dbt_packages/dbt_utils/macros/x.sql",
        "logs/dbt.log",
        "edr_target/run_results.json",
        ".user.yml",
    ]:
        path = project / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
    assert reset_dbt_cache.project_hash(project) == before


def test_missing_cache_dir_created(tmp_path: Path) -> None:
    project, cache = _project(tmp_path), tmp_path / "dbt-cache"
    assert reset_dbt_cache.reset(project, cache) == []
    assert (cache / reset_dbt_cache.STAMP).is_file()


def test_airflow_init_runs_the_reset() -> None:
    compose = yaml.safe_load((ROOT / "airflow" / "docker-compose.yml").read_text(encoding="utf-8"))
    script = "\n".join(compose["services"]["airflow-init"]["command"])
    call = script.index("reset_dbt_cache.py")
    # After the dev-mode parse, against the project and the cache volume the tasks use
    assert script.index("dbt parse") < call
    assert "--project /opt/project/transform --cache /opt/dbt-cache" in script


def test_script_is_in_the_image() -> None:
    # .dockerignore is an allowlist: without this line init fails on a missing file
    lines = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert "!airflow/reset_dbt_cache.py" in lines
