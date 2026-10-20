"""Local Airflow stack (airflow/docker-compose*.yml): no login, so loopback-only ports."""

from pathlib import Path

import pytest
import yaml

_AIRFLOW = Path(__file__).resolve().parents[1] / "airflow"
_COMPOSE_FILES = sorted(_AIRFLOW.glob("docker-compose*.yml"))


def _published_ports() -> list[tuple[str, str, str]]:
    found = []
    for path in _COMPOSE_FILES:
        services = yaml.safe_load(path.read_text(encoding="utf-8")).get("services") or {}
        for name, service in services.items():
            for port in (service or {}).get("ports") or []:
                found.append((path.name, name, str(port)))
    return found


def test_compose_files_found() -> None:
    assert [p.name for p in _COMPOSE_FILES] == ["docker-compose.dev.yml", "docker-compose.yml"]
    assert _published_ports(), "no published ports: the check below would pass vacuously"


@pytest.mark.parametrize(("file", "service", "port"), _published_ports())
def test_ports_bound_to_loopback(file: str, service: str, port: str) -> None:
    # "8080:8080" binds every interface: anyone on the network gets an admin UI
    assert port.startswith("127.0.0.1:"), f"{file} {service}: publish as 127.0.0.1:{port}"


def test_cosmos_cache_on_a_volume() -> None:
    # Default /tmp/cosmos is the container's filesystem: every recreated container
    # (new image) paid a full dbt parse per call of its first run (D55)
    common = yaml.safe_load((_AIRFLOW / "docker-compose.yml").read_text(encoding="utf-8"))[
        "x-airflow-common"
    ]
    cache_dir = common["environment"]["AIRFLOW__COSMOS__CACHE_DIR"]
    mounts = [v.split(":")[1] for v in common["volumes"]]
    assert any(cache_dir.startswith(m.rstrip("/") + "/") for m in mounts), cache_dir
