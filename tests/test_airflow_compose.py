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
