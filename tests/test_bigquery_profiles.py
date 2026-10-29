"""Every BigQuery target in profiles.yml carries the per-query cost cap."""

from pathlib import Path

import yaml

_PROFILES = Path(__file__).resolve().parents[1] / "transform" / "profiles.yml"
_CAP = 1_000_000_000


def test_bigquery_outputs_cap_bytes_billed() -> None:
    profiles = yaml.safe_load(_PROFILES.read_text(encoding="utf-8"))
    outputs = {
        f"{profile}.{name}": output
        for profile, body in profiles.items()
        for name, output in body["outputs"].items()
        if output["type"] == "bigquery"
    }
    assert len(outputs) == 5
    for name, output in outputs.items():
        assert output.get("maximum_bytes_billed") == _CAP, name
