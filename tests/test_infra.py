"""Terraform guards that `terraform test` can't express (it sees attribute values,
not resource types or lifecycle settings): no service account keys in the code,
and the stateful resources can't be destroyed by a plan. The value checks live in
infra/tests/policy.tftest.hcl."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra"


def _resources() -> dict[str, str]:
    """`type.name` -> block body, for every top-level resource in infra/*.tf."""
    blocks: dict[str, str] = {}
    for tf in sorted(INFRA.glob("*.tf")):
        text = tf.read_text(encoding="utf-8")
        starts = [m for m in re.finditer(r'^resource "([\w-]+)" "([\w-]+)"', text, re.M)]
        for i, m in enumerate(starts):
            end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
            blocks[f"{m[1]}.{m[2]}"] = text[m.end() : end]
    return blocks


def test_resources_found() -> None:
    assert "google_bigquery_dataset.this" in _resources()


def test_no_service_account_keys() -> None:
    # A key resource writes the private key into the state in plain text (D46)
    keys = [r for r in _resources() if r.startswith(("google_service_account_key.", "google_service_account_access_token."))]
    assert not keys, f"service account keys stay out of Terraform: {keys}"


def test_stateful_resources_prevent_destroy() -> None:
    stateful = {r: body for r, body in _resources().items() if r.startswith(("google_bigquery_dataset.", "google_storage_bucket."))}
    assert stateful
    unprotected = [r for r, body in stateful.items() if not re.search(r"prevent_destroy\s*=\s*true", body)]
    assert not unprotected, f"datasets and buckets hold data; add lifecycle {{ prevent_destroy = true }}: {unprotected}"
