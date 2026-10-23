""".github/scripts/sa_keys.py (D63): key age and keys on accounts that should have
none, on fake API responses (no GCP calls)."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("sa_keys", ROOT / ".github" / "scripts" / "sa_keys.py")
assert _spec and _spec.loader
sa_keys = importlib.util.module_from_spec(_spec)
sys.modules["sa_keys"] = sa_keys  # dataclasses resolve annotations through it
_spec.loader.exec_module(sa_keys)

P = "olist-analytics-dbt-472315"
NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
CONFIG = {"max_age_days": 90, "accounts": {"dbt-runner": "local Airflow", "dashboard-reader": "Streamlit"}}
IAM = f"https://iam.googleapis.com/v1/projects/{P}/serviceAccounts"


def email(account: str) -> str:
    return f"{account}@{P}.iam.gserviceaccount.com"


def key(account: str, key_id: str, age_days: int, disabled: bool = False) -> dict[str, Any]:
    created = (NOW - timedelta(days=age_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "name": f"projects/{P}/serviceAccounts/{email(account)}/keys/{key_id}",
        "validAfterTime": created,
        "keyType": "USER_MANAGED",
        **({"disabled": True} if disabled else {}),
    }


def api(keys: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Two pages of accounts (dbt-runner | dashboard-reader, terraform-plan) and their keys."""
    responses: dict[str, Any] = {
        IAM: {"accounts": [{"email": email("dbt-runner")}, {"email": email("dashboard-reader")}], "nextPageToken": "t"},
        f"{IAM}?pageToken=t": {"accounts": [{"email": email("terraform-plan")}]},
    }
    for account in ("dbt-runner", "dashboard-reader", "terraform-plan"):
        responses[f"{IAM}/{email(account)}/keys?keyTypes=USER_MANAGED"] = {"keys": keys[account]} if keys.get(account) else {}
    return responses


class FakeSession:
    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses

    def get(self, url: str):
        body = self.responses[url]

        class Resp:
            def raise_for_status(self) -> None: ...

            def json(self) -> Any:
                return body

        return Resp()


def check(keys: dict[str, list[dict[str, Any]]]) -> list[str]:
    accounts, found = sa_keys.read_keys(FakeSession(api(keys)), P)
    assert accounts == ["dashboard-reader", "dbt-runner", "terraform-plan"]
    return sa_keys.findings(found, CONFIG, NOW)


def test_fresh_keys_and_keyless_accounts_pass() -> None:
    assert check({"dbt-runner": [key("dbt-runner", "a1", 10)], "dashboard-reader": [key("dashboard-reader", "b1", 90)]}) == []


def test_key_over_the_limit_names_account_key_and_use() -> None:
    found = check({"dbt-runner": [key("dbt-runner", "new", 5), key("dbt-runner", "old", 91)]})
    assert found == ["key older than 90 days  dbt-runner  old  91 days  (used by: local Airflow)"]


def test_disabled_old_key_still_reported() -> None:
    # A disabled key is harmless until someone enables it: rotation ends with deleting it
    found = check({"dashboard-reader": [key("dashboard-reader", "b0", 200, disabled=True)]})
    assert found == ["key older than 90 days  dashboard-reader  b0  200 days, disabled  (used by: Streamlit)"]


def test_any_key_on_an_unlisted_account_is_reported() -> None:
    found = check({"terraform-plan": [key("terraform-plan", "c1", 0)]})
    assert found == ["key on an account that should have none  terraform-plan  c1  0 days"]


def test_config_lists_terraform_accounts_only() -> None:
    # Every account allowed a key is one infra/ creates, with where the key is used
    config = yaml.safe_load(sa_keys.CONFIG.read_text(encoding="utf-8"))
    tf = (ROOT / "infra" / "service_accounts.tf").read_text(encoding="utf-8")
    assert isinstance(config["max_age_days"], int) and config["max_age_days"] > 0
    for account, used_by in config["accounts"].items():
        assert f"    {account} " in tf and used_by.strip()
    assert "terraform-plan" not in config["accounts"]
