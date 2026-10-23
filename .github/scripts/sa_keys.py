"""Old or unexpected service account keys (job `sa-keys` in drift.yml, D63).

GitHub Actions is keyless (D57), but the local Airflow stack, Streamlit Cloud and
local runs use JSON keys made in the console. This lists the user-managed keys of
every service account in the project (metadata only, never the secret) and fails on
- a key older than max_age_days in infra/sa_keys.yaml, and
- any key of an account that infra/sa_keys.yaml doesn't list.
Read-only; runs as `terraform-plan` (roles/iam.serviceAccountViewer lists keys).

Usage: python .github/scripts/sa_keys.py <project id>
Env:   Application Default Credentials (google-github-actions/auth in CI)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "infra" / "sa_keys.yaml"


@dataclass
class Key:
    account: str  # account id, the part of the email before @
    key_id: str
    created: datetime
    disabled: bool


def read_keys(session: Any, project: str) -> tuple[list[str], list[Key]]:
    """Account ids and their user-managed keys. `session`: an authorized requests session."""

    def get(url: str) -> dict[str, Any]:
        resp = session.get(url)
        resp.raise_for_status()
        return resp.json()

    iam = f"https://iam.googleapis.com/v1/projects/{project}/serviceAccounts"
    emails: list[str] = []
    url = iam
    while True:
        page = get(url)
        emails += [a["email"] for a in page.get("accounts", [])]
        if not page.get("nextPageToken"):
            break
        url = f"{iam}?pageToken={page['nextPageToken']}"

    accounts, keys = [], []
    for email in sorted(emails):
        account = email.split("@", 1)[0]
        accounts.append(account)
        for k in get(f"{iam}/{email}/keys?keyTypes=USER_MANAGED").get("keys", []):
            created = datetime.fromisoformat(k["validAfterTime"].replace("Z", "+00:00"))
            keys.append(Key(account, k["name"].rsplit("/", 1)[-1], created, k.get("disabled", False)))
    return accounts, keys


def findings(keys: list[Key], config: dict[str, Any], now: datetime) -> list[str]:
    """Keys over the age limit or on an account that should have none, one line each."""
    limit, allowed = config["max_age_days"], config["accounts"]
    out = []
    for k in sorted(keys, key=lambda k: (k.account, k.created)):
        age = (now - k.created).days
        state = ", disabled" if k.disabled else ""
        if k.account not in allowed:
            out.append(f"key on an account that should have none  {k.account}  {k.key_id}  {age} days{state}")
        elif age > limit:
            out.append(f"key older than {limit} days  {k.account}  {k.key_id}  {age} days{state}  (used by: {allowed[k.account]})")
    return out


def main() -> None:
    project = sys.argv[1]
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(credentials)
    session.headers["x-goog-user-project"] = project  # quota project, as in iam_drift.py
    accounts, keys = read_keys(session, project)

    now = datetime.now(UTC)
    print(f"Checked {len(accounts)} service accounts, {len(keys)} user-managed keys (limit {config['max_age_days']} days)")
    for k in keys:
        print(f"  {k.account}  {k.key_id}  {(now - k.created).days} days{', disabled' if k.disabled else ''}")
    found = findings(keys, config, now)
    for line in found:
        print(line)
    if found:
        print(f"::error::{len(found)} service account key(s) to rotate or delete; see the lines above and docs/RUNNING.md")
        sys.exit(1)
    print("No old or unexpected keys")


if __name__ == "__main__":
    main()
