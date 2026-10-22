"""IAM bindings made outside Terraform (job `iam-bindings` in drift.yml, D62).

`terraform plan` can't see them: grants are additive (`*_iam_member`) and labels
non-authoritative, so a role granted in the console leaves the plan clean (D60). This
reads the live IAM of everything infra/ manages and compares every binding with
- the bindings in the Terraform state, and
- infra/iam_allowlist.yaml (bindings outside Terraform on purpose).
Anything else fails: an unknown binding, a service account Terraform doesn't manage,
a label on a managed dataset. Read-only; runs as `terraform-plan` (viewer roles).

Usage: python .github/scripts/iam_drift.py <state.json>   (terraform show -json)
Env:   IAM_OWNER  the project owner's account (allowlist ${OWNER}), not in the repo
       Application Default Credentials (google-github-actions/auth in CI)

The log is public (Actions of a public repo): user and group emails are masked.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = ROOT / "infra" / "iam_allowlist.yaml"

Binding = tuple[str, str, str]  # (scope, role, member)

# Dataset access entries use BigQuery's basic roles; Terraform's dataset IAM members
# land there as these
DATASET_ROLES = {
    "READER": "roles/bigquery.dataViewer",
    "WRITER": "roles/bigquery.dataEditor",
    "OWNER": "roles/bigquery.dataOwner",
}


@dataclass
class Managed:
    """What infra/ manages, from the Terraform state."""

    project: str = ""
    project_number: str = ""
    billing_account: str = ""
    bucket: str = ""
    datasets: list[str] = field(default_factory=list)
    service_accounts: set[str] = field(default_factory=set)  # emails
    bindings: set[Binding] = field(default_factory=set)


def managed(state: dict[str, Any]) -> Managed:
    m = Managed()
    for r in state["values"]["root_module"]["resources"]:
        t, v = r["type"], r["values"]
        if t == "google_project_iam_member":
            m.project = v["project"]
            m.bindings.add(("project", v["role"], v["member"]))
        elif t == "google_service_account":
            m.service_accounts.add(v["email"])
        elif t == "google_service_account_iam_member":
            email = v["service_account_id"].rsplit("/", 1)[-1]
            m.bindings.add((f"serviceAccount/{email}", v["role"], v["member"]))
        elif t == "google_bigquery_dataset":
            m.datasets.append(v["dataset_id"])
        elif t == "google_bigquery_dataset_iam_member":
            m.bindings.add((f"dataset/{v['dataset_id']}", v["role"], v["member"]))
        elif t == "google_storage_bucket":
            m.bucket = v["name"]
        elif t == "google_storage_bucket_iam_member":
            m.bindings.add((f"bucket/{v['bucket'].removeprefix('b/')}", v["role"], v["member"]))
        elif t == "google_billing_account_iam_member":
            m.billing_account = v["billing_account_id"]
            m.bindings.add(("billingAccount", v["role"], v["member"]))
        elif t == "google_iam_workload_identity_pool":
            m.project_number = v["name"].split("/")[1]  # projects/<number>/locations/...
    m.datasets.sort()
    return m


def dataset_member(entry: dict[str, Any]) -> str:
    """An access entry's grantee in IAM member form."""
    if email := entry.get("userByEmail"):
        return f"{'serviceAccount' if email.endswith('.gserviceaccount.com') else 'user'}:{email}"
    for key, prefix in (("groupByEmail", "group"), ("domain", "domain"), ("specialGroup", "specialGroup")):
        if key in entry:
            return f"{prefix}:{entry[key]}"
    if "iamMember" in entry:
        return entry["iamMember"]
    # Authorized views, routines, datasets
    kind = next((k for k in ("view", "routine", "dataset") if k in entry), "unknown")
    return f"{kind}:{json.dumps(entry.get(kind, entry), sort_keys=True)}"


def policy_bindings(scope: str, policy: dict[str, Any]) -> set[Binding]:
    """Bindings of an IAM policy; a conditional one keeps its condition in the role."""
    out = set()
    for b in policy.get("bindings", []):
        role = b["role"] + (f" [condition: {b['condition'].get('title', '?')}]" if "condition" in b else "")
        out.update((scope, role, member) for member in b.get("members", []))
    return out


@dataclass
class Live:
    bindings: set[Binding] = field(default_factory=set)
    service_accounts: set[str] = field(default_factory=set)
    labels: dict[str, dict[str, str]] = field(default_factory=dict)  # dataset -> labels


def read_live(session: Any, m: Managed) -> Live:
    """The live IAM of what `m` covers. `session`: an authorized requests session."""

    def get(url: str, post: bool = False) -> dict[str, Any]:
        resp = session.post(url, json={}) if post else session.get(url)
        resp.raise_for_status()
        return resp.json()

    live = Live()
    crm = f"https://cloudresourcemanager.googleapis.com/v1/projects/{m.project}:getIamPolicy"
    live.bindings |= policy_bindings("project", get(crm, post=True))

    iam = f"https://iam.googleapis.com/v1/projects/{m.project}/serviceAccounts"
    url = iam
    while True:
        page = get(url)
        live.service_accounts |= {a["email"] for a in page.get("accounts", [])}
        if not page.get("nextPageToken"):
            break
        url = f"{iam}?pageToken={page['nextPageToken']}"
    for email in sorted(live.service_accounts):
        policy = get(f"{iam}/{email}:getIamPolicy", post=True)
        live.bindings |= policy_bindings(f"serviceAccount/{email}", policy)

    for ds in m.datasets:
        dataset = get(f"https://bigquery.googleapis.com/bigquery/v2/projects/{m.project}/datasets/{ds}")
        for entry in dataset.get("access", []):
            role = DATASET_ROLES.get(entry["role"], entry["role"])
            live.bindings.add((f"dataset/{ds}", role, dataset_member(entry)))
        if dataset.get("labels"):
            live.labels[ds] = dataset["labels"]

    bucket = get(f"https://storage.googleapis.com/storage/v1/b/{m.bucket}/iam")
    live.bindings |= policy_bindings(f"bucket/{m.bucket}", bucket)

    billing = f"https://cloudbilling.googleapis.com/v1/billingAccounts/{m.billing_account}:getIamPolicy"
    live.bindings |= policy_bindings("billingAccount", get(billing))
    return live


def allowlist(path: Path, values: dict[str, str]) -> list[dict[str, str]]:
    """Allowlist entries with ${OWNER} / ${PROJECT} / ${PROJECT_NUMBER} filled in."""
    entries = yaml.safe_load(path.read_text(encoding="utf-8"))

    def fill(text: str) -> str:
        return re.sub(r"\$\{(\w+)\}", lambda g: values[g[1]], text)

    return [{k: fill(entry[k]) for k in ("scope", "role", "member")} for entry in entries]


def allowed(binding: Binding, entries: list[dict[str, str]]) -> bool:
    scope, role, member = binding
    return any(
        fnmatch.fnmatchcase(scope, e["scope"])
        and fnmatch.fnmatchcase(role, e["role"])
        and fnmatch.fnmatchcase(member, e["member"])
        for e in entries
    )


def findings(m: Managed, live: Live, entries: list[dict[str, str]]) -> list[str]:
    """What is live but neither in the state nor allowlisted, one line each."""
    out = [
        f"binding  {scope}  {role}  {member}"
        for scope, role, member in sorted(live.bindings - m.bindings)
        if not allowed((scope, role, member), entries)
    ]
    out += [f"service account not in Terraform  {email}" for email in sorted(live.service_accounts - m.service_accounts)]
    out += [f"labels on dataset/{ds}  {json.dumps(labels, sort_keys=True)}" for ds, labels in sorted(live.labels.items())]
    return out


def mask(text: str) -> str:
    """user:/group: emails -> first letter + domain (the log is public)."""
    return re.sub(r"\b(user|group):([^@\s])[^@\s]*@", r"\1:\2***@", text)


def main() -> None:
    owner = os.environ.get("IAM_OWNER", "").strip()
    if not owner:
        print("::error::IAM_OWNER is not set (environment secret of drift-check)")
        sys.exit(1)
    m = managed(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")))
    entries = allowlist(ALLOWLIST, {"OWNER": owner, "PROJECT": m.project, "PROJECT_NUMBER": m.project_number})

    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(credentials)
    session.headers["x-goog-user-project"] = m.project  # quota project, as the provider's user_project_override
    live = read_live(session, m)

    print(
        f"Checked {len(live.bindings)} bindings: project, {len(live.service_accounts)} service accounts, "
        f"{len(m.datasets)} datasets, state bucket, billing account"
    )
    found = findings(m, live, entries)
    for line in found:
        print(mask(line))
    if found:
        print(f"::error::{len(found)} IAM change(s) outside Terraform; see the lines above and docs/RUNNING.md")
        sys.exit(1)
    print("No IAM changes outside Terraform")


if __name__ == "__main__":
    main()
