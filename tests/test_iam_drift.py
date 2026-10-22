""".github/scripts/iam_drift.py (D62): live IAM vs the Terraform state and the
allowlist, on a fake state and fake API responses (no GCP calls)."""

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("iam_drift", ROOT / ".github" / "scripts" / "iam_drift.py")
assert _spec and _spec.loader
iam_drift = importlib.util.module_from_spec(_spec)
sys.modules["iam_drift"] = iam_drift  # dataclasses resolve annotations through it
_spec.loader.exec_module(iam_drift)

P = "olist-analytics-dbt-472315"
NUMBER = "456327248425"
OWNER = "owner@example.com"
RUNNER = f"dbt-runner@{P}.iam.gserviceaccount.com"
READER = f"dashboard-reader@{P}.iam.gserviceaccount.com"
BUCKET = f"{P}-tfstate"


def resource(type_: str, **values: Any) -> dict[str, Any]:
    return {"type": type_, "values": values}


STATE = {
    "values": {
        "root_module": {
            "resources": [
                resource("google_project_iam_member", project=P, role="roles/bigquery.jobUser", member=f"serviceAccount:{RUNNER}"),
                resource("google_service_account", email=RUNNER),
                resource("google_service_account", email=READER),
                resource(
                    "google_service_account_iam_member",
                    service_account_id=f"projects/{P}/serviceAccounts/{RUNNER}",
                    role="roles/iam.workloadIdentityUser",
                    member="principal://iam.googleapis.com/x/subject/prod",
                ),
                resource("google_bigquery_dataset", dataset_id="marts"),
                resource("google_bigquery_dataset_iam_member", dataset_id="marts", role="roles/bigquery.dataViewer", member=f"serviceAccount:{READER}"),
                resource("google_storage_bucket", name=BUCKET),
                resource("google_billing_account_iam_member", billing_account_id="000000-000000-000000", role="roles/billing.viewer", member="serviceAccount:tp@x"),
                resource("google_iam_workload_identity_pool", name=f"projects/{NUMBER}/locations/global/workloadIdentityPools/github"),
            ]
        }
    }
}


def policy(*bindings: tuple[str, str]) -> dict[str, Any]:
    return {"bindings": [{"role": role, "members": [member]} for role, member in bindings]}


def api(extra_project: tuple[str, str] | None = None, labels: dict | None = None, extra_sa: str | None = None) -> dict[str, Any]:
    """Live API responses as they look today, plus one optional change."""
    project = [("roles/bigquery.jobUser", f"serviceAccount:{RUNNER}"), ("roles/owner", f"user:{OWNER}")]
    accounts = [RUNNER, READER] + ([extra_sa] if extra_sa else [])
    responses = {
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{P}:getIamPolicy": policy(*project, *([extra_project] if extra_project else [])),
        f"https://iam.googleapis.com/v1/projects/{P}/serviceAccounts": {"accounts": [{"email": a} for a in accounts]},
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/datasets/marts": {
            "access": [
                {"role": "OWNER", "specialGroup": "projectOwners"},
                {"role": "OWNER", "userByEmail": RUNNER},
                {"role": "READER", "userByEmail": READER},
            ],
            **({"labels": labels} if labels else {}),
        },
        f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/iam": policy(("roles/storage.legacyBucketOwner", f"projectOwner:{P}")),
        "https://cloudbilling.googleapis.com/v1/billingAccounts/000000-000000-000000:getIamPolicy": policy(
            ("roles/billing.admin", f"user:{OWNER}"), ("roles/billing.viewer", "serviceAccount:tp@x")
        ),
    }
    for a in accounts:
        sa_policy = policy(("roles/iam.workloadIdentityUser", "principal://iam.googleapis.com/x/subject/prod")) if a == RUNNER else {}
        responses[f"https://iam.googleapis.com/v1/projects/{P}/serviceAccounts/{a}:getIamPolicy"] = sa_policy
    return responses


class FakeSession:
    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses

    def _reply(self, url: str):
        body = self.responses[url]

        class Resp:
            def raise_for_status(self) -> None: ...

            def json(self) -> Any:
                return body

        return Resp()

    def get(self, url: str):
        return self._reply(url)

    def post(self, url: str, json: Any = None):
        return self._reply(url)


def check(responses: dict[str, Any]) -> list[str]:
    m = iam_drift.managed(STATE)
    entries = iam_drift.allowlist(iam_drift.ALLOWLIST, {"OWNER": OWNER, "PROJECT": m.project, "PROJECT_NUMBER": m.project_number})
    return iam_drift.findings(m, iam_drift.read_live(FakeSession(responses), m), entries)


def test_state_bindings_and_allowlisted_defaults_pass() -> None:
    # Terraform's grants, the owner, BigQuery's default entries, the creator's OWNER,
    # GCS's legacy bucket bindings: nothing to report
    assert check(api()) == []


def test_console_grant_is_reported() -> None:
    assert check(api(extra_project=("roles/editor", "user:someone@example.com"))) == [
        "binding  project  roles/editor  user:someone@example.com"
    ]


def test_allowlisted_role_for_another_member_is_reported() -> None:
    # roles/owner is allowed for the owner only
    assert check(api(extra_project=("roles/owner", "user:intruder@example.com"))) == [
        "binding  project  roles/owner  user:intruder@example.com"
    ]


def test_service_agent_is_allowed() -> None:
    agent = f"serviceAccount:service-{NUMBER}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
    assert check(api(extra_project=("roles/bigquerydatatransfer.serviceAgent", agent))) == []


def test_unmanaged_service_account_and_dataset_labels_are_reported() -> None:
    extra = f"backdoor@{P}.iam.gserviceaccount.com"
    found = check(api(labels={"drift": "test"}, extra_sa=extra))
    assert found == [
        f"service account not in Terraform  {extra}",
        'labels on dataset/marts  {"drift": "test"}',
    ]


def test_conditional_binding_is_never_the_terraform_one() -> None:
    live = iam_drift.policy_bindings(
        "project", {"bindings": [{"role": "roles/bigquery.jobUser", "members": ["user:a@b"], "condition": {"title": "t"}}]}
    )
    assert live == {("project", "roles/bigquery.jobUser [condition: t]", "user:a@b")}


@pytest.mark.parametrize(
    ("entry", "member"),
    [
        ({"userByEmail": RUNNER}, f"serviceAccount:{RUNNER}"),
        ({"userByEmail": "a@gmail.com"}, "user:a@gmail.com"),
        ({"groupByEmail": "g@x.com"}, "group:g@x.com"),
        ({"specialGroup": "projectReaders"}, "specialGroup:projectReaders"),
        ({"iamMember": "principal://x"}, "principal://x"),
        ({"view": {"datasetId": "d", "tableId": "t"}}, 'view:{"datasetId": "d", "tableId": "t"}'),
    ],
)
def test_dataset_member(entry: dict, member: str) -> None:
    assert iam_drift.dataset_member(entry) == member


def test_mask_hides_people_not_service_accounts() -> None:
    line = f"binding  project  roles/editor  user:someone@example.com group:team@example.com serviceAccount:{RUNNER}"
    assert iam_drift.mask(line) == (
        f"binding  project  roles/editor  user:s***@example.com group:t***@example.com serviceAccount:{RUNNER}"
    )


def test_allowlist_entries_are_explained() -> None:
    import yaml

    for entry in yaml.safe_load(iam_drift.ALLOWLIST.read_text(encoding="utf-8")):
        assert set(entry) == {"scope", "role", "member", "why"} and entry["why"].strip()
