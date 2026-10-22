# GitHub Actions -> GCP without long-lived keys: Workload Identity Federation.
# A job asks GitHub for an OIDC token, Google's STS exchanges it for a federated
# token, and the federated principal impersonates one service account. Only
# tokens of this repository are accepted, and each account is reachable from
# one OIDC subject only.

locals {
  # Numeric IDs, not names: a deleted and re-created owner or repository with
  # the same name gets new IDs and can't reuse these bindings
  github_owner_id      = "90512692"
  github_repository_id = "1413000102"

  # The repository uses GitHub's immutable subject format (owner and repository
  # IDs in `sub`): gh api repos/ShuCiG/olist-analytics-dbt/actions/oidc/customization/sub
  github_subject_prefix = "repo:ShuCiG@${local.github_owner_id}/olist-analytics-dbt@${local.github_repository_id}"

  # Service account -> the only OIDC subject that may impersonate it
  github_identities = {
    # deploy-prod: environment `prod`, whose deployment rule admits only main
    dbt-runner = "${local.github_subject_prefix}:environment:prod"
    # dbt build (BigQuery) on pull requests
    olist-ci = "${local.github_subject_prefix}:pull_request"
    # drift.yml: environment `drift-check`, main only (drift.tf)
    terraform-plan = "${local.github_subject_prefix}:environment:drift-check"
  }

  # Environment tokens are accepted from main only (attribute_condition below)
  github_main_only_subjects = [local.github_identities["dbt-runner"], local.github_identities["terraform-plan"]]

  github_service_accounts = merge(google_service_account.this, {
    terraform-plan = google_service_account.terraform_plan
  })
}

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"
  description               = "OIDC tokens of the olist-analytics-dbt workflows"

  depends_on = [google_project_service.this]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "olist-analytics-dbt"
  display_name                       = "olist-analytics-dbt"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }

  # Tokens of this repository only. An environment token (prod, drift-check) must
  # also come from main: the environments' deployment rules already say so, this
  # holds even if a rule is edited.
  attribute_condition = join(" && ", [
    "assertion.repository_owner_id == '${local.github_owner_id}'",
    "assertion.repository_id == '${local.github_repository_id}'",
    "(!(assertion.sub in ['${join("', '", local.github_main_only_subjects)}']) || assertion.ref == 'refs/heads/main')",
  ])
}

resource "google_service_account_iam_member" "github" {
  for_each = local.github_identities

  service_account_id = local.github_service_accounts[each.key].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/subject/${each.value}"
}

# For google-github-actions/auth
output "github_workload_identity_provider" {
  value = google_iam_workload_identity_pool_provider.github.name
}
