# Read-only identity for the scheduled drift check (.github/workflows/drift.yml):
# `terraform plan` against the live project, so a change made in the console
# shows up as a failed run instead of waiting for the next manual plan. It can
# read everything this configuration manages and change nothing. Reachable only
# from the `drift-check` environment on main (wif.tf).
# Kept out of `local.service_accounts` / `local.project_roles`: those feed the
# import blocks (imports.tf), and these resources are created, not adopted.

resource "google_service_account" "terraform_plan" {
  account_id   = "terraform-plan"
  display_name = "terraform-plan"
  description  = "Read-only: scheduled terraform plan (drift check)"
}

locals {
  # Each one is a viewer role; together they cover the refresh of every resource
  # type in infra/
  terraform_plan_project_roles = toset([
    "roles/iam.securityReviewer",            # IAM policies: project, service accounts, state bucket
    "roles/iam.serviceAccountViewer",        # service accounts
    "roles/iam.workloadIdentityPoolViewer",  # pool and provider (wif.tf)
    "roles/bigquery.metadataViewer",         # datasets and their access lists, no table data
    "roles/serviceusage.serviceUsageViewer", # enabled APIs, quota override
    # The provider bills API calls to the project (user_project_override in
    # versions.tf); this role allows that and nothing else
    "roles/serviceusage.serviceUsageConsumer",
  ])
}

resource "google_project_iam_member" "terraform_plan" {
  for_each = local.terraform_plan_project_roles

  project = var.project_id
  role    = each.value
  member  = google_service_account.terraform_plan.member
}

# The state: bucket metadata and objects, read only. `plan -lock=false` writes no
# lock file.
resource "google_storage_bucket_iam_member" "terraform_plan" {
  for_each = toset(["roles/storage.legacyBucketReader", "roles/storage.objectViewer"])

  bucket = google_storage_bucket.tfstate.name
  role   = each.value
  member = google_service_account.terraform_plan.member
}

# The budget lives on the billing account, not in the project
resource "google_billing_account_iam_member" "terraform_plan" {
  billing_account_id = var.billing_account
  role               = "roles/billing.viewer"
  member             = google_service_account.terraform_plan.member
}
