# APIs this project relies on. Not disabled on destroy: other things in the
# project use them too.
locals {
  services = toset([
    "bigquery.googleapis.com",             # warehouse (dlt, dbt, dashboard)
    "bigquerystorage.googleapis.com",      # Storage Read API (datacontract-cli)
    "billingbudgets.googleapis.com",       # budget below
    "cloudbilling.googleapis.com",         # billing account IAM (drift.tf)
    "cloudresourcemanager.googleapis.com", # project IAM, Terraform itself
    "iam.googleapis.com",                  # service accounts below
    "iamcredentials.googleapis.com",       # service account impersonation (wif.tf)
    "serviceusage.googleapis.com",         # quota override below
    "storage.googleapis.com",              # state bucket
    "sts.googleapis.com",                  # OIDC token exchange (wif.tf)
  ])

  # Enabled by Terraform, not adopted (imports.tf)
  services_added = toset(["sts.googleapis.com", "cloudbilling.googleapis.com"])
}

resource "google_project_service" "this" {
  for_each = local.services

  service            = each.value
  disable_on_destroy = false
}
