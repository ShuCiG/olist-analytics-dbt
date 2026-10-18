# APIs this project relies on. Not disabled on destroy: other things in the
# project use them too.
locals {
  services = toset([
    "bigquery.googleapis.com",             # warehouse (dlt, dbt, dashboard)
    "bigquerystorage.googleapis.com",      # Storage Read API (datacontract-cli)
    "billingbudgets.googleapis.com",       # budget below
    "cloudresourcemanager.googleapis.com", # project IAM, Terraform itself
    "iam.googleapis.com",                  # service accounts below
    "serviceusage.googleapis.com",         # quota override below
    "storage.googleapis.com",              # state bucket
  ])
}

resource "google_project_service" "this" {
  for_each = local.services

  service            = each.value
  disable_on_destroy = false
}
