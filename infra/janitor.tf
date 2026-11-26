# Identity of the weekly cleanup of throwaway BigQuery datasets (.github/workflows/
# cleanup.yml, D137): drill, scale-test and leftover CI datasets. Nothing else in the
# project can delete a dataset (dbt-runner is dataEditor, olist-ci drops only what it
# made), and those datasets were made by hand.
#
# What it can do is limited twice. The delete role is bound with an IAM condition on the
# dataset name, so a call on raw, marts, airflow_* or any other managed dataset is
# refused by GCP, not by the script. And it is reachable only from the `janitor`
# environment on main (wif.tf). The listing role is metadata only (no table data).
# Created, not adopted: kept out of `local.service_accounts` (imports.tf).

locals {
  # Dataset name prefixes the janitor may delete: dr254_raw, scale10x_marts, ci_pr_12_34_marts.
  # No managed dataset may start with one (tests/policy.tftest.hcl).
  janitor_prefixes = ["dr", "scale", "ci_pr_"]

  janitor_condition = join(" || ", [
    for p in local.janitor_prefixes :
    "resource.name.startsWith(\"projects/${var.project_id}/datasets/${p}\")"
  ])
}

resource "google_service_account" "dataset_janitor" {
  account_id   = "dataset-janitor"
  display_name = "dataset-janitor"
  description  = "Weekly drop of throwaway BigQuery datasets (dr*, scale*, ci_pr_*), by name condition"
}

resource "google_project_iam_custom_role" "dataset_janitor" {
  role_id     = "datasetJanitor"
  title       = "BigQuery dataset janitor"
  description = "Delete a dataset with its tables, routines and models. Bound with a name condition (janitor.tf)"
  permissions = [
    "bigquery.datasets.get",
    "bigquery.datasets.delete",
    "bigquery.tables.list",
    "bigquery.tables.delete",
    "bigquery.routines.list",
    "bigquery.routines.delete",
    "bigquery.models.list",
    "bigquery.models.delete",
  ]
}

# Lists the project's datasets and their metadata (name, created, labels), no table data
resource "google_project_iam_member" "dataset_janitor_lister" {
  project = var.project_id
  role    = "roles/bigquery.metadataViewer"
  member  = google_service_account.dataset_janitor.member
}

resource "google_project_iam_member" "dataset_janitor_delete" {
  project = var.project_id
  role    = google_project_iam_custom_role.dataset_janitor.name
  member  = google_service_account.dataset_janitor.member

  condition {
    title       = "throwaway-datasets-only"
    description = "dr*, scale* and ci_pr_* datasets (janitor.tf)"
    expression  = local.janitor_condition
  }
}

# The API calls are billed to the project (user_project_override in versions.tf)
resource "google_project_iam_member" "dataset_janitor_consumer" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = google_service_account.dataset_janitor.member
}

# bigquery.jobUser is not needed: datasets.list and delete are API calls, not jobs
