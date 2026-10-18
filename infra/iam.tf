# Grants are additive (`*_iam_member`): Terraform adds and removes only the
# bindings listed here and never rewrites a whole policy, so grants made
# outside this configuration (project owner, dataset creator OWNER entries)
# are left alone.

locals {
  project_roles = {
    "dbt-runner/bigquery.dataEditor"      = { sa = "dbt-runner", role = "roles/bigquery.dataEditor" }
    "dbt-runner/bigquery.jobUser"         = { sa = "dbt-runner", role = "roles/bigquery.jobUser" }
    "dbt-runner/bigquery.readSessionUser" = { sa = "dbt-runner", role = "roles/bigquery.readSessionUser" }
    "dashboard-reader/bigquery.jobUser"   = { sa = "dashboard-reader", role = "roles/bigquery.jobUser" }
    # Run jobs and create datasets; the creator owns the ci_* datasets it makes
    "olist-ci/bigquery.user" = { sa = "olist-ci", role = "roles/bigquery.user" }
  }

  # Read access per dataset (dataset level, not table level: dbt re-creates
  # tables on every build, which would drop table-level grants)
  dataset_readers = {
    dashboard-reader = ["marts", "airflow_marts"]
    # What a full build or slim CI's --defer reads
    olist-ci = ["raw", "staging", "intermediate", "marts", "seeds", "snapshots"]
  }

  dataset_reader_grants = merge([
    for sa, datasets in local.dataset_readers : {
      for ds in datasets : "${ds}/${sa}" => { sa = sa, dataset = ds }
    }
  ]...)
}

resource "google_project_iam_member" "this" {
  for_each = local.project_roles

  project = var.project_id
  role    = each.value.role
  member  = google_service_account.this[each.value.sa].member
}

resource "google_bigquery_dataset_iam_member" "reader" {
  for_each = local.dataset_reader_grants

  dataset_id = google_bigquery_dataset.this[each.value.dataset].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.this[each.value.sa].member
}
