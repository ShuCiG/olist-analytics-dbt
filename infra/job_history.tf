# Job history for the cost mart (transform/models/marts/platform, D71): the prod
# deploy builds it as dbt-runner from `region-eu.INFORMATION_SCHEMA.JOBS_BY_PROJECT`,
# which needs bigquery.jobs.listAll. No predefined role holds just that
# (bigquery.resourceViewer adds reservations and capacity commitments), so a
# custom role with the one permission. It lists every job of the project with its
# labels, bytes and query text; it can't read table data or change anything.
# Created, not adopted: kept out of `local.project_roles` (imports.tf).

resource "google_project_iam_custom_role" "job_history_viewer" {
  role_id     = "jobHistoryViewer"
  title       = "BigQuery job history viewer"
  description = "List every BigQuery job of the project (INFORMATION_SCHEMA.JOBS_BY_PROJECT)"
  permissions = ["bigquery.jobs.listAll"]
}

resource "google_project_iam_member" "job_history_viewer" {
  project = var.project_id
  role    = google_project_iam_custom_role.job_history_viewer.name
  member  = google_service_account.this["dbt-runner"].member
}
