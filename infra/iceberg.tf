# Apache Iceberg tables written by BigQuery (BigLake), evaluation (#184): a
# bucket for the table files and a Cloud resource connection whose service
# account writes them. dbt's BigQuery adapter creates Iceberg tables
# `WITH CONNECTION DEFAULT`, so this connection is also set as the project's
# default one for EU, a project option Terraform has no resource for (owner
# runs it once, docs/RUNNING.md "Iceberg tables").
# Only rebuildable dbt output lives here: no versioning. prevent_destroy as on
# every bucket (tests/test_infra.py); removing the evaluation means dropping
# that first, then force_destroy deletes the bucket with its files.

resource "google_storage_bucket" "iceberg" {
  name                        = "${var.project_id}-iceberg"
  location                    = var.location # same multi-region as the datasets
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  # Every rebuild replaces the files; soft delete would bill the old ones for
  # another 7 days
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_connection" "iceberg" {
  connection_id = "iceberg"
  location      = var.location
  friendly_name = "iceberg"
  description   = "BigLake Iceberg tables in ${google_storage_bucket.iceberg.name}"
  cloud_resource {}

  depends_on = [google_project_service.this]
}

# The connection's service account (Google-made, bqcx-...) reads and writes
# the table files. Both roles, as BigQuery's docs list them: without
# legacyBucketReader (storage.buckets.get) the CREATE TABLE tries to grant the
# account itself and fails on storage.buckets.setIamPolicy.
resource "google_storage_bucket_iam_member" "iceberg_connection" {
  for_each = toset(["roles/storage.objectUser", "roles/storage.legacyBucketReader"])

  bucket = google_storage_bucket.iceberg.name
  role   = each.value
  member = "serviceAccount:${google_bigquery_connection.iceberg.cloud_resource[0].service_account_id}"
}

# Adopts the grant made before it became a for_each
moved {
  from = google_storage_bucket_iam_member.iceberg_connection
  to   = google_storage_bucket_iam_member.iceberg_connection["roles/storage.objectUser"]
}

# dbt-runner creates the tables through the connection: that takes
# bigquery.connections.delegate, only in connectionAdmin, granted on this one
# connection instead of the project
resource "google_bigquery_connection_iam_member" "iceberg_dbt_runner" {
  location      = google_bigquery_connection.iceberg.location
  connection_id = google_bigquery_connection.iceberg.connection_id
  role          = "roles/bigquery.connectionAdmin"
  member        = google_service_account.this["dbt-runner"].member
}

# The drift check's plan refreshes the connection (bigquery.connections.get)
# and the bucket (storage.buckets.get); its project roles cover neither
resource "google_bigquery_connection_iam_member" "iceberg_terraform_plan" {
  location      = google_bigquery_connection.iceberg.location
  connection_id = google_bigquery_connection.iceberg.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = google_service_account.terraform_plan.member
}

resource "google_storage_bucket_iam_member" "iceberg_terraform_plan" {
  bucket = google_storage_bucket.iceberg.name
  role   = "roles/storage.legacyBucketReader"
  member = google_service_account.terraform_plan.member
}
