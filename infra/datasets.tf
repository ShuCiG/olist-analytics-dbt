# BigQuery datasets of the long-lived environments. dlt and dbt write the
# tables; Terraform owns the datasets, their settings and read grants.
# - prod (GitHub Actions CD): raw (+ raw_staging, dlt's merge staging), dbt
#   layers, elementary
# - Airflow BigQuery DAG: the same set prefixed airflow_. Its reset
#   (drop_isolated_datasets --target airflow) empties these datasets instead
#   of dropping them, so it leaves no drift here.
# PR datasets (ci_*) live for one CI run and stay out of Terraform.
locals {
  dataset_layers = ["raw", "raw_staging", "staging", "intermediate", "marts", "seeds", "snapshots", "elementary"]

  datasets = toset(concat(
    local.dataset_layers,
    [for layer in local.dataset_layers : "airflow_${layer}"],
  ))
}

resource "google_bigquery_dataset" "this" {
  for_each = local.datasets

  dataset_id = each.value
  location   = var.location

  # No default_table_expiration_ms: tables never expire. (raw and seeds had
  # 60 days left over from the BigQuery sandbox.)

  # `access` is not set: entries outside the grants in iam.tf (project
  # roles, the creator's OWNER) are left as BigQuery made them.

  lifecycle {
    prevent_destroy = true
  }
}
