# Service accounts only. Their keys stay out of Terraform: a key created here
# would sit in the state file in plain text. Keys are made in the console and
# stored as GitHub secrets (GCP_SA_KEY, GCP_SA_KEY_CI) / Streamlit secrets.
locals {
  service_accounts = {
    dbt-runner       = "Writes the warehouse: dlt loads, dbt builds (CD prod, Airflow)"
    dashboard-reader = "Streamlit dashboard, read-only marts"
    olist-ci         = "PR CI: ci_* datasets only, read prod"
  }
}

resource "google_service_account" "this" {
  for_each = local.service_accounts

  account_id   = each.key
  display_name = each.key
  description  = each.value
}
