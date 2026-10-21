# Service accounts only. Their keys stay out of Terraform: a key created here
# would sit in the state file in plain text. GitHub Actions uses no keys
# (wif.tf); the local Airflow stack and Streamlit keep keys made in the console.
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
