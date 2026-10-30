# dlt creates RAW and dbt its layer schemas (CREATE SCHEMA below), so the
# runtime role owns everything inside; Terraform owns only the database.
resource "snowflake_database" "olist" {
  name    = "OLIST"
  comment = "olist-analytics-dbt: raw (dlt) and dbt layers"

  lifecycle {
    prevent_destroy = true
  }
}
