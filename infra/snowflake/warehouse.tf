# Smallest size, billed per second with a 60 s minimum after each resume
resource "snowflake_warehouse" "olist" {
  name                         = "OLIST_WH"
  warehouse_type               = "STANDARD"
  warehouse_size               = "XSMALL"
  auto_suspend                 = 60
  auto_resume                  = "true"
  initially_suspended          = true
  statement_timeout_in_seconds = 1800
  comment                      = "dlt loads, dbt builds and the task graph (olist-analytics-dbt)"
}
