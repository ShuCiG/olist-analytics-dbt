# One account-level monitor: it covers every warehouse, including the trial's
# default COMPUTE_WH that Snowsight uses. Serverless compute (serverless tasks,
# Cortex) is not covered by monitors, so the task graph runs on OLIST_WH.
resource "snowflake_resource_monitor" "account" {
  name                      = "OLIST_ACCOUNT_MONITOR"
  credit_quota              = var.monthly_credit_quota
  frequency                 = "MONTHLY"
  start_timestamp           = "IMMEDIATELY"
  notify_triggers           = [50, 80]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
  notify_users              = [var.owner_user]
}

# The resource has no account assignment yet; the provider docs point to
# snowflake_execute for it.
resource "snowflake_execute" "account_monitor" {
  execute = "ALTER ACCOUNT SET RESOURCE_MONITOR = ${snowflake_resource_monitor.account.fully_qualified_name}"
  revert  = "ALTER ACCOUNT UNSET RESOURCE_MONITOR"
}
