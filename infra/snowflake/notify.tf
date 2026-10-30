# Failure email of the task graph (snowflake/tasks.sql): its finalizer task calls
# SYSTEM$SEND_SNOWFLAKE_NOTIFICATION with this integration's default recipient, so
# the address stays out of the repo. Task ERROR_INTEGRATION takes cloud queues
# only (SNS, Pub/Sub, Event Grid), not email. The recipient must be a verified
# email of a user in the account. snowflake_email_notification_integration has no
# DEFAULT_RECIPIENTS yet, hence snowflake_execute.
resource "snowflake_execute" "email_integration" {
  execute = <<-SQL
    CREATE NOTIFICATION INTEGRATION OLIST_EMAIL
      TYPE = EMAIL
      ENABLED = TRUE
      ALLOWED_RECIPIENTS = ('${var.alert_email}')
      DEFAULT_RECIPIENTS = ('${var.alert_email}')
      DEFAULT_SUBJECT = 'olist-analytics-dbt: Snowflake task graph'
      COMMENT = 'Failure email of the dbt task graph'
  SQL
  revert  = "DROP NOTIFICATION INTEGRATION IF EXISTS OLIST_EMAIL"
}

resource "snowflake_grant_privileges_to_account_role" "runner_email" {
  account_role_name = snowflake_account_role.runner.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = "OLIST_EMAIL"
  }
  depends_on = [snowflake_execute.email_integration]
}
