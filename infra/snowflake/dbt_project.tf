# dbt Projects on Snowflake (#185): a schema for the DBT PROJECT object and its task
# graph, both made by OLIST_RUNNER (snowflake/), not here. The project is uploaded
# with its packages by `snow dbt deploy`: trial accounts allow no external access
# integration, so neither `dbt deps` inside Snowflake nor a Git repository source
# without vendored packages works.

resource "snowflake_schema" "dbt" {
  database = snowflake_database.olist.name
  name     = "DBT_PROJECTS"
  comment  = "DBT PROJECT object and task graph"
}

resource "snowflake_grant_privileges_to_account_role" "runner_dbt_schema" {
  account_role_name = snowflake_account_role.runner.name
  privileges        = ["USAGE", "CREATE DBT PROJECT", "CREATE TASK", "CREATE STAGE"]
  on_schema {
    schema_name = snowflake_schema.dbt.fully_qualified_name
  }
}

# Tasks run as their owner (OLIST_RUNNER): resuming a task needs EXECUTE TASK
resource "snowflake_grant_privileges_to_account_role" "runner_execute_task" {
  account_role_name = snowflake_account_role.runner.name
  privileges        = ["EXECUTE TASK"]
  on_account        = true
}
