# Runtime role for dlt, dbt Core, dbt Projects on Snowflake and the task graph
resource "snowflake_account_role" "runner" {
  name    = "OLIST_RUNNER"
  comment = "Loads raw and builds the dbt project in OLIST"
}

# Role hierarchy: SYSADMIN sees what the runner creates
resource "snowflake_grant_account_role" "runner_to_sysadmin" {
  role_name        = snowflake_account_role.runner.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "runner_to_owner" {
  role_name = snowflake_account_role.runner.name
  user_name = var.owner_user
}

resource "snowflake_grant_privileges_to_account_role" "runner_warehouse" {
  account_role_name = snowflake_account_role.runner.name
  privileges        = ["USAGE", "OPERATE", "MONITOR"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.olist.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "runner_database" {
  account_role_name = snowflake_account_role.runner.name
  privileges        = ["USAGE", "CREATE SCHEMA", "MONITOR"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.olist.name
  }
}

# Key-pair only: a SERVICE user cannot have a password or log in to Snowsight
resource "snowflake_service_user" "olist" {
  name                           = "OLIST_SVC"
  comment                        = "dlt, dbt and CI for olist-analytics-dbt"
  default_role                   = snowflake_account_role.runner.name
  default_warehouse              = snowflake_warehouse.olist.name
  default_secondary_roles_option = "NONE"
  rsa_public_key                 = var.olist_svc_rsa_public_key
}

resource "snowflake_grant_account_role" "runner_to_svc" {
  role_name = snowflake_account_role.runner.name
  user_name = snowflake_service_user.olist.name
}
