# Plans on a mock provider: no Snowflake account or key needed (CI)
mock_provider "snowflake" {}

variables {
  organization_name          = "ORG"
  account_name               = "ACC"
  owner_user                 = "OWNER"
  olist_svc_rsa_public_key   = "MIIB"
  terraform_private_key_path = "tests/dummy.p8"
  alert_email                = "ops@example.com"
}

run "cost_guards" {
  command = plan

  assert {
    condition     = snowflake_warehouse.olist.warehouse_size == "XSMALL" && snowflake_warehouse.olist.auto_suspend <= 60
    error_message = "OLIST_WH must stay XSMALL with auto-suspend of 60 s or less"
  }

  assert {
    condition     = snowflake_resource_monitor.account.credit_quota <= 20 && snowflake_resource_monitor.account.suspend_trigger <= 100
    error_message = "Account monitor: at most 20 credits a month, warehouses suspended at 100 %"
  }

  assert {
    condition     = startswith(snowflake_execute.account_monitor.execute, "ALTER ACCOUNT SET RESOURCE_MONITOR")
    error_message = "The monitor must be assigned to the account, not only to OLIST_WH"
  }
}

run "least_privilege" {
  command = plan

  assert {
    condition     = toset(snowflake_grant_privileges_to_account_role.runner_database.privileges) == toset(["USAGE", "CREATE SCHEMA", "MONITOR"])
    error_message = "OLIST_RUNNER gets no database privileges beyond usage, schema creation and monitor"
  }

  assert {
    condition     = snowflake_service_user.olist.default_role == "OLIST_RUNNER"
    error_message = "OLIST_SVC runs as OLIST_RUNNER"
  }
}

run "dbt_project_access" {
  command = plan

  assert {
    condition     = toset(snowflake_grant_privileges_to_account_role.runner_dbt_schema.privileges) == toset(["USAGE", "CREATE DBT PROJECT", "CREATE TASK", "CREATE STAGE"])
    error_message = "OLIST_RUNNER creates only the dbt project, its stage and tasks in DBT_PROJECTS"
  }
}
