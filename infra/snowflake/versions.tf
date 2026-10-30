terraform {
  required_version = ">= 1.6"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.22.0"
    }
  }

  # Same bucket as the GCP root (../state.tf), own prefix: a separate root, so
  # the GCP plan never needs the Snowflake account (trial, may be dropped).
  backend "gcs" {
    bucket = "olist-analytics-dbt-472315-tfstate"
    prefix = "snowflake"
  }
}

# Runs as TERRAFORM_SVC, a key-pair service user made once by bootstrap.sql.
# The private key stays on the operator's disk; no secrets in this repo or state.
provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = "TERRAFORM_SVC"
  role              = "ACCOUNTADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.terraform_private_key_path))
}
