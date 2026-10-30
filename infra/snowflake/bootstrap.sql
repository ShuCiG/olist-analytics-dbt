-- Run once in Snowsight as ACCOUNTADMIN before the first terraform init.
-- Creates the key-pair service user Terraform runs as (see versions.tf).
-- Resource monitors need ACCOUNTADMIN, hence the role.
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS TERRAFORM_SVC
  TYPE = SERVICE
  RSA_PUBLIC_KEY = '<contents of ~/.snowflake/keys/olist_terraform.pub without header/footer, one line>'
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Terraform for olist-analytics-dbt (infra/snowflake)';

GRANT ROLE ACCOUNTADMIN TO USER TERRAFORM_SVC;

-- Values for terraform.tfvars
SELECT CURRENT_ORGANIZATION_NAME() AS organization_name,
       CURRENT_ACCOUNT_NAME()      AS account_name,
       CURRENT_USER()              AS owner_user;
