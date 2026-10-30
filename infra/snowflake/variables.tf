variable "organization_name" {
  description = "Snowflake organization (SELECT CURRENT_ORGANIZATION_NAME())"
  type        = string
}

variable "account_name" {
  description = "Snowflake account name in the organization (SELECT CURRENT_ACCOUNT_NAME())"
  type        = string
}

variable "terraform_private_key_path" {
  description = "Unencrypted PKCS#8 private key of TERRAFORM_SVC"
  type        = string
  default     = "~/.snowflake/keys/olist_terraform.p8"
}

variable "olist_svc_rsa_public_key" {
  description = "Public key of OLIST_SVC: one line, no PEM header or footer"
  type        = string
}

variable "owner_user" {
  description = "Owner's login user: gets the OLIST_RUNNER role and the credit alerts (needs a verified email)"
  type        = string
}

variable "monthly_credit_quota" {
  description = "Account resource monitor quota, credits per calendar month"
  type        = number
  default     = 20
}

variable "alert_email" {
  description = "Recipient of the task graph failure email: a verified email of a user in the account"
  type        = string
}
