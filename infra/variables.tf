variable "project_id" {
  description = "GCP project that holds the BigQuery warehouse"
  type        = string
  default     = "olist-analytics-dbt-472315"
}

variable "location" {
  description = "Location of the BigQuery datasets and the state bucket"
  type        = string
  default     = "EU"
}

variable "billing_account" {
  description = "Billing account ID (XXXXXX-XXXXXX-XXXXXX) that owns the budget; set in terraform.tfvars"
  type        = string
}

variable "owner_member" {
  description = "The project owner as an IAM member (\"user:...\"), granted Fine-Grained Reader on the PII policy tags (pii.tf); set in terraform.tfvars, kept out of the repo. Null: no grant."
  type        = string
  default     = null
}
