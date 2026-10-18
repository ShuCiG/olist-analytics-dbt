terraform {
  required_version = ">= 1.6" # import blocks with expressions in `id`

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "8.6.0"
    }
    # Only for the BigQuery quota override (beta-only resource)
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "8.7.0"
    }
  }

  # Bucket is declared in state.tf; first run bootstraps with a local state,
  # see infra/README.md
  backend "gcs" {
    bucket = "olist-analytics-dbt-472315-tfstate"
    prefix = "infra"
  }
}

# Runs with the operator's own Google account (gcloud ADC), not a service
# account key: the CI / dbt keys hold no IAM or billing rights. User
# credentials need a quota project for the Budgets and Service Usage APIs.
provider "google" {
  project               = var.project_id
  region                = var.location
  billing_project       = var.project_id
  user_project_override = true
}

provider "google-beta" {
  project               = var.project_id
  region                = var.location
  billing_project       = var.project_id
  user_project_override = true
}
