# Policy checks on the planned configuration (D46). Mock providers: no
# credentials, no calls to GCP; runs in CI on every PR. They guard the code,
# not the live project (`terraform plan` against GCP does that, locally).
# Run: cd infra && terraform init -backend=false && terraform test

mock_provider "google" {}
mock_provider "google-beta" {}

# imports.tf targets: a mock provider can't import, an override stands in
override_resource {
  target = google_project_service.this
}
override_resource {
  target = google_service_account.this
  values = { # the provider validates the IAM member and account name formats
    name   = "projects/olist-analytics-dbt-472315/serviceAccounts/mock-account@olist-analytics-dbt-472315.iam.gserviceaccount.com"
    email  = "mock-account@olist-analytics-dbt-472315.iam.gserviceaccount.com"
    member = "serviceAccount:mock-account@olist-analytics-dbt-472315.iam.gserviceaccount.com"
  }
}
override_resource {
  target = google_service_account.terraform_plan
  values = {
    name   = "projects/olist-analytics-dbt-472315/serviceAccounts/terraform-plan@olist-analytics-dbt-472315.iam.gserviceaccount.com"
    email  = "terraform-plan@olist-analytics-dbt-472315.iam.gserviceaccount.com"
    member = "serviceAccount:terraform-plan@olist-analytics-dbt-472315.iam.gserviceaccount.com"
  }
}
override_resource {
  target = google_project_iam_member.this
}
override_resource {
  target = google_bigquery_dataset.this
}
override_resource {
  target = google_bigquery_dataset_iam_member.reader
}
override_resource {
  target = google_billing_budget.monthly
}
override_resource {
  target = google_service_usage_consumer_quota_override.bigquery_query_per_day
}

variables {
  billing_account = "000000-000000-000000"
}

run "datasets" {
  command = plan

  # Every prod layer has its Airflow twin, and nothing else is managed
  assert {
    condition = toset(keys(google_bigquery_dataset.this)) == toset(concat(
      local.dataset_layers, [for layer in local.dataset_layers : "airflow_${layer}"]
    ))
    error_message = "Datasets must be exactly the prod layers plus their airflow_ twins."
  }

  assert {
    condition     = alltrue([for ds in google_bigquery_dataset.this : ds.location == "EU"])
    error_message = "All datasets live in EU (dbt profiles and the dashboard query there)."
  }

  # The BigQuery sandbox left a 60-day expiry on raw and seeds, which would have
  # deleted the prod raw tables (D46)
  assert {
    condition = alltrue([
      for ds in google_bigquery_dataset.this :
      ds.default_table_expiration_ms == null && ds.default_partition_expiration_ms == null
    ])
    error_message = "No dataset may set a default table or partition expiry: tables would be deleted."
  }
}

run "grants" {
  command = plan

  # Datasets are only ever granted read access
  assert {
    condition     = alltrue([for g in google_bigquery_dataset_iam_member.reader : g.role == "roles/bigquery.dataViewer"])
    error_message = "Dataset grants must be roles/bigquery.dataViewer."
  }

  # No service account gets a basic role on the project
  assert {
    condition = alltrue([
      for m in google_project_iam_member.this :
      !contains(["roles/owner", "roles/editor", "roles/viewer"], m.role)
    ])
    error_message = "Service accounts must not hold owner / editor / viewer on the project."
  }

  # PR CI (olist-ci) reads only what a build or slim CI's --defer reads: no
  # Elementary history, no Airflow datasets, no write role (D45)
  assert {
    condition = alltrue([
      for k in keys(google_bigquery_dataset_iam_member.reader) :
      !(endswith(k, "/olist-ci") && (startswith(k, "airflow_") || startswith(k, "elementary/")))
    ])
    error_message = "olist-ci must not read elementary or airflow_* datasets."
  }

  assert {
    condition     = [for k, m in google_project_iam_member.this : m.role if startswith(k, "olist-ci/")] == ["roles/bigquery.user"]
    error_message = "olist-ci's only project role is roles/bigquery.user."
  }

  # The dashboard must be able to read the dataset its data usage agreement
  # names (consumer -> contract server -> dataset)
  assert {
    condition = contains(
      keys(google_bigquery_dataset_iam_member.reader),
      "${one([
        for s in yamldecode(file("../data-product/contracts/olist-marts.odcs.yaml")).servers :
        s.dataset if s.server == yamldecode(file("../data-product/consumers/olist-dashboard.yaml")).provider.server
      ])}/${yamldecode(file("../data-product/consumers/olist-dashboard.yaml")).provider.role}"
    )
    error_message = "The dashboard's service account lacks read access to the dataset its agreement names."
  }
}

run "cost_and_state" {
  command = plan

  # Hard daily cap stays inside BigQuery's free 1 TiB of queries per month
  assert {
    condition     = tonumber(google_service_usage_consumer_quota_override.bigquery_query_per_day.override_value) <= 1048576 / 30
    error_message = "Daily query quota must stay at or below 1/30 of the free TiB (MiB)."
  }

  assert {
    condition     = length(google_billing_budget.monthly.threshold_rules) > 0
    error_message = "The budget must alert at some threshold."
  }

  assert {
    condition = (
      google_storage_bucket.tfstate.versioning[0].enabled
      && google_storage_bucket.tfstate.public_access_prevention == "enforced"
      && google_storage_bucket.tfstate.uniform_bucket_level_access
    )
    error_message = "State bucket must be versioned, private and use uniform access."
  }
}

run "github_federation" {
  command = plan

  # One binding per account: prod writer from the prod environment, olist-ci
  # from pull requests, the drift check's reader from its environment. Nothing
  # else may impersonate any of them.
  assert {
    condition     = toset(keys(google_service_account_iam_member.github)) == toset(["dbt-runner", "olist-ci", "terraform-plan"])
    error_message = "Only dbt-runner, olist-ci and terraform-plan are reachable from GitHub Actions."
  }

  assert {
    condition     = alltrue([for m in google_service_account_iam_member.github : m.role == "roles/iam.workloadIdentityUser"])
    error_message = "GitHub bindings grant roles/iam.workloadIdentityUser only."
  }

  # The prod writer is never reachable from a pull request (D45)
  assert {
    condition = (
      local.github_identities["dbt-runner"] == "${local.github_subject_prefix}:environment:prod"
      && local.github_identities["olist-ci"] == "${local.github_subject_prefix}:pull_request"
      && local.github_identities["terraform-plan"] == "${local.github_subject_prefix}:environment:drift-check"
    )
    error_message = "dbt-runner binds to the prod environment only, olist-ci to pull requests only, terraform-plan to drift-check only."
  }

  # Environment subjects are accepted from main only
  assert {
    condition = alltrue([
      for s in values(local.github_identities) :
      contains(local.github_main_only_subjects, s) if strcontains(s, ":environment:")
    ])
    error_message = "Every environment subject must be in the main-only list of the provider condition."
  }

  # Exact subjects of this repository: no wildcard, no other repository
  assert {
    condition = alltrue([
      for s in values(local.github_identities) :
      startswith(s, "repo:ShuCiG@90512692/olist-analytics-dbt@1413000102:") && !strcontains(s, "*")
    ])
    error_message = "Subjects must be exact subjects of this repository."
  }

  assert {
    condition = (
      google_iam_workload_identity_pool_provider.github.oidc[0].issuer_uri == "https://token.actions.githubusercontent.com"
      && strcontains(google_iam_workload_identity_pool_provider.github.attribute_condition, "assertion.repository_id == '1413000102'")
      && strcontains(google_iam_workload_identity_pool_provider.github.attribute_condition, "assertion.ref == 'refs/heads/main'")
    )
    error_message = "The provider accepts GitHub tokens of this repository only, prod tokens from main only."
  }

  # Impersonation is granted per account above, never project-wide
  assert {
    condition = alltrue([
      for m in google_project_iam_member.this :
      !contains(["roles/iam.workloadIdentityUser", "roles/iam.serviceAccountTokenCreator", "roles/iam.serviceAccountUser"], m.role)
    ])
    error_message = "No project-level impersonation roles."
  }
}

run "drift_check" {
  command = plan

  # The drift check's account reads and never writes: viewer-type roles only
  # (D60). serviceUsageConsumer lets it bill its API calls to the project.
  assert {
    condition = alltrue([
      for m in google_project_iam_member.terraform_plan :
      can(regex("(Viewer|Reviewer|serviceUsageConsumer)$", m.role))
    ])
    error_message = "terraform-plan may hold viewer roles only on the project."
  }

  assert {
    condition = (
      toset([for m in google_storage_bucket_iam_member.terraform_plan : m.role]) == toset(["roles/storage.legacyBucketReader", "roles/storage.objectViewer"])
      && google_billing_account_iam_member.terraform_plan.role == "roles/billing.viewer"
    )
    error_message = "terraform-plan reads the state bucket and the billing account, nothing more."
  }
}
