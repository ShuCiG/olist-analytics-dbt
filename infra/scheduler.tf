# Weekly engine runs on time (D95). GitHub's `schedule` event is best effort: on this
# repo it fired 4-8 h late and may drop runs under load. Cloud Scheduler starts a
# Cloud Workflows execution at the exact minute; the workflow reads a GitHub token
# from Secret Manager and calls the `workflow_dispatch` API for one workflow file.
# A missed or failed dispatch shows up in the daily drift check (job weekly-runs in
# .github/workflows/drift.yml), which stays on GitHub's schedule on purpose: the
# watchdog must not depend on what it watches.
#
# The token's value never passes through Terraform (it would sit in the state):
# this file makes the empty secret, the owner adds a version with gcloud
# (docs/RUNNING.md, "Weekly runs"). Free tier: 3 Scheduler jobs per billing
# account, Workflows and Secret Manager far below their free quotas.

locals {
  dispatch_region = "europe-west1"

  # Scheduler job -> workflow file and cron (UTC); off the full hour like the old
  # GitHub schedules
  weekly_dispatches = {
    weekly-databricks = { workflow = "databricks.yml", cron = "17 6 * * 1" } # Mondays 06:17
    weekly-snowflake  = { workflow = "snowflake.yml", cron = "37 6 * * 1" }  # Mondays 06:37
  }
}

resource "google_service_account" "github_dispatch" {
  account_id   = "github-dispatch"
  display_name = "github-dispatch"
  description  = "Cloud Scheduler + Workflows: starts the weekly GitHub workflows (D95)"
}

# Fine-grained GitHub token, this repository only, permission Actions: read and write
resource "google_secret_manager_secret" "github_dispatch_token" {
  secret_id = "github-dispatch-token"

  replication {
    user_managed {
      replicas {
        location = local.dispatch_region
      }
    }
  }

  depends_on = [google_project_service.this]
}

resource "google_secret_manager_secret_iam_member" "github_dispatch" {
  secret_id = google_secret_manager_secret.github_dispatch_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.github_dispatch.member
}

# Scheduler starts executions as github-dispatch; the workflow itself runs as it too
resource "google_project_iam_member" "github_dispatch_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = google_service_account.github_dispatch.member
}

resource "google_workflows_workflow" "github_dispatch" {
  name            = "github-dispatch"
  region          = local.dispatch_region
  description     = "workflow_dispatch of one weekly GitHub workflow on main (D95)"
  service_account = google_service_account.github_dispatch.id
  source_contents = file("${path.module}/workflows/github_dispatch.yaml")

  depends_on = [google_project_service.this]
}

resource "google_cloud_scheduler_job" "weekly" {
  for_each = local.weekly_dispatches

  name      = each.key
  region    = local.dispatch_region
  schedule  = each.value.cron
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.github_dispatch.id}/executions"
    body        = base64encode(jsonencode({ argument = jsonencode({ workflow = each.value.workflow }) }))
    headers     = { "Content-Type" = "application/json" }

    oauth_token {
      service_account_email = google_service_account.github_dispatch.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_project_iam_member.github_dispatch_invoker]
}
