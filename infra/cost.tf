# Cost guards. The public dataset is small (a full build scans well under
# 1 GiB), so both limits sit far above normal use and inside the free tier.

# Email alerts to the billing account's admins. Covers the whole billing
# account, not only this project.
resource "google_billing_budget" "monthly" {
  billing_account = var.billing_account
  display_name    = "zł2 Monthly Budget Alert"

  amount {
    specified_amount {
      currency_code = "PLN"
      units         = "2"
    }
  }

  budget_filter {
    calendar_period        = "MONTH"
    credit_types_treatment = "INCLUDE_ALL_CREDITS"
  }

  dynamic "threshold_rules" {
    for_each = [0.5, 0.9, 1.0, 1.5]
    content {
      threshold_percent = threshold_rules.value
      spend_basis       = "CURRENT_SPEND"
    }
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  depends_on = [google_project_service.this]
}

# Hard stop: BigQuery "Query usage per day" for the project, in MiB (~30 GiB).
# Queries over it fail until the next day.
resource "google_service_usage_consumer_quota_override" "bigquery_query_per_day" {
  provider = google-beta

  project        = var.project_id
  service        = "bigquery.googleapis.com"
  metric         = urlencode("bigquery.googleapis.com/quota/query/usage")
  limit          = urlencode("/d/project")
  override_value = "31457"
  force          = true # a decrease over 10 % needs it
}
