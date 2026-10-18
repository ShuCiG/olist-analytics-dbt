# Everything below existed before Terraform (set up by hand in the console,
# docs/DECISIONS.md D17, D27, D45). Import blocks adopt it into the state on
# the first apply instead of creating duplicates; once imported they are
# no-ops and stay as a record of the adoption. Only the state bucket
# (state.tf) is new.

import {
  for_each = local.services
  to       = google_project_service.this[each.value]
  id       = "${var.project_id}/${each.value}"
}

import {
  for_each = local.service_accounts
  to       = google_service_account.this[each.key]
  id       = "projects/${var.project_id}/serviceAccounts/${each.key}@${var.project_id}.iam.gserviceaccount.com"
}

import {
  for_each = local.project_roles
  to       = google_project_iam_member.this[each.key]
  id       = "${var.project_id} ${each.value.role} serviceAccount:${each.value.sa}@${var.project_id}.iam.gserviceaccount.com"
}

import {
  for_each = local.datasets
  to       = google_bigquery_dataset.this[each.value]
  id       = "projects/${var.project_id}/datasets/${each.value}"
}

import {
  for_each = local.dataset_reader_grants
  to       = google_bigquery_dataset_iam_member.reader[each.key]
  id       = "projects/${var.project_id}/datasets/${each.value.dataset} roles/bigquery.dataViewer serviceAccount:${each.value.sa}@${var.project_id}.iam.gserviceaccount.com"
}

import {
  to = google_billing_budget.monthly
  id = "billingAccounts/${var.billing_account}/budgets/18c5caf2-742e-4b33-aeee-7d0da92fbcfc"
}

import {
  to = google_service_usage_consumer_quota_override.bigquery_query_per_day
  id = "projects/${var.project_id}/services/bigquery.googleapis.com/consumerQuotaMetrics/${urlencode("bigquery.googleapis.com/quota/query/usage")}/limits/${urlencode("/d/project")}/consumerOverrides/Cg1RdW90YU92ZXJyaWRl"
}
