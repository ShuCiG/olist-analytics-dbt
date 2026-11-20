# The marts as a listing in BigQuery sharing (Analytics Hub): a subscriber links
# the dataset into their own project, nothing is copied, and their queries are
# billed to them (D122). Text comes from the data product descriptor, so the
# listing and the product page say the same.
# - Shared: prod `marts` only (the Airflow feed is a replay for this project's
#   own demos). Person columns stay protected: the policy tags of pii.tf are
#   checked on the source tables, so a subscriber without the Fine-Grained
#   Reader role gets "access denied" on them (D94).
# - Who may subscribe: the members in var.listing_subscribers (roles/analyticshub.subscriber
#   on the exchange). The exchange is private: it is not listed publicly.

locals {
  product = yamldecode(file("${path.module}/../data-product/olist-commerce.odps.yaml"))

  listing_documentation = <<-EOT
    ${local.product.description.purpose}

    **Limitations.** ${local.product.description.limitations}

    **Usage.** ${local.product.description.usage}

    Person-related columns (person id, customer address) carry policy tags and
    are denied unless you hold the Fine-Grained Reader role on them. Select the
    other columns, or `customer_key`, the pseudonym to count and join people on.

    Contract, SLOs and lineage: https://shucig.github.io/olist-analytics-dbt/data-product/
  EOT
}

resource "google_bigquery_analytics_hub_data_exchange" "olist" {
  location         = var.location
  data_exchange_id = "olist_commerce"
  display_name     = local.product.name
  description      = trimspace(local.product.description.purpose)
  documentation    = local.listing_documentation

  # Private: only subscribers named below can see and link the listing
  sharing_environment_config {
    default_exchange_config {}
  }

  depends_on = [google_project_service.this]
}

resource "google_bigquery_analytics_hub_listing" "marts" {
  location         = var.location
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.olist.data_exchange_id
  listing_id       = "olist_marts"
  display_name     = "Olist marts"
  description      = trimspace(local.product.description.purpose)
  documentation    = local.listing_documentation
  categories       = ["CATEGORY_RETAIL"]
  discovery_type   = "DISCOVERY_TYPE_PRIVATE"

  bigquery_dataset {
    dataset = google_bigquery_dataset.this["marts"].id
  }
}

resource "google_bigquery_analytics_hub_data_exchange_iam_member" "subscriber" {
  for_each = toset(var.listing_subscribers)

  location         = var.location
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.olist.data_exchange_id
  role             = "roles/analyticshub.subscriber"
  member           = each.value
}

output "analytics_hub_listing" {
  description = "Resource name of the marts listing (data product output port, delivery channel)"
  value       = google_bigquery_analytics_hub_listing.marts.name
}
