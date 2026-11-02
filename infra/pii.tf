# Column-level security for the person-related columns of the marts (D94). The
# output contract classifies them `restricted`: the person id (customer_unique_id)
# and the customer's address (zip prefix, city, coordinates in dim_customers).
# dbt attaches the policy tags below to those columns on BigQuery; whoever reads a
# tagged column needs a role on its tag, dataset-level read alone gets "access denied".
# - person_id, person_location: Fine-Grained Readers only. The dashboard reads
#   neither: it counts and joins people on `customer_key`, a hash dbt adds next to
#   the id. BigQuery's own masking (a SHA-256 data policy for the dashboard) needs
#   the project to belong to an organization, which this one doesn't (D94).
# Taxonomies are regional: "eu" for the EU datasets.

locals {
  pii_location = lower(var.location)

  pii_tags = {
    person_id       = "The person behind an order (customer_unique_id), pseudonymized by Olist"
    person_location = "A person's address: zip prefix, city, coordinates of the zip centroid"
  }

  # Full access to every PII tag: the prod writer (dbt builds, Airflow, tests),
  # CI (builds and tests its own tagged tables, reads prod for --defer and the
  # data diff) and the project owner (from terraform.tfvars, not in the repo)
  pii_fine_grained_readers = merge(
    {
      dbt-runner = google_service_account.this["dbt-runner"].member
      olist-ci   = google_service_account.this["olist-ci"].member
    },
    var.owner_member == null ? {} : { owner = var.owner_member },
  )

  pii_reader_grants = merge([
    for tag in keys(local.pii_tags) : {
      for name, member in local.pii_fine_grained_readers : "${tag}/${name}" => { tag = tag, member = member }
    }
  ]...)
}

resource "google_data_catalog_taxonomy" "pii" {
  region                 = local.pii_location
  display_name           = "olist-pii"
  description            = "Person-related columns of the Olist marts (output contract: classification restricted)"
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]

  depends_on = [google_project_service.this]
}

resource "google_data_catalog_policy_tag" "pii" {
  for_each = local.pii_tags

  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = each.key
  description  = each.value
}

resource "google_data_catalog_policy_tag_iam_member" "fine_grained_reader" {
  for_each = local.pii_reader_grants

  policy_tag = google_data_catalog_policy_tag.pii[each.value.tag].name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = each.value.member
}

# Attaching a tag to a column needs bigquery.tables.setCategory (only in BigQuery
# Admin / Data Owner, far more than a writer needs) and datacatalog.taxonomies.get:
# a custom role with just these two, for the accounts that build tables (dbt)
resource "google_project_iam_custom_role" "policy_tag_user" {
  role_id     = "policyTagUser"
  title       = "BigQuery policy tag user"
  description = "Attach the project's policy tags to table columns (dbt builds, D94)"
  permissions = ["bigquery.tables.setCategory", "datacatalog.taxonomies.get"]
}

resource "google_project_iam_member" "policy_tag_user" {
  for_each = toset(["dbt-runner", "olist-ci"])

  project = var.project_id
  role    = google_project_iam_custom_role.policy_tag_user.name
  member  = google_service_account.this[each.value].member
}

output "pii_policy_tags" {
  description = "Policy tag resource names for dbt (transform/dbt_project.yml, var pii_policy_tags)"
  value       = { for tag, t in google_data_catalog_policy_tag.pii : tag => t.name }
}
