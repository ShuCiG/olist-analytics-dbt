# Remote state for this configuration. Versioned, so an overwritten state can
# be restored; no public access.
resource "google_storage_bucket" "tfstate" {
  name                        = "${var.project_id}-tfstate"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 10
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}
