# Cloud Storage Buckets for Data Lake
resource "google_storage_bucket" "raw_bucket" {
  name     = var.raw_bucket_name
  location = var.region
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
  
  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "archive_bucket" {
  name     = var.archive_bucket_name
  location = var.region
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 365  # Archive for 1 year
    }
    action {
      type = "Delete"
    }
  }
  
  versioning {
    enabled = true
  }
}
