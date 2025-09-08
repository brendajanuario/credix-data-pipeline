# Cloud Storage Buckets for Data Lake
resource "google_storage_bucket" "raw_bucket" {
  name     = var.raw_bucket_name
  location = var.region
  
  uniform_bucket_level_access = true
  force_destroy = true  # Allow deletion with objects
  
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
  force_destroy = true  # Allow deletion with objects
  
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

resource "google_storage_bucket" "elementary_bucket" {
  name     = "credix-elementary-report"
  location = var.region

  uniform_bucket_level_access = true
  force_destroy = true  # Allow deletion with objects

  versioning {
    enabled = true
  }

  # 1. Configura o bucket como um site estático.
  website {
    main_page_suffix = "index.html"
  }
}

# 2. Torna o bucket acessível publicamente para visualização.
# O objeto `elementary_report.html` será lido por `allUsers`.
resource "google_storage_bucket_iam_member" "public_report_access" {
  bucket = google_storage_bucket.elementary_bucket.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}