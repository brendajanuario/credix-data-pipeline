# Service Account for Credix Data Pipeline
resource "google_service_account" "credix_pipeline" {
  account_id   = "credix-pipeline"
  display_name = "Credix Data Pipeline Service Account"
  description  = "Service account for Credix data pipeline operations"
}

# IAM roles for the service account
resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.credix_pipeline.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.credix_pipeline.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.credix_pipeline.email}"
}

resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.credix_pipeline.email}"
}

# Service Account Key for local development
resource "google_service_account_key" "credix_pipeline_key" {
  service_account_id = google_service_account.credix_pipeline.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# Store the key locally for dbt and other tools
resource "local_file" "service_account_key" {
  content  = base64decode(google_service_account_key.credix_pipeline_key.private_key)
  filename = "../secrets/credix-pipeline-key.json"
}
