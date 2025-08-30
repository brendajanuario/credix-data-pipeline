output "raw_bucket_name" {
  description = "Name of the raw data lake bucket"
  value       = google_storage_bucket.raw_bucket.name
}

output "archive_bucket_name" {
  description = "Name of the archive data lake bucket"
  value       = google_storage_bucket.archive_bucket.name
}

output "bronze_dataset_id" {
  description = "BigQuery Bronze dataset ID"
  value       = google_bigquery_dataset.bronze.dataset_id
}

output "silver_dataset_id" {
  description = "BigQuery Silver dataset ID"
  value       = google_bigquery_dataset.silver.dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery Gold dataset ID"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "temp_dataset_id" {
  description = "BigQuery Temp dataset ID"
  value       = google_bigquery_dataset.temp.dataset_id
}

output "service_account_email" {
  description = "Email of the Credix pipeline service account"
  value       = google_service_account.credix_pipeline.email
}

output "service_account_key_path" {
  description = "Path to the service account key file"
  value       = "../secrets/credix-pipeline-key.json"
  sensitive   = true
}
