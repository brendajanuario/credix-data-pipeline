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

output "elementary_dataset_id" {
  description = "BigQuery Elementary dataset ID"
  value       = google_bigquery_dataset.elementary.dataset_id
}
