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


output "elementary_dataset_id" {
  description = "BigQuery Elementary dataset ID"
  value       = google_bigquery_dataset.elementary.dataset_id
}

output "service_account_email" {
  description = "Email of the Credix pipeline service account"
  value       = google_service_account.credix_pipeline.email
}

output "service_account_key_path" {
  description = "Path to the service account key file"
  value       = "/Users/jemzin/Github/credix-pipeline-key.json"
  sensitive   = true
}

# Datastream outputs
output "datastream_bucket_name" {
  description = "Name of the GCS bucket for Datastream output"
  value       = google_storage_bucket.datastream_bucket.name
}

output "datastream_raw_dataset" {
  description = "BigQuery dataset for raw Datastream data"
  value       = google_bigquery_dataset.datastream_raw.dataset_id
}

output "datastream_processed_dataset" {
  description = "BigQuery dataset for processed Datastream data"
  value       = google_bigquery_dataset.datastream_processed.dataset_id
}

output "postgres_vm_external_ip" {
  description = "External IP of the PostgreSQL VM"
  value       = google_compute_instance.postgres_vm.network_interface[0].access_config[0].nat_ip
}

output "postgres_vm_internal_ip" {
  description = "Internal IP of the PostgreSQL VM (use this for Datastream)"
  value       = google_compute_instance.postgres_vm.network_interface[0].network_ip
}

output "postgres_vm_ssh_command" {
  description = "SSH command to connect to the VM"
  value       = "gcloud compute ssh postgres-dagster-vm --zone=${var.region}-a"
}

output "dagster_ui_url" {
  description = "Dagster Web UI URL"
  value       = "http://${google_compute_instance.postgres_vm.network_interface[0].access_config[0].nat_ip}:3000"
}

output "postgres_connection_string" {
  description = "PostgreSQL connection string for Datastream"
  value       = "postgresql://postgres:postgres123@${google_compute_instance.postgres_vm.network_interface[0].network_ip}:5432/credix_transactions"
  sensitive   = true
}

output "cnpj_stream_id" {
  description = "Datastream stream ID for CNPJ data"
  value       = google_datastream_stream.cnpj_stream.id
}

output "installments_stream_id" {
  description = "Datastream stream ID for installments data"
  value       = google_datastream_stream.installments_stream.id
}
