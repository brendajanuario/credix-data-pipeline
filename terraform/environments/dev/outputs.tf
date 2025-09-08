output "raw_bucket_name" {
  description = "Name of the raw data lake bucket"
  value       = module.gcs.raw_bucket_name
}

output "archive_bucket_name" {
  description = "Name of the archive data lake bucket"
  value       = module.gcs.archive_bucket_name
}

output "bronze_dataset_id" {
  description = "BigQuery Bronze dataset ID"
  value       = module.bigquery.bronze_dataset_id
}

output "silver_dataset_id" {
  description = "BigQuery Silver dataset ID"
  value       = module.bigquery.silver_dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery Gold dataset ID"
  value       = module.bigquery.gold_dataset_id
}

output "temp_dataset_id" {
  description = "BigQuery Temp dataset ID"
  value       = module.bigquery.temp_dataset_id
}

output "elementary_dataset_id" {
  description = "BigQuery Elementary dataset ID"
  value       = module.bigquery.elementary_dataset_id
}

output "service_account_email" {
  description = "Service account email"
  value       = module.iam.service_account_email
}

output "service_account_key_path" {
  description = "Path to the service account key file"
  value       = module.iam.service_account_key_path
  sensitive   = true
}

output "datastream_bucket_name" {
  description = "Name of the Datastream output bucket"
  value       = module.datastream.datastream_bucket_name
}

output "datastream_raw_dataset" {
  description = "Datastream raw dataset ID"
  value       = module.datastream.datastream_raw_dataset
}

output "datastream_processed_dataset" {
  description = "Datastream processed dataset ID"
  value       = module.datastream.datastream_processed_dataset
}

output "postgres_vm_external_ip" {
  description = "External IP of the PostgreSQL VM"
  value       = module.datastream.postgres_vm_external_ip
}

output "postgres_vm_internal_ip" {
  description = "Internal IP of the PostgreSQL VM"
  value       = module.datastream.postgres_vm_internal_ip
}

output "postgres_vm_ssh_command" {
  description = "SSH command to connect to PostgreSQL VM"
  value       = module.datastream.postgres_vm_ssh_command
}

output "dagster_ui_url" {
  description = "URL for Dagster UI"
  value       = module.datastream.dagster_ui_url
}

output "postgres_connection_string" {
  description = "PostgreSQL connection string"
  value       = module.datastream.postgres_connection_string
  sensitive   = true
}

output "cnpj_stream_id" {
  description = "CNPJ Datastream stream ID"
  value       = module.datastream.cnpj_stream_id
}

output "installments_stream_id" {
  description = "Installments Datastream stream ID"
  value       = module.datastream.installments_stream_id
}
