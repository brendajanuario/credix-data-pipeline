output "datastream_bucket_name" {
  description = "Name of the Datastream output bucket"
  value       = google_storage_bucket.datastream_bucket.name
}

output "datastream_raw_dataset" {
  description = "Datastream raw dataset ID"
  value       = google_bigquery_dataset.datastream_raw.dataset_id
}

output "datastream_processed_dataset" {
  description = "Datastream processed dataset ID"
  value       = google_bigquery_dataset.datastream_processed.dataset_id
}

output "postgres_vm_external_ip" {
  description = "External IP of the PostgreSQL VM"
  value       = google_compute_instance.postgres_vm.network_interface[0].access_config[0].nat_ip
}

output "postgres_vm_internal_ip" {
  description = "Internal IP of the PostgreSQL VM"
  value       = google_compute_instance.postgres_vm.network_interface[0].network_ip
}

output "postgres_vm_ssh_command" {
  description = "SSH command to connect to PostgreSQL VM"
  value       = "gcloud compute ssh postgres-dagster-vm --zone=${var.region}-a"
}

output "dagster_ui_url" {
  description = "URL for Dagster UI"
  value       = "http://${google_compute_instance.postgres_vm.network_interface[0].access_config[0].nat_ip}:3000"
}

output "postgres_connection_string" {
  description = "PostgreSQL connection string"
  value       = "postgresql://postgres:postgres123@${google_compute_instance.postgres_vm.network_interface[0].network_ip}:5432/credix_transactions"
  sensitive   = true
}

output "cnpj_stream_id" {
  description = "CNPJ Datastream stream ID"
  value       = google_datastream_stream.cnpj_stream.id
}

output "installments_stream_id" {
  description = "Installments Datastream stream ID"
  value       = google_datastream_stream.installments_stream.id
}
