output "service_account_email" {
  description = "Email of the Credix pipeline service account"
  value       = google_service_account.credix_pipeline.email
}

output "service_account_key" {
  description = "Service account key"
  value       = google_service_account_key.credix_pipeline_key.private_key
  sensitive   = true
}

output "service_account_key_path" {
  description = "Path to the service account key file"
  value       = "/Users/jemzin/Github/credix-pipeline-key.json"
  sensitive   = true
}
