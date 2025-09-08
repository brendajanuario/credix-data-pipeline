output "raw_bucket_name" {
  description = "Name of the raw data lake bucket"
  value       = google_storage_bucket.raw_bucket.name
}

output "archive_bucket_name" {
  description = "Name of the archive data lake bucket"
  value       = google_storage_bucket.archive_bucket.name
}

output "elementary_bucket_name" {
  description = "Name of the elementary bucket"
  value       = google_storage_bucket.elementary_bucket.name
}
