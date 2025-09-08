variable "raw_bucket_name" {
  description = "Name for the raw data lake bucket"
  type        = string
}

variable "archive_bucket_name" {
  description = "Name for the archive data lake bucket"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}
