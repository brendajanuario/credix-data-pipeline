variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "us-central1-a"
}

# Storage Configuration
variable "raw_bucket_name" {
  description = "Name for the raw data lake bucket"
  type        = string
}

variable "archive_bucket_name" {
  description = "Name for the archive data lake bucket"
  type        = string
}

# BigQuery Datasets
variable "bronze_dataset_id" {
  description = "BigQuery dataset for Bronze layer"
  type        = string
  default     = "business_case_bronze"
}

variable "silver_dataset_id" {
  description = "BigQuery dataset for Silver layer"
  type        = string
  default     = "business_case_silver"
}

variable "gold_dataset_id" {
  description = "BigQuery dataset for Gold layer"
  type        = string
  default     = "business_case_gold"
}

variable "temp_dataset_id" {
  description = "BigQuery dataset for Temp layer"
  type        = string
  default     = "business_case_temp"
}

variable "elementary_dataset_id" {
  description = "BigQuery dataset for Elementary layer"
  type        = string
  default     = "business_case_elementary"
}

# Datastream Variables
variable "postgres_username" {
  description = "PostgreSQL username for Datastream"
  type        = string
  default     = "datastream_user"
}

variable "postgres_password" {
  description = "PostgreSQL password for Datastream user"
  type        = string
  sensitive   = true
  default     = "datastream123"
}

variable "postgres_database" {
  description = "PostgreSQL database name"
  type        = string
  default     = "credix_transactions"
}