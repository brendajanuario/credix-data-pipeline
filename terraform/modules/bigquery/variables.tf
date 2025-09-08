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

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}
