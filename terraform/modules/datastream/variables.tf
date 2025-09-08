variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

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

variable "vpc_network" {
  description = "The VPC network name"
  type        = string
}

variable "vpc_subnet" {
  description = "The VPC subnet name"
  type        = string
}

variable "vpc_network_id" {
  description = "The VPC network ID"
  type        = string
}

variable "service_account_key" {
  description = "Service account key for VM metadata"
  type        = string
  sensitive   = true
}
