terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.84"
    }
  }

  backend "gcs" {
    bucket  = "credix-tfstate-bucket"
    prefix  = "terraform/state"
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
// credentials = "${path.module}/dev-sa-credentials.json"
}

# Enable required APIs for Datastream
resource "google_project_service" "datastream_api" {
  service = "datastream.googleapis.com"
}

resource "google_project_service" "dataflow_api" {
  service = "dataflow.googleapis.com"
}

resource "google_project_service" "compute_api" {
  service = "compute.googleapis.com"
}

# IAM Module
module "iam" {
  source = "../../modules/iam"
  
  project_id = var.project_id
}

# BigQuery Module
module "bigquery" {
  source = "../../modules/bigquery"
  
  bronze_dataset_id     = var.bronze_dataset_id
  silver_dataset_id     = var.silver_dataset_id
  gold_dataset_id       = var.gold_dataset_id
  temp_dataset_id       = var.temp_dataset_id
  elementary_dataset_id = var.elementary_dataset_id
  region                = var.region
}

# GCS Storage Module
module "gcs" {
  source = "../../modules/gcs"
  
  raw_bucket_name     = var.raw_bucket_name
  archive_bucket_name = var.archive_bucket_name
  region              = var.region
}

# VPC Module for Datastream
module "vpc" {
  source = "../../modules/vpc"
  
  region = var.region
}

# Datastream Module
module "datastream" {
  source = "../../modules/datastream"
  
  project_id         = var.project_id
  region             = var.region
  postgres_username  = var.postgres_username
  postgres_password  = var.postgres_password
  postgres_database  = var.postgres_database
  service_account_key = module.iam.service_account_key
  
  # Depend on VPC module
  vpc_network    = module.vpc.vpc_network
  vpc_subnet     = module.vpc.vpc_subnet
  vpc_network_id = module.vpc.vpc_network_id
}