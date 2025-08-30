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