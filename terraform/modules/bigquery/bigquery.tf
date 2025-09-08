# BigQuery Datasets for Medallion Architecture

# Bronze Layer Dataset (Raw Data)
resource "google_bigquery_dataset" "bronze" {
  dataset_id  = var.bronze_dataset_id
  description = "Bronze layer - Raw data from source systems"
  location    = var.region
  
  delete_contents_on_destroy = true

  labels = {
    layer = "bronze"
    env   = "production"
  }
}

# Silver Layer Dataset (Cleansed & Processed Data)
resource "google_bigquery_dataset" "silver" {
  dataset_id  = var.silver_dataset_id
  description = "Silver layer - Cleansed and standardized data"
  location    = var.region
  
  delete_contents_on_destroy = true

  labels = {
    layer = "silver"
    env   = "production"
  }
}

# Gold Layer Dataset (Business-ready Data)
resource "google_bigquery_dataset" "gold" {
  dataset_id  = var.gold_dataset_id
  description = "Gold layer - Business-ready aggregated data"
  location    = var.region
  
  delete_contents_on_destroy = true

  labels = {
    layer = "gold"
    env   = "production"
  }
}


# Temporary layer to merge in Bronze dataset
resource "google_bigquery_dataset" "temp" {
  dataset_id  = var.temp_dataset_id
  description = "Temp layer - Temporary data to be merged into the bronze layer"
  location    = var.region
  
  delete_contents_on_destroy = true

  labels = {
    layer = "temp"
    env   = "production"
  }
}


# Elementary bucket
resource "google_bigquery_dataset" "elementary" {
  dataset_id  = var.elementary_dataset_id
  description = "Elementary layer - Basic, raw data"
  location    = var.region
  
  delete_contents_on_destroy = true

  labels = {
    layer = "elementary"
    env   = "production"
  }
}