# Project ID
project_id = "product-reliability-analyzer"

# GCP Region and Zone
region = "us-central1"  # Fixed: was "us" which is invalid
zone   = "us-central1-a"

# Storage Configuration
raw_bucket_name     = "data_lake_credix"
archive_bucket_name = "data_lake_credix_archive"

# BigQuery Datasets
bronze_dataset_id = "business_case_bronze"
silver_dataset_id = "business_case_silver"
gold_dataset_id   = "business_case_gold"
temp_dataset_id   = "business_case_temp"
elementary_dataset_id = "business_case_elementary"

# Datastream PostgreSQL Configuration
postgres_username = "postgres"
postgres_password = "postgres123"
postgres_database = "credix_transactions"