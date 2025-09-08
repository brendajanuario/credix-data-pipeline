# GCS bucket for Datastream output
resource "google_storage_bucket" "datastream_bucket" {
  name          = "${var.project_id}-datastream-output"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery datasets for Datastream
resource "google_bigquery_dataset" "datastream_raw" {
  dataset_id  = "datastream_raw"
  description = "Raw data from Datastream"
  location    = var.region

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "datastream_processed" {
  dataset_id  = "datastream_processed"
  description = "Processed data from Datastream"
  location    = var.region

  delete_contents_on_destroy = true
}

# Private connection for PostgreSQL
resource "google_datastream_private_connection" "postgres_connection" {
  display_name          = "postgres-private-connection"
  location              = var.region
  private_connection_id = "postgres-connection"

  vpc_peering_config {
    vpc    = var.vpc_network_id
    subnet = "10.1.0.0/29"  # Different range to avoid overlap
  }
}

# Connection profile for PostgreSQL source
resource "google_datastream_connection_profile" "postgres_profile" {
  display_name         = "postgres-source-profile"
  location             = var.region
  connection_profile_id = "postgres-source"

  postgresql_profile {
    hostname = google_compute_instance.postgres_vm.network_interface[0].network_ip
    port     = "5432"
    username = var.postgres_username
    password = var.postgres_password
    database = var.postgres_database
  }

  private_connectivity {
    private_connection = google_datastream_private_connection.postgres_connection.id
  }
}

# Connection profile for GCS destination
resource "google_datastream_connection_profile" "gcs_profile" {
  display_name         = "gcs-destination-profile"
  location             = var.region
  connection_profile_id = "gcs-destination"

  gcs_profile {
    bucket    = google_storage_bucket.datastream_bucket.name
    root_path = "/datastream-output"
  }
}
