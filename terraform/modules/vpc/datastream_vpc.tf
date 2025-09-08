# VPC Network for Datastream
resource "google_compute_network" "datastream_vpc" {
  name                    = "datastream-vpc"
  auto_create_subnetworks = false
}

# Subnet for the VPC
resource "google_compute_subnetwork" "datastream_subnet" {
  name          = "datastream-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.datastream_vpc.id
}

# Firewall rule to allow PostgreSQL traffic
resource "google_compute_firewall" "allow_postgres" {
  name    = "allow-postgres-datastream"
  network = google_compute_network.datastream_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }

  source_ranges = ["10.1.0.0/29", "10.0.0.0/24"]  # Datastream + VM subnet ranges
  target_tags   = ["postgres"]
}

# Firewall rule to allow SSH
resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh-postgres-vm"
  network = google_compute_network.datastream_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]  # Restrict this to your IP for security
  target_tags   = ["postgres"]
}

# Firewall rule to allow Dagster web UI
resource "google_compute_firewall" "allow_dagster" {
  name    = "allow-dagster-ui"
  network = google_compute_network.datastream_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }

  source_ranges = ["0.0.0.0/0"]  # Restrict this to your IP for security
  target_tags   = ["postgres"]
}
