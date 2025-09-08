# GCP VM for PostgreSQL Docker simulation
resource "google_compute_instance" "postgres_vm" {
  name         = "postgres-dagster-vm"
  machine_type = "e2-micro"  # 1 vCPU, 1GB RAM - smallest possible (~$5/month)
  zone         = "${var.region}-a"

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"  # Container-Optimized OS
      size  = 20
    }
  }

  network_interface {
    network    = var.vpc_network
    subnetwork = var.vpc_subnet
    
    access_config {
      # Ephemeral public IP for SSH access
    }
  }

  # Pass the service account key as metadata
  metadata = {
    google-cloud-credentials = base64decode(var.service_account_key)
  }

  # Use docker-compose to start the full pipeline
  metadata_startup_script = <<-EOF
    #!/bin/bash
    
    # Get service account key from metadata
    curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/google-cloud-credentials" -H "Metadata-Flavor: Google" > /opt/service-account-key.json
    chmod 600 /opt/service-account-key.json
    
    # Install Python, pip, and docker-compose
    apt-get update
    apt-get install -y python3 python3-pip git docker-compose-plugin
    
    # Clone your repository
    cd /opt
    git clone https://github.com/brendajanuario/credix-data-pipeline.git
    cd credix-data-pipeline
    
    # Install Python dependencies and Dagster/dbt
    pip3 install --upgrade pip
    pip3 install -r requirements.txt
    pip3 install -e credix_pipeline/credix_pipeline/
    
    # Install additional dependencies for Dagster and dbt
    pip3 install dagster dagster-webserver dagster-postgres dagster-gcp
    pip3 install dbt-core dbt-bigquery
    
    # Set environment variables for the session and for Dagster
    cat >> /etc/environment << 'ENV_EOF'
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=credix_transactions
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres123
GCP_PROJECT=${var.project_id}
GOOGLE_APPLICATION_CREDENTIALS=/opt/service-account-key.json
ENV_EOF

    # Set environment variables for current session
    export POSTGRES_HOST=localhost
    export POSTGRES_PORT=5432
    export POSTGRES_DB=credix_transactions
    export POSTGRES_USER=postgres
    export POSTGRES_PASSWORD=postgres123
    export GCP_PROJECT=${var.project_id}
    export GOOGLE_APPLICATION_CREDENTIALS=/opt/service-account-key.json
    
    # The data directory already contains the parquet files in the repo
    # No need to create sample files - use existing ones from data/
    
    # Start PostgreSQL with logical replication using docker-compose
    # First, update the postgres service to enable logical replication
    cat > docker-compose.override.yml << 'OVERRIDE_EOF'
services:
  postgres:
    command: postgres -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 -c max_connections=100
OVERRIDE_EOF
    
    # Start PostgreSQL
    docker compose up -d postgres
    
    # Wait for PostgreSQL to be ready
    timeout 60 bash -c 'until docker compose exec postgres pg_isready -U postgres; do sleep 2; done'
    
    # Load data using the data loader
    docker compose --profile data-loader up --build data_loader
    
    # Create oltp schema if it doesn't exist (data loader uses oltp schema)
    docker compose exec postgres psql -U postgres -d credix_transactions -c "CREATE SCHEMA IF NOT EXISTS oltp;"
    
    # Create Datastream user with replication privileges
    docker compose exec postgres psql -U postgres -d credix_transactions -c "CREATE USER datastream_user WITH REPLICATION PASSWORD 'datastream123';"
    docker compose exec postgres psql -U postgres -d credix_transactions -c "GRANT CONNECT ON DATABASE credix_transactions TO datastream_user;"
    docker compose exec postgres psql -U postgres -d credix_transactions -c "GRANT USAGE ON SCHEMA oltp TO datastream_user;"
    docker compose exec postgres psql -U postgres -d credix_transactions -c "GRANT SELECT ON ALL TABLES IN SCHEMA oltp TO datastream_user;"
    docker compose exec postgres psql -U postgres -d credix_transactions -c "ALTER DEFAULT PRIVILEGES IN SCHEMA oltp GRANT SELECT ON TABLES TO datastream_user;"
    
    # Create publication for Datastream (for tables in oltp schema)
    docker compose exec postgres psql -U postgres -d credix_transactions -c "CREATE PUBLICATION datastream_publication FOR ALL TABLES;"
    
    # Create replication slot for Datastream
    docker compose exec postgres psql -U postgres -d credix_transactions -c "SELECT pg_create_logical_replication_slot('datastream_slot', 'pgoutput');"
    
    # Start Dagster with proper environment
    cd /opt/credix-data-pipeline
    
    # Source environment variables
    source /etc/environment
    
    # Start Dagster in the background with logging
    nohup python3 -m dagster dev -m credix_pipeline.credix_pipeline.definitions --host 0.0.0.0 --port 3000 > /tmp/dagster.log 2>&1 &
    
    # Wait a moment for Dagster to start
    sleep 10
    
    echo "Setup complete! PostgreSQL with data loaded, Dagster on :3000"
    echo "Tables loaded in oltp schema:"
    docker compose exec postgres psql -U postgres -d credix_transactions -c "\\dt oltp.*"
    
    echo "Dagster UI available at: http://$(curl -s ifconfig.me):3000"
    echo "Check Dagster logs: tail -f /tmp/dagster.log"
  EOF

  tags = ["postgres", "datastream-test"]

  service_account {
    scopes = ["cloud-platform"]
  }
}
