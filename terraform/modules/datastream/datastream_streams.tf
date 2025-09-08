# Datastream stream for CNPJ table
resource "google_datastream_stream" "cnpj_stream" {
  display_name = "cnpj-postgres-to-gcs"
  location     = var.region
  stream_id    = "cnpj-stream"

  source_config {
    source_connection_profile = google_datastream_connection_profile.postgres_profile.id
    postgresql_source_config {
      include_objects {
        postgresql_schemas {
          schema = "oltp"
          postgresql_tables {
            table = "cnpj_ws"
            postgresql_columns {
              column = "buyer_tax_id"
            }
            postgresql_columns {
              column = "share_capital"
            }
            postgresql_columns {
              column = "company_size"
            }
            postgresql_columns {
              column = "legal_nature"
            }
            postgresql_columns {
              column = "simples_option"
            }
            postgresql_columns {
              column = "is_mei"
            }
            postgresql_columns {
              column = "is_main_company"
            }
            postgresql_columns {
              column = "company_status"
            }
            postgresql_columns {
              column = "is_active"
            }
            postgresql_columns {
              column = "zipcode"
            }
            postgresql_columns {
              column = "main_cnae"
            }
            postgresql_columns {
              column = "state"
            }
            postgresql_columns {
              column = "uf"
            }
            postgresql_columns {
              column = "city"
            }
            postgresql_columns {
              column = "created_at"
            }
            postgresql_columns {
              column = "updated_at"
            }
          }
        }
      }
      publication      = "datastream_publication"
      replication_slot = "datastream_slot"
    }
  }

  destination_config {
    destination_connection_profile = google_datastream_connection_profile.gcs_profile.id
    gcs_destination_config {
      path                = "/cnpj/"
      file_rotation_mb    = 100
      file_rotation_interval = "60s"
      avro_file_format {}
    }
  }

  backfill_all {
    postgresql_excluded_objects {
      postgresql_schemas {
        schema = "information_schema"
      }
    }
  }
}

# Datastream stream for installments table
resource "google_datastream_stream" "installments_stream" {
  display_name = "installments-postgres-to-gcs"
  location     = var.region
  stream_id    = "installments-stream"

  source_config {
    source_connection_profile = google_datastream_connection_profile.postgres_profile.id
    postgresql_source_config {
      include_objects {
        postgresql_schemas {
          schema = "oltp"
          postgresql_tables {
            table = "installments"
            postgresql_columns {
              column = "asset_id"
            }
            postgresql_columns {
              column = "invoice_id"
            }
            postgresql_columns {
              column = "buyer_tax_id"
            }
            postgresql_columns {
              column = "buyer_main_tax_id"
            }
            postgresql_columns {
              column = "original_amount_in_cents"
            }
            postgresql_columns {
              column = "expected_amount_in_cents"
            }
            postgresql_columns {
              column = "paid_amount_in_cents"
            }
            postgresql_columns {
              column = "due_date"
            }
            postgresql_columns {
              column = "paid_date"
            }
            postgresql_columns {
              column = "invoice_issue_date"
            }
          }
        }
      }
      publication      = "datastream_publication"
      replication_slot = "datastream_slot"
    }
  }

  destination_config {
    destination_connection_profile = google_datastream_connection_profile.gcs_profile.id
    gcs_destination_config {
      path                = "/installments/"
      file_rotation_mb    = 100
      file_rotation_interval = "60s"
      avro_file_format {}
    }
  }

  backfill_all {
    postgresql_excluded_objects {
      postgresql_schemas {
        schema = "information_schema"
      }
    }
  }
}
