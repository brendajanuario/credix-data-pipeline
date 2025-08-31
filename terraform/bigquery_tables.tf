# BigQuery table schemas for raw data ingestion

resource "google_bigquery_table" "business_case_cnpj_ws" {
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  table_id   = "cnpj_ws"
  
  deletion_protection = false

  schema = jsonencode([
    {
      name = "share_capital"
      type = "FLOAT"
      mode = "NULLABLE"
      description = "Company share capital"
    },
    {
      name = "company_size"
      type = "STRING"
      mode = "NULLABLE"
      description = "Company size classification"
    },
    {
      name = "legal_nature"
      type = "STRING"
      mode = "NULLABLE"
      description = "Legal nature of the company"
    },
    {
      name = "simples_option"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether company opted for Simples tax regime"
    },
    {
      name = "is_mei"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether company is MEI (Individual Microentrepreneur)"
    },
    {
      name = "is_main_company"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether this is the main company"
    },
    {
      name = "company_status"
      type = "STRING"
      mode = "NULLABLE"
      description = "Current company status"
    },
    {
      name = "is_active"
      type = "BOOLEAN"
      mode = "NULLABLE"
      description = "Whether company is active"
    },
    {
      name = "zipcode"
      type = "STRING"
      mode = "NULLABLE"
      description = "Company zipcode"
    },
    {
      name = "main_cnae"
      type = "STRING"
      mode = "NULLABLE"
      description = "Main CNAE (economic activity code)"
    },
    {
      name = "state"
      type = "STRING"
      mode = "NULLABLE"
      description = "State name"
    },
    {
      name = "uf"
      type = "STRING"
      mode = "NULLABLE"
      description = "State abbreviation (UF)"
    },
    {
      name = "city"
      type = "STRING"
      mode = "NULLABLE"
      description = "City name"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
      description = "Record creation timestamp"
    },
    {
      name = "updated_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
      description = "Record update timestamp"
    },
    {name = "buyer_tax_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Buyer tax ID"
    }
  ])

  labels = {
    env = "production"
    layer = "bronze"
  }
}

resource "google_bigquery_table" "business_case_installments" {
  dataset_id = google_bigquery_dataset.bronze.dataset_id
  table_id   = "installments"
  
  deletion_protection = false

  schema = jsonencode([
    {
      name = "asset_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Asset identifier"
    },
    {
      name = "invoice_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Invoice identifier"
    },
    {
      name = "buyer_tax_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Buyer tax ID"
    },
    {
      name = "original_amount_in_cents"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Original amount in cents"
    },
    {
      name = "expected_amount_in_cents"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Expected amount in cents"
    },
    {
      name = "paid_amount_in_cents"
      type = "INTEGER"
      mode = "NULLABLE"
      description = "Paid amount in cents"
    },
    {
      name = "due_date"
      type = "DATE"
      mode = "NULLABLE"
      description = "Payment due date"
    },
    {
      name = "paid_date"
      type = "DATE"
      mode = "NULLABLE"
      description = "Payment date"
    },
    {
      name = "invoice_issue_date"
      type = "DATE"
      mode = "NULLABLE"
      description = "Invoice issue date"
    },
    {
      name = "buyer_main_tax_id"
      type = "STRING"
      mode = "NULLABLE"
      description = "Buyer main tax ID"
    }
  ])

  labels = {
    env = "production"
    layer = "bronze"
  }
}
