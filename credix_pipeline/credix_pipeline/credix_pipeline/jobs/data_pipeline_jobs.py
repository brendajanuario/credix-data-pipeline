from dagster import define_asset_job, AssetSelection

# Job for CNPJ data pipeline
cnpj_pipeline_job = define_asset_job(
    name="cnpj_data_pipeline",
    description="Extract CNPJ data from PostgreSQL, store in GCS, and load to BigQuery bronze layer",
    selection=AssetSelection.groups("cnpj_pipeline"),
)

# Job for installments data pipeline
installments_pipeline_job = define_asset_job(
    name="installments_data_pipeline",
    description="Extract installments data from PostgreSQL, store in GCS, and load to BigQuery bronze layer",
    selection=AssetSelection.groups("installments_pipeline"),
)

# Combined job for full pipeline
full_data_pipeline_job = define_asset_job(
    name="full_data_pipeline",
    description="Run both CNPJ and installments pipelines",
    selection=AssetSelection.groups("cnpj_pipeline", "installments_pipeline", "gold_layer"),
)

monitoring_job = define_asset_job(
    name="monitoring_job",
    description="Run EDR monitoring and send reports",
    selection=AssetSelection.groups("monitoring")
)