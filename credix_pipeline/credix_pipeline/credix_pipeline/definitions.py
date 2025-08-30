import os
from dagster import Definitions, load_assets_from_modules

from credix_pipeline import assets  # noqa: TID252
from credix_pipeline.jobs import cnpj_pipeline_job, installments_pipeline_job, full_data_pipeline_job
from credix_pipeline.resources import PostgresResource, GCPResource

# Load all assets from the assets modules
all_assets = load_assets_from_modules([assets])

# Define resources
resources = {
    "postgres": PostgresResource(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "credix_transactions"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres123"),
    ),
    "gcp": GCPResource(
        project_id=os.getenv("GCP_PROJECT", "product-reliability-analyzer"),
        credentials_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
    ),
}

defs = Definitions(
    assets=all_assets,
    jobs=[cnpj_pipeline_job, installments_pipeline_job, full_data_pipeline_job],
    resources=resources,
)
