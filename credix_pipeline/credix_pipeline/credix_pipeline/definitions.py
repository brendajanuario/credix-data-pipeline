import os
from dagster import Definitions, load_assets_from_modules, RunRequest, schedule
from dagster_dbt import DbtCliResource

from credix_pipeline import assets  # noqa: TID252
from credix_pipeline.jobs import cnpj_pipeline_job, installments_pipeline_job, full_data_pipeline_job, monitoring_job
from credix_pipeline.resources import PostgresResource, GCPResource

# Load all assets from the assets modules
all_assets = load_assets_from_modules([assets])

DBT_PROJECT_DIR = "/Users/jemzin/Github/credix-data-pipeline/dbt/business_case"
DBT_PROFILES_DIR = "/Users/jemzin/Github/credix-data-pipeline/dbt"

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
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    ),
}

@schedule(job=installments_pipeline_job, cron_schedule="*/5 * * * *")
def installments_5_minute_schedule():
    return RunRequest()

@schedule(job=cnpj_pipeline_job, cron_schedule="0 0 * * *")
def cnpj_daily_schedule():
    return RunRequest()

@schedule(job=monitoring_job, cron_schedule="@daily")
def edr_daily_schedule():
    return RunRequest()

defs = Definitions(
    assets=all_assets,
    jobs=[cnpj_pipeline_job, installments_pipeline_job, monitoring_job, full_data_pipeline_job],
    schedules=[installments_5_minute_schedule, cnpj_daily_schedule, edr_daily_schedule],
    resources=resources,
)