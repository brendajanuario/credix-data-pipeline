import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from ..resources import PostgresResource, GCPResource

@asset(
    group_name="cnpj_pipeline",
    description="Extract CNPJ data from PostgreSQL"
)
def cnpj_raw_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    """Extract raw CNPJ data from PostgreSQL."""
    query = """
    SELECT 
        share_capital,
        company_size,
        legal_nature,
        simples_option,
        is_mei,
        is_main_company,
        company_status,
        is_active,
        zipcode,
        main_cnae,
        state,
        uf,
        city,
        created_at,
        updated_at
    FROM oltp.business_case_cnpj_ws
    LIMIT 1000  -- Add limit for testing
    """
    
    context.log.info("Extracting CNPJ data from PostgreSQL")
    df = postgres.execute_query(query)
    context.log.info(f"Extracted {len(df)} rows")
    
    return df

@asset(
    group_name="cnpj_pipeline",
    description="Convert CNPJ data to Parquet and store in GCS"
)
def cnpj_gcs_parquet(
    context: AssetExecutionContext, 
    gcp: GCPResource, 
    cnpj_raw_data: pd.DataFrame
) -> str:
    """Convert CNPJ data to Parquet format and upload to GCS."""
    
    # Convert DataFrame to Parquet bytes
    table = pa.Table.from_pandas(cnpj_raw_data)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue().to_pybytes()
    
    # Upload to GCS
    bucket_name = "data_lake_credix"
    blob_name = f"business_case/landing/cnpj_ws.parquet"
    
    context.log.info(f"Uploading CNPJ data to gs://{bucket_name}/{blob_name}")
    gcs_uri = gcp.upload_to_gcs(bucket_name, blob_name, parquet_bytes)
    context.log.info(f"Successfully uploaded to {gcs_uri}")
    
    return gcs_uri

@asset(
    group_name="cnpj_pipeline",
    description="Load CNPJ data from GCS to BigQuery temp layer"
)
def cnpj_temp_table(
    context: AssetExecutionContext,
    gcp: GCPResource,
    cnpj_gcs_parquet: str
) -> str:
    """Load CNPJ data from GCS to BigQuery temp layer."""
    
    dataset_id = "business_case_temp"
    table_id = "cnpj_ws"
    
    context.log.info(f"Loading data from {cnpj_gcs_parquet} to {dataset_id}.{table_id}")
    result = gcp.load_to_bigquery(dataset_id, table_id, cnpj_gcs_parquet)
    context.log.info(f"Successfully loaded data to {result}")
    
    return result

@asset(
    group_name="cnpj_pipeline",
    description="Load CNPJ data from GCS to BigQuery bronze layer"
)
def cnpj_bronze_table(
    context: AssetExecutionContext,
    gcp: GCPResource,
    cnpj_gcs_parquet: str
) -> str:
    """Load CNPJ data from GCS to BigQuery bronze layer."""
    
    dataset_id = "credix_bronze"
    table_id = "business_case_cnpj_ws"
    
    context.log.info(f"Loading data from {cnpj_gcs_parquet} to {dataset_id}.{table_id}")
    result = gcp.load_to_bigquery(dataset_id, table_id, cnpj_gcs_parquet)
    context.log.info(f"Successfully loaded data to {result}")
    
    return result

@asset(
    group_name="cnpj_pipeline",
    description="Transform CNPJ data to silver layer using dbt"
)
def cnpj_silver_table(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    cnpj_bronze_table: str
) -> str:
    """Transform CNPJ data to silver layer using dbt."""
    
    context.log.info("Running dbt model for CNPJ silver layer")
    
    # Run specific dbt model for CNPJ silver transformation
    result = dbt.cli(["run", "--select", "silver_cnpj_clean"], context=context)
    
    if result.success:
        context.log.info("Successfully transformed CNPJ data to silver layer")
        return "credix_silver.silver_cnpj_clean"
    else:
        raise Exception(f"dbt run failed: {result.stderr}")

@asset(
    group_name="cnpj_pipeline", 
    description="Create business metrics for CNPJ data in gold layer"
)
def cnpj_gold_metrics(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    cnpj_silver_table: str
) -> str:
    """Create business metrics for CNPJ data in gold layer using dbt."""
    
    context.log.info("Running dbt model for CNPJ gold metrics")
    
    # Run specific dbt model for CNPJ gold metrics
    result = dbt.cli(["run", "--select", "gold_cnpj_metrics"], context=context)
    
    if result.success:
        context.log.info("Successfully created CNPJ gold metrics")
        return "credix_gold.gold_cnpj_metrics"
    else:
        raise Exception(f"dbt run failed: {result.stderr}")
