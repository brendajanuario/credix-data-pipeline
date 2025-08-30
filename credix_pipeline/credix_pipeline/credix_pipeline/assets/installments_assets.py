import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import asset, AssetExecutionContext
from ..resources import PostgresResource, GCPResource

@asset(
    group_name="installments_pipeline",
    description="Extract installments data from PostgreSQL"
)
def installments_raw_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    """Extract raw installments data from PostgreSQL."""
    query = """
    SELECT 
        asset_id,
        invoice_id,
        buyer_tax_id,
        original_amount_in_cents,
        expected_amount_in_cents,
        paid_amount_in_cents,
        due_date,
        paid_date,
        invoice_issue_date,
        buyer_main_tax_id
    FROM oltp.business_case_installments
    LIMIT 1000  -- Add limit for testing
    """
    
    context.log.info("Extracting installments data from PostgreSQL")
    df = postgres.execute_query(query)
    context.log.info(f"Extracted {len(df)} rows")
    
    return df

@asset(
    group_name="installments_pipeline",
    description="Convert installments data to Parquet and store in GCS"
)
def installments_gcs_parquet(
    context: AssetExecutionContext, 
    gcp: GCPResource, 
    installments_raw_data: pd.DataFrame
) -> str:
    """Convert installments data to Parquet format and upload to GCS."""
    
    # Convert DataFrame to Parquet bytes
    table = pa.Table.from_pandas(installments_raw_data)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue().to_pybytes()
    
    # Upload to GCS
    bucket_name = "data_lake_credix"
    blob_name = f"business_case/landing/installments.parquet"
    
    context.log.info(f"Uploading installments data to gs://{bucket_name}/{blob_name}")
    gcs_uri = gcp.upload_to_gcs(bucket_name, blob_name, parquet_bytes)
    context.log.info(f"Successfully uploaded to {gcs_uri}")
    
    return gcs_uri

@asset(
    group_name="installments_pipeline",
    description="Load installments data from GCS to BigQuery temp layer"
)
def installments_temp_table(
    context: AssetExecutionContext,
    gcp: GCPResource,
    installments_gcs_parquet: str
) -> str:
    """Load installments data from GCS to BigQuery temp layer."""

    dataset_id = "business_case_temp"
    table_id = "installments"
    
    context.log.info(f"Loading data from {installments_gcs_parquet} to {dataset_id}.{table_id}")
    result = gcp.load_to_bigquery(dataset_id, table_id, installments_gcs_parquet)
    context.log.info(f"Successfully loaded data to {result}")
    
    return result
