import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import asset, AssetExecutionContext
from ..resources import PostgresResource, GCPResource
from ..utils.cdc_helpers import get_cdc_last_processed_time, build_cdc_query, process_cdc_results
from ..utils.data_processing import prepare_dataframe_for_bigquery, dataframe_to_parquet_bytes, filter_schema_columns
from ..utils.gcs_operations import generate_gcs_path, generate_no_changes_path, parse_gcs_uri, generate_archive_fail_paths, generate_unique_table_name

@asset(
    group_name="installments_pipeline",
    description="Extract installments data from PostgreSQL with CDC logic"
)
def installments_raw_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    """Extract raw installments data from PostgreSQL using CDC logic."""
    
    # Get the last processed timestamp from CDC checkpoint
    last_processed_time = get_cdc_last_processed_time(context, "installments_cdc_checkpoint")

    # Build CDC query
    columns = [
        "asset_id", "invoice_id", "buyer_tax_id", "original_amount_in_cents",
        "expected_amount_in_cents", "paid_amount_in_cents", "due_date", 
        "paid_date", "invoice_issue_date", "buyer_main_tax_id",
        "created_at", "updated_at"
    ]
    query = build_cdc_query("oltp.business_case_installments", columns, last_processed_time)
    
    context.log.info(f"Extracting installments data with CDC logic since: {last_processed_time}")
    df = postgres.execute_query(query)
    context.log.info(f"Extracted {len(df)} changed/new rows")
    
    # Process results and add metadata
    metadata = process_cdc_results(context, df, last_processed_time)
    context.add_output_metadata(metadata)
    
    return df

@asset(
    group_name="installments_pipeline",
    description="Convert installments data to Parquet and store in GCS (CDC-aware)"
)
def installments_gcs_parquet(
    context: AssetExecutionContext, 
    gcp: GCPResource, 
    installments_raw_data: pd.DataFrame
) -> str:
    """Convert installments data to Parquet format and upload to GCS (only if there are changes)."""
    
    bucket_name = "data_lake_credix"
    
    # Skip processing if no new data
    if len(installments_raw_data) == 0:
        context.log.info("No new or changed data detected, skipping GCS upload")
        return generate_no_changes_path(bucket_name)
    
    # Prepare DataFrame for BigQuery (assuming date columns need conversion)
    df_processed = prepare_dataframe_for_bigquery(
        installments_raw_data, 
        timestamp_columns=['due_date', 'paid_date', 'invoice_issue_date', 'created_at', 'updated_at']
    )
    parquet_bytes = dataframe_to_parquet_bytes(df_processed)
    
    # Generate GCS path with date partition
    blob_name, current_date, timestamp = generate_gcs_path(bucket_name, "installments")
    
    context.log.info(f"Uploading {len(installments_raw_data)} changed records to gs://{bucket_name}/{blob_name}")
    gcs_uri = gcp.upload_to_gcs(bucket_name, blob_name, parquet_bytes)
    context.log.info(f"Successfully uploaded CDC batch to {gcs_uri}")
    
    context.add_output_metadata({
        "gcs_uri": gcs_uri,
        "records_uploaded": len(installments_raw_data),
        "file_timestamp": timestamp,
        "ingestion_date": current_date
    })
    
    return gcs_uri

@asset(
    group_name="installments_pipeline",
    description="Load installments data from GCS to BigQuery temp layer (CDC-aware)"
)
def installments_temp_table(
    context: AssetExecutionContext,
    gcp: GCPResource,
    installments_gcs_parquet: str
) -> str:
    """Load installments data from GCS to BigQuery temp layer (skip if no changes)."""
    
    # Skip if no changes detected
    if "no_changes" in installments_gcs_parquet:
        context.log.info("No changes detected, skipping BigQuery load")
        return "business_case_temp.installments"
    
    dataset_id = "business_case_temp"
    # Generate unique table name with hash
    table_id = generate_unique_table_name("installments", installments_gcs_parquet)
    
    # Get schema and filter metadata columns (assuming installments bronze table exists)
    try:
        bronze_schema = gcp.get_table_schema("business_case_bronze", "installments")
        schema = filter_schema_columns(bronze_schema)
    except Exception as e:
        context.log.warning(f"Could not get bronze schema, proceeding without schema enforcement: {e}")
        schema = None
    
    context.log.info(f"Loading CDC data from {installments_gcs_parquet} to {dataset_id}.{table_id}")
    
    # Parse GCS URI and generate archive/fail paths
    source_bucket, source_blob = parse_gcs_uri(installments_gcs_parquet)
    archive_bucket, archive_blob, fail_bucket, fail_blob = generate_archive_fail_paths(source_bucket, source_blob)

    try:
        if schema:
            result = gcp.load_to_bigquery_with_schema(dataset_id, table_id, installments_gcs_parquet, schema)
        else:
            result = gcp.load_to_bigquery(dataset_id, table_id, installments_gcs_parquet)
        
        context.log.info(f"Successfully loaded CDC batch to {result}")
        
        # Store table name in metadata for dbt to access
        context.add_output_metadata({
            "temp_table_name": table_id,
            "temp_dataset": dataset_id,
            "full_table_ref": f"{dataset_id}.{table_id}"
        })
        
        # Move to archive bucket
        archive_uri = gcp.move_blob(source_bucket, source_blob, archive_bucket, archive_blob)
        context.log.info(f"Moved file to archive: {archive_uri}")
        return table_id
    except Exception as e:
        context.log.error(f"Failed to load CDC batch: {e}")
        
        # Move to fail bucket
        fail_uri = gcp.move_blob(source_bucket, source_blob, fail_bucket, fail_blob)
        context.log.info(f"Moved file to fail folder: {fail_uri}")
        raise

@asset(
    group_name="installments_pipeline",
    description="CDC checkpoint - advance watermark only after successful bronze ingestion",
    deps=["installments"]  # Assuming there will be a dbt bronze layer for installments
)
def installments_cdc_checkpoint(
    context: AssetExecutionContext,
    installments_raw_data: pd.DataFrame
):
    """Advance CDC watermark only after bronze layer succeeds."""
    
    if len(installments_raw_data) > 0:
        # Now it's safe to advance the CDC watermark
        max_updated_at = installments_raw_data['updated_at'].max()
        context.log.info(f"Advancing CDC watermark to: {max_updated_at}")
        
        context.add_output_metadata({
            "max_updated_at": str(max_updated_at),
            "records_processed": len(installments_raw_data),
            "cdc_watermark": str(max_updated_at)
        })
    else:
        context.log.info("No records to process, CDC watermark unchanged")
        context.add_output_metadata({
            "records_processed": 0
        })
    
    return "checkpoint_complete"
