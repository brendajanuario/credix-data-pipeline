import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import asset, AssetExecutionContext, AssetIn, MetadataValue, AssetKey
from dagster_dbt import DbtProject, dbt_assets, DbtCliResource
from ..resources import PostgresResource, GCPResource
from ..utils.cdc_helpers import get_cdc_last_processed_time, build_cdc_query, process_cdc_results
from ..utils.data_processing import prepare_dataframe_for_bigquery, dataframe_to_parquet_bytes, filter_schema_columns
from ..utils.gcs_operations import generate_gcs_path, generate_no_changes_path, parse_gcs_uri, generate_archive_fail_paths, generate_unique_table_name

@asset(
    group_name="cnpj_pipeline",
    description="Extract CNPJ data from PostgreSQL with CDC logic"
)
def cnpj_raw_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    """Extract raw CNPJ data from PostgreSQL using CDC logic."""
    
    # Get the last processed timestamp from CDC checkpoint
    last_processed_time = get_cdc_last_processed_time(context, "cnpj_cdc_checkpoint")

    # Build CDC query
    columns = [
        "share_capital", "company_size", "legal_nature", "simples_option", 
        "is_mei", "is_main_company", "company_status", "is_active", 
        "zipcode", "main_cnae", "state", "uf", "city", "buyer_tax_id", 
        "created_at", "updated_at"
    ]
    query = build_cdc_query("oltp.business_case_cnpj_ws", columns, last_processed_time)
    
    context.log.info(f"Extracting CNPJ data with CDC logic since: {last_processed_time}")
    df = postgres.execute_query(query)
    context.log.info(f"Extracted {len(df)} changed/new rows")
    
    # Process results and add metadata
    metadata = process_cdc_results(context, df, last_processed_time)
    context.add_output_metadata(metadata)
    
    return df

@asset(
    group_name="cnpj_pipeline",
    description="Convert CNPJ data to Parquet and store in GCS (CDC-aware)"
)
def cnpj_gcs_parquet(
    context: AssetExecutionContext, 
    gcp: GCPResource, 
    cnpj_raw_data: pd.DataFrame
) -> str:
    """Convert CNPJ data to Parquet format and upload to GCS (only if there are changes)."""
    
    bucket_name = "data_lake_credix"
    
    # Skip processing if no new data
    if len(cnpj_raw_data) == 0:
        context.log.info("No new or changed data detected, skipping GCS upload")
        return generate_no_changes_path(bucket_name)
    
    # Prepare DataFrame for BigQuery
    df_processed = prepare_dataframe_for_bigquery(cnpj_raw_data)
    parquet_bytes = dataframe_to_parquet_bytes(df_processed)
    
    # Generate GCS path with date partition
    blob_name, current_date, timestamp = generate_gcs_path(bucket_name, "cnpj_ws")
    
    context.log.info(f"Uploading {len(cnpj_raw_data)} changed records to gs://{bucket_name}/{blob_name}")
    gcs_uri = gcp.upload_to_gcs(bucket_name, blob_name, parquet_bytes)
    context.log.info(f"Successfully uploaded CDC batch to {gcs_uri}")
    
    context.add_output_metadata({
        "gcs_uri": gcs_uri,
        "records_uploaded": len(cnpj_raw_data),
        "file_timestamp": timestamp,
        "ingestion_date": current_date
    })
    
    return gcs_uri

@asset(
    group_name="cnpj_pipeline",
    description="Load CNPJ data from GCS to BigQuery temp layer (CDC-aware)"
)
def cnpj_temp_table(
    context: AssetExecutionContext,
    gcp: GCPResource,
    cnpj_gcs_parquet: str
) -> str:
    """Load CNPJ data from GCS to BigQuery temp layer (skip if no changes)."""
    
    # Skip if no changes detected
    if "no_changes" in cnpj_gcs_parquet:
        context.log.info("No changes detected, skipping BigQuery load")
        return "business_case_temp.cnpj_ws"
    
    dataset_id = "business_case_temp"
    # Generate unique table name with hash
    table_id = generate_unique_table_name("cnpj_ws", cnpj_gcs_parquet)
    
    # Get schema and filter metadata columns
    bronze_schema = gcp.get_table_schema("business_case_bronze", "cnpj_ws")
    schema = filter_schema_columns(bronze_schema)
    
    context.log.info(f"Loading CDC data from {cnpj_gcs_parquet} to {dataset_id}.{table_id} with enforced schema")
    
    # Parse GCS URI and generate archive/fail paths
    source_bucket, source_blob = parse_gcs_uri(cnpj_gcs_parquet)
    archive_bucket, archive_blob, fail_bucket, fail_blob = generate_archive_fail_paths(source_bucket, source_blob)

    try:
        result = gcp.load_to_bigquery_with_schema(dataset_id, table_id, cnpj_gcs_parquet, schema)
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


DBT_PROJECT_DIR = "/Users/jemzin/Github/credix-data-pipeline/dbt/business_case"
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="cnpj_ws",
    name="cnpj_bronze_layer"
)
def dbt_bronze_cnpj(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt bronze layer for CNPJ data."""

    latest_materialization = context.instance.get_latest_materialization_event(
        AssetKey(["cnpj_temp_table"])
    )

    temp_table_name = latest_materialization.asset_materialization.metadata.get("temp_table_name", "cnpj_ws")
    if hasattr(temp_table_name, 'value'):
        temp_table_name = temp_table_name.value

    context.log.info(f"Using temp table name: {temp_table_name}")
    
    # Pass table name as dbt variable
    yield from dbt.cli([
        "build", 
        "--select", "cnpj_ws",
        "--vars", f"temp_table_name: {temp_table_name}"
    ], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="cnpj_ws_clean",
    name="cnpj_silver_layer"
)
def dbt_silver_cnpj_clean(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt silver layer for CNPJ data."""
    yield from dbt.cli(["build", "--select", "cnpj_ws_clean"], context=context).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="cnpj_ws_metrics", 
    name="cnpj_gold_layer"
)
def dbt_gold_cnpj_metrics(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt gold layer for CNPJ metrics."""
    yield from dbt.cli(["build", "--select", "cnpj_ws_metrics"], context=context).stream()


@asset(
    group_name="cnpj_pipeline",
    description="CDC checkpoint - advance watermark only after successful bronze ingestion",
    deps=["cnpj_ws"]
)
def cnpj_cdc_checkpoint(
    context: AssetExecutionContext,
    cnpj_raw_data: pd.DataFrame
):
    """Advance CDC watermark only after bronze layer succeeds."""
    
    if len(cnpj_raw_data) > 0:
        # Now it's safe to advance the CDC watermark
        max_updated_at = cnpj_raw_data['updated_at'].max()
        context.log.info(f"Advancing CDC watermark to: {max_updated_at}")
        
        context.add_output_metadata({
            "max_updated_at": str(max_updated_at),
            "records_processed": len(cnpj_raw_data),
            "cdc_watermark": str(max_updated_at)
        })
    else:
        context.log.info("No records to process, CDC watermark unchanged")
        context.add_output_metadata({
            "records_processed": 0
        })
    
    return "checkpoint_complete"


