import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import asset, AssetExecutionContext, AssetKey
from dagster_dbt import DbtProject, dbt_assets, DbtCliResource

from ..resources import PostgresResource, GCPResource
from ..utils.cdc_helpers import (
    get_cdc_last_processed_time,
    process_cdc_results,
)
from ..utils.data_processing import (
    prepare_dataframe_for_bigquery,
    dataframe_to_parquet_bytes,
    filter_schema_columns,
)
from ..utils.gcs_operations import (
    generate_gcs_path,
    generate_no_changes_path,
    parse_gcs_uri,
    generate_archive_fail_paths,
    generate_unique_table_name,
)

DBT_PROJECT_DIR = "/Users/jemzin/Github/credix-data-pipeline/dbt/business_case"
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)


@asset(
    group_name="installments_pipeline",
    description="Extract installments data from PostgreSQL with CDC logic",
)
def installments_raw_data(context: AssetExecutionContext, postgres: PostgresResource) -> pd.DataFrame:
    last_processed_time = get_cdc_last_processed_time(context, "installments_cdc_checkpoint")

    query = f"""SELECT 
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
        WHERE GREATEST(
            COALESCE(invoice_issue_date, '1900-01-01'::timestamp),
            COALESCE(paid_date, '1900-01-01'::timestamp)
        ) > '{last_processed_time}'
        ORDER BY GREATEST(
            COALESCE(invoice_issue_date, '1900-01-01'::timestamp),
            COALESCE(paid_date, '1900-01-01'::timestamp)
        ) DESC
    -- LIMIT 1000;
    """
    
    context.log.info(f"Extracting installments data with CDC logic since: {last_processed_time}")
    df = postgres.execute_query(query)
    context.log.info(f"Extracted {len(df)} changed/new rows")

    if len(df) > 0:
        # Parse event date/timestamp columns and compute batch max across them
        event_cols = [c for c in ["invoice_issue_date", "paid_date", "due_date"] if c in df.columns]
        max_latest_date = None
        for c in event_cols:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().any():
                cur_max = s.max()
                max_latest_date = cur_max if max_latest_date is None or cur_max > max_latest_date else max_latest_date

        context.log.info(f"Max latest_date in this batch: {max_latest_date}")

        metadata = {
            "records_extracted": len(df),
            "batch_max_updated_at": str(max_latest_date),
        }
    else:
        context.log.info("No new or changed records found")
        metadata = {
            "records_extracted": 0,
            "batch_max_updated_at": last_processed_time,
        }

    context.add_output_metadata(metadata)

    return df


@asset(
    group_name="installments_pipeline",
    description="Convert installments data to Parquet and store in GCS (CDC-aware)",
)
def installments_gcs_parquet(
    context: AssetExecutionContext,
    gcp: GCPResource,
    installments_raw_data: pd.DataFrame,
) -> str:
    bucket_name = "data_lake_credix"

    if len(installments_raw_data) == 0:
        context.log.info("No new or changed data detected, skipping GCS upload")
        return generate_no_changes_path(bucket_name)

    # Ensure BigQuery-compatible types: event columns as DATE (Parquet date32)
    df_processed = prepare_dataframe_for_bigquery(
        installments_raw_data,
        timestamp_columns=[],
        date_columns=["due_date", "paid_date", "invoice_issue_date"],
    )
    parquet_bytes = dataframe_to_parquet_bytes(df_processed)

    blob_name, current_date, timestamp = generate_gcs_path(bucket_name, "installments")

    context.log.info(f"Uploading {len(installments_raw_data)} changed records to gs://{bucket_name}/{blob_name}")
    gcs_uri = gcp.upload_to_gcs(bucket_name, blob_name, parquet_bytes)
    context.log.info(f"Successfully uploaded CDC batch to {gcs_uri}")

    context.add_output_metadata(
        {
            "gcs_uri": gcs_uri,
            "records_uploaded": len(installments_raw_data),
            "file_timestamp": timestamp,
            "ingestion_date": current_date,
        }
    )

    return gcs_uri


@asset(
    group_name="installments_pipeline",
    description="Load installments data from GCS to BigQuery temp layer (CDC-aware)",
)
def installments_temp_table(
    context: AssetExecutionContext,
    gcp: GCPResource,
    installments_gcs_parquet: str,
) -> str:
    """Load to hashed temp table (append) and a standard table (truncate) for stable references."""
    if "no_changes" in installments_gcs_parquet:
        context.log.info("No changes detected, skipping BigQuery load")
        return "business_case_temp.installments"

    dataset_id = "business_case_temp"
    hashed_table_id = generate_unique_table_name("installments", installments_gcs_parquet)
    standard_table_id = "installments"

    # Try to enforce schema from bronze table
    try:
        bronze_schema = gcp.get_table_schema("business_case_temp", "installments_dbt")
        schema = filter_schema_columns(bronze_schema)
    except Exception as e:
        context.log.warning(f"Could not get bronze schema, proceeding without schema enforcement: {e}")
        schema = None

    context.log.info(
        f"Loading CDC data from {installments_gcs_parquet} to both {dataset_id}.{hashed_table_id} and {dataset_id}.{standard_table_id}"
    )

    source_bucket, source_blob = parse_gcs_uri(installments_gcs_parquet)
    archive_bucket, archive_blob, fail_bucket, fail_blob = generate_archive_fail_paths(source_bucket, source_blob)

    try:
        # Hashed table: append
        if schema:
            result_hashed = gcp.load_to_bigquery_with_schema(dataset_id, hashed_table_id, installments_gcs_parquet, schema)
        else:
            result_hashed = gcp.load_to_bigquery(dataset_id, hashed_table_id, installments_gcs_parquet)
        context.log.info(f"Successfully loaded CDC batch to {result_hashed}")

        # Standard table: truncate/overwrite for convenience
        if schema:
            result_standard = gcp.load_to_bigquery_truncate(dataset_id, standard_table_id, installments_gcs_parquet, schema)
        else:
            # Fallback if truncate helper without schema is not available
            result_standard = gcp.load_to_bigquery_truncate(dataset_id, standard_table_id, installments_gcs_parquet, None)
        context.log.info(f"Successfully loaded CDC batch to {result_standard} (WRITE_TRUNCATE)")

        context.add_output_metadata(
            {
                "temp_table_name": hashed_table_id,
                "standard_table_name": standard_table_id,
                "temp_dataset": dataset_id,
                "full_table_ref": f"{dataset_id}.{hashed_table_id}",
                "standard_table_ref": f"{dataset_id}.{standard_table_id}",
            }
        )

        archive_uri = gcp.move_blob(source_bucket, source_blob, archive_bucket, archive_blob)
        context.log.info(f"Moved file to archive: {archive_uri}")
        return hashed_table_id
    except Exception as e:
        context.log.error(f"Failed to load CDC batch: {e}")
        fail_uri = gcp.move_blob(source_bucket, source_blob, fail_bucket, fail_blob)
        context.log.info(f"Moved file to fail folder: {fail_uri}")
        raise


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="installments",
    name="installments_bronze_layer",
)
def dbt_bronze_installments(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt bronze layer for installments data."""
    try:
        latest_materialization = context.instance.get_latest_materialization_event(
            AssetKey(["installments_temp_table"])
        )
        if latest_materialization and latest_materialization.asset_materialization.metadata:
            temp_table_name = latest_materialization.asset_materialization.metadata.get("temp_table_name", "installments")
            if hasattr(temp_table_name, "value"):
                temp_table_name = temp_table_name.value
        else:
            temp_table_name = "installments"
    except Exception as e:
        context.log.warning(f"Could not retrieve temp table name from upstream asset: {e}")
        temp_table_name = "installments"

    context.log.info(f"Using temp table name: {temp_table_name}")

    yield from dbt.cli(
        [
            "build",
            "--select",
            "installments",
            "--vars",
            f"temp_table_name: {temp_table_name}",
        ],
        context=context,
    ).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="installments_clean",
    name="installments_silver_layer",
)
def dbt_silver_installments_clean(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt silver layer for installments data."""
    yield from dbt.cli(["build", "--select", "installments_clean"], context=context).stream()


@asset(
    group_name="installments_pipeline",
    description="CDC checkpoint - advance watermark only after successful bronze ingestion",
    deps=["installments"],  # dbt bronze model
)
def installments_cdc_checkpoint(
    context: AssetExecutionContext,
    installments_raw_data: pd.DataFrame,
    gcp: GCPResource,
):
    """Advance CDC watermark only after bronze layer succeeds and clean up temp table."""
    if len(installments_raw_data) > 0:
        # Compute max event timestamp across available event columns
        max_ts = None
        for col in ["invoice_issue_date", "paid_date", "due_date"]:
            if col in installments_raw_data.columns:
                s = pd.to_datetime(installments_raw_data[col], errors="coerce")
                if s.notna().any():
                    cur_max = s.max()
                    max_ts = cur_max if max_ts is None or cur_max > max_ts else max_ts
        if max_ts is None:
            context.log.warning("Could not determine max event timestamp; defaulting to earliest watermark")
            max_ts = pd.to_datetime("1900-01-01 00:00:00")
        context.log.info(f"Advancing CDC watermark to: {max_ts}")
        context.add_output_metadata(
            {
                # Backward-compatible key name expected by get_cdc_last_processed_time
                "max_updated_at": str(max_ts),
                "records_processed": len(installments_raw_data),
                # Also include explicit batch event timestamp for observability
                "cdc_watermark": str(max_ts),
            }
        )
    else:
        context.log.info("No records to process, CDC watermark unchanged")
        context.add_output_metadata({"records_processed": 0})

    latest_materialization = context.instance.get_latest_materialization_event(
        AssetKey(["installments_temp_table"])
    )
    temp_table_name = None
    if latest_materialization and latest_materialization.asset_materialization.metadata:
        temp_table_name = latest_materialization.asset_materialization.metadata.get("temp_table_name")
        if hasattr(temp_table_name, "value"):
            temp_table_name = temp_table_name.value

    if temp_table_name:
        gcp.delete_temp_table("business_case_temp", temp_table_name)
        context.log.info(f"Cleaned up temp table: {temp_table_name}")