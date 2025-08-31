import pandas as pd
from dagster import AssetExecutionContext, AssetKey
from typing import Optional

def get_cdc_last_processed_time(context: AssetExecutionContext, checkpoint_asset_key: str) -> str:
    """Get the last processed timestamp from CDC checkpoint."""
    checkpoint_cursor = context.instance.get_latest_materialization_event(
        AssetKey(checkpoint_asset_key)
    )
    
    if checkpoint_cursor and checkpoint_cursor.asset_materialization:
        last_processed_time = checkpoint_cursor.asset_materialization.metadata.get("max_updated_at")
        if last_processed_time:
            last_processed_time = last_processed_time.value
            context.log.info(f"Found last processed timestamp: {last_processed_time}")
            return last_processed_time
    
    default_time = "1900-01-01 00:00:00"
    context.log.info("No previous timestamp found, using default")
    return default_time

def build_cdc_query(table_name: str, columns: list, last_processed_time: str, limit: int = 10000) -> str:
    """Build CDC query for extracting changed records."""
    columns_str = ",\n        ".join(columns)
    
    query = f"""
    SELECT 
        {columns_str}
    FROM {table_name}
    WHERE updated_at > '{last_processed_time}'
       OR created_at > '{last_processed_time}'
    ORDER BY updated_at DESC
    LIMIT {limit}  -- Safety limit for CDC processing
    """
    
    return query

def process_cdc_results(context: AssetExecutionContext, df: pd.DataFrame, last_processed_time: str) -> dict:
    """Process CDC query results and return metadata."""
    if len(df) > 0:
        max_updated_at = df['updated_at'].max()
        context.log.info(f"Max updated_at in this batch: {max_updated_at}")
        
        return {
            "records_extracted": len(df),
            "batch_max_updated_at": str(max_updated_at)
        }
    else:
        context.log.info("No new or changed records found")
        return {
            "records_extracted": 0,
            "batch_max_updated_at": last_processed_time
        }
