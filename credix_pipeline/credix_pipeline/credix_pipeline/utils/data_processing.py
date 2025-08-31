import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional

def prepare_dataframe_for_bigquery(df: pd.DataFrame, timestamp_columns: List[str] = None) -> pd.DataFrame:
    """Prepare DataFrame for BigQuery compatibility."""
    df_copy = df.copy()
    
    # Default timestamp columns if not provided
    if timestamp_columns is None:
        timestamp_columns = ['created_at', 'updated_at']
    
    # Fix timestamp precision for BigQuery compatibility
    for col in timestamp_columns:
        if col in df_copy.columns:
            df_copy[col] = (df_copy[col].astype(int) / 10**3).astype('int64')
    
    return df_copy

def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Parquet bytes."""
    table = pa.Table.from_pandas(df)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)
    return parquet_buffer.getvalue().to_pybytes()

def filter_schema_columns(schema: List, excluded_columns: List[str] = None) -> List:
    """Filter out metadata columns from schema."""
    if excluded_columns is None:
        excluded_columns = ['_loaded_at', '_source_table']
    
    return [
        field for field in schema 
        if field.name not in excluded_columns
    ]
