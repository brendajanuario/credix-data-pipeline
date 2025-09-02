import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional

def prepare_dataframe_for_bigquery(
    df: pd.DataFrame,
    timestamp_columns: List[str] = [],
    date_columns: List[str] = [],
) -> pd.DataFrame:
    """Prepare DataFrame for BigQuery compatibility.

    - Coerce timestamp columns into pandas datetime (UTC) robustly.
    - Coerce date columns into Python date objects so Arrow writes DATE (int32 days).
    - Accepts datetime/date objects, strings, or numeric epochs (s/ms/us).
    - Timestamps are output as timezone-naive datetime64[ns] (interpreted as UTC) for Parquet->BigQuery.
    - Dates are output as date objects -> Parquet date32, matching BigQuery DATE.
    """
    df_copy = df.copy()

    # Normalize dates (ensure Parquet DATE logical type)
    for col in date_columns:
        if col in df_copy.columns:
            s = pd.to_datetime(df_copy[col], errors="coerce")
            # Keep only the date component; becomes array of python datetime.date
            df_copy[col] = s.dt.date


    # Fix timestamp precision for BigQuery compatibility
    for col in timestamp_columns:
        if col in df_copy.columns:
            df_copy[col] = (df_copy[col].astype(int) / 10**3).astype('int64')
    
    return df_copy


def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Parquet bytes."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer, coerce_timestamps="us", allow_truncated_timestamps=True)
    return parquet_buffer.getvalue().to_pybytes()

def filter_schema_columns(schema: List, excluded_columns: List[str] = None) -> List:
    """Filter out metadata columns from schema."""
    if excluded_columns is None:
        excluded_columns = ["_loaded_at", "_source_table"]

    return [
        field for field in schema
        if field.name not in excluded_columns
    ]
