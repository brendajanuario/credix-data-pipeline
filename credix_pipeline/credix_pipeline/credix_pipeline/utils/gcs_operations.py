import pandas as pd
from typing import Tuple
import hashlib
import time

def generate_gcs_path(bucket_name: str, table_name: str, prefix: str = "business_case/landing") -> Tuple[str, str, str]:
    """Generate GCS path with date partition and timestamp."""
    current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{prefix}/ingestion_dt={current_date}/{table_name}_{timestamp}.parquet"
    
    return blob_name, current_date, timestamp

def generate_no_changes_path(bucket_name: str, prefix: str = "business_case/landing") -> str:
    """Generate placeholder path for no changes case."""
    current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    return f"gs://{bucket_name}/{prefix}/ingestion_dt={current_date}/no_changes"

def parse_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
    """Parse GCS URI into bucket and blob components."""
    uri_parts = gcs_uri.replace('gs://', '').split('/', 1)
    source_bucket = uri_parts[0]
    source_blob = uri_parts[1]
    return source_bucket, source_blob

def generate_archive_fail_paths(source_bucket: str, source_blob: str) -> Tuple[str, str, str, str]:
    """Generate archive and fail bucket paths."""
    archive_bucket = f"{source_bucket}_archive"
    archive_blob = source_blob.replace('landing/', 'archive/')
    fail_bucket = source_bucket
    fail_blob = source_blob.replace('landing/', 'failed/')
    
    return archive_bucket, archive_blob, fail_bucket, fail_blob

def generate_unique_table_name(base_name: str, gcs_uri: str = None) -> str:
    """Generate unique table name with hash suffix."""
    timestamp = str(int(time.time()))
    hash_input = f"{timestamp}_{gcs_uri or ''}"
    hash_suffix = hashlib.md5(hash_input.encode()).hexdigest()[:12]
    return f"{base_name}_{hash_suffix}"
