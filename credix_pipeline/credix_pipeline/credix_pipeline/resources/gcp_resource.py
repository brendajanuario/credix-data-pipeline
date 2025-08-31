from dagster import ConfigurableResource
from google.cloud import storage
from google.cloud import bigquery
import os

class GCPResource(ConfigurableResource):
    """Resource for GCP services (Storage and BigQuery)."""
    
    project_id: str = "product-reliability-analyzer"
    credentials_path: str = ""
    
    def get_storage_client(self):
        """Get Google Cloud Storage client."""
        if self.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
        return storage.Client(project=self.project_id)
    
    def get_bigquery_client(self):
        """Get BigQuery client."""
        if self.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
        return bigquery.Client(project=self.project_id)
    
    def upload_to_gcs(self, bucket_name: str, blob_name: str, data: bytes):
        """Upload data to Google Cloud Storage."""
        client = self.get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data)
        return f"gs://{bucket_name}/{blob_name}"
    
    def load_to_bigquery(self, dataset_id: str, table_id: str, gcs_uri: str):
        """Load data from GCS to BigQuery."""
        client = self.get_bigquery_client()
        
        table_ref = client.dataset(dataset_id).table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        
        load_job.result()  # Wait for job to complete
        return f"{dataset_id}.{table_id}"
    
    def load_to_bigquery_with_schema(self, dataset_id: str, table_id: str, gcs_uri: str, schema: list):
        """Load data from GCS to BigQuery with explicit schema definition."""
        client = self.get_bigquery_client()
        
        table_ref = client.dataset(dataset_id).table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema
        )
        
        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        
        load_job.result()  # Wait for job to complete
        return f"{dataset_id}.{table_id}"
    
    def copy_gcs_file(self, source_bucket: str, source_blob: str, dest_bucket: str, dest_blob: str):
        """Copy a file from one GCS bucket to another."""
        client = self.get_storage_client()
        
        source_bucket_obj = client.bucket(source_bucket)
        source_blob_obj = source_bucket_obj.blob(source_blob)
        
        dest_bucket_obj = client.bucket(dest_bucket)
        
        # Copy the blob
        copied_blob = source_bucket_obj.copy_blob(source_blob_obj, dest_bucket_obj, dest_blob)
        
        return f"gs://{dest_bucket}/{dest_blob}"
    
    def delete_gcs_file(self, bucket_name: str, blob_name: str):
        """Delete a file from GCS."""
        client = self.get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        return True
    
    def move_blob(self, source_bucket: str, source_blob: str, dest_bucket: str, dest_blob: str):
        """Move a file from one GCS bucket to another (copy then delete)."""
        # First copy the file
        dest_uri = self.copy_gcs_file(source_bucket, source_blob, dest_bucket, dest_blob)
        
        # Then delete the original
        self.delete_gcs_file(source_bucket, source_blob)
        
        return dest_uri
    
    def get_table_schema(self, dataset_id: str, table_id: str):
        """Get the schema of an existing BigQuery table."""
        client = self.get_bigquery_client()
        
        try:
            table_ref = client.dataset(dataset_id).table(table_id)
            table = client.get_table(table_ref)
            return table.schema
        except Exception as e:
            # If table doesn't exist, return None
            return None
