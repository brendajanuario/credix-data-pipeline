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
