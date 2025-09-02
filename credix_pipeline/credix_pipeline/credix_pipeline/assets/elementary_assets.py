from dagster import AssetExecutionContext, asset
import subprocess
from ..resources import GCPResource

@asset(
    group_name="monitoring",
    description="Run EDR monitoring to analyze data quality"
)
def edr_monitor_asset(context: AssetExecutionContext):
    """Run EDR monitor command"""
    context.log.info("Running EDR monitor...")
    
    try:
        result = subprocess.run(
            ["edr", "monitor"],
            capture_output=True,
            text=True,
            check=True
        )
        context.log.info(f"EDR monitor completed: {result.stdout}")
        
        context.add_output_metadata({
            "edr_monitor_status": "success",
            "monitoring_timestamp": context.run.created_timestamp
        })
        
        return result.stdout
    except subprocess.CalledProcessError as e:
        context.log.error(f"EDR monitor failed: {e.stderr}")
        context.add_output_metadata({
            "edr_monitor_status": "failed",
            "error": str(e)
        })
        raise

@asset(
    group_name="monitoring", 
    description="Send EDR report to GCS bucket",
    deps=[edr_monitor_asset]
)
def edr_send_report_asset(context: AssetExecutionContext, gcp: GCPResource):
    """Run EDR send-report command"""
    context.log.info("Sending EDR report...")
    
    bucket_name = "credix-elementary-report"  # Matches terraform bucket name
    report_path = f"reports/{context.run.run_id}/index.html"
    
    try:
        result = subprocess.run([
            "edr", "send-report",
            "--google-service-account-path", gcp.service_account_key_path,
            "--gcs-bucket-name", bucket_name,
            "--bucket-file-path", report_path,
            "--update-bucket-website", "true"
        ], capture_output=True, text=True, check=True)
        
        report_url = f"gs://{bucket_name}/{report_path}"
        context.log.info(f"EDR send-report completed: {result.stdout}")
        context.log.info(f"Report available at: {report_url}")
        
        context.add_output_metadata({
            "edr_report_status": "success",
            "report_location": report_url,
            "bucket_name": bucket_name,
            "report_path": report_path
        })
        
        return result.stdout
    except subprocess.CalledProcessError as e:
        context.log.error(f"EDR send-report failed: {e.stderr}")
        context.add_output_metadata({
            "edr_report_status": "failed",
            "error": str(e)
        })
        raise
