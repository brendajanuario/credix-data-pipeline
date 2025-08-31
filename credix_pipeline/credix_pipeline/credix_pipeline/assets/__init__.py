from .cnpj_assets import *
from .installments_assets import *
#from .dbt_assets import dbt_silver_cnpj_clean, dbt_gold_cnpj_metrics

__all__ = [
    "cnpj_raw_data",
    "cnpj_gcs_parquet", 
    "cnpj_temp_table",
    "dbt_bronze_cnpj",
    "dbt_silver_cnpj_clean",
    "dbt_gold_cnpj_metrics",
    "cnpj_cdc_checkpoint",
    "installments_raw_data",
    "installments_gcs_parquet",
    "installments_temp_table",
    "installments_cdc_checkpoint",
]
