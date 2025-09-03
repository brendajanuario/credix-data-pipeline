-- Silver layer: Cleaned and standardized CNPJ data
-- This model performs data quality checks and standardization for CNPJ data

{{ config(
    materialized='table',
    schema='silver',
    meta={'dagster': {'group': 'cnpj_pipeline'}}

) }}

WITH cleaned_data AS (
    SELECT 
        -- Add your column transformations here
        *,
        -- Example data quality rules for CNPJ data
        CASE 
            WHEN share_capital IS NULL OR share_capital < 0 THEN 'INVALID_CAPITAL'
            WHEN zipcode IS NULL OR LENGTH(zipcode) != 8 THEN 'INVALID_ZIPCODE'
            WHEN main_cnae IS NULL THEN 'MISSING_CNAE'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Standardization examples for CNPJ data
        UPPER(TRIM(state)) as standardized_state,
        UPPER(TRIM(uf)) as standardized_uf,
        UPPER(TRIM(city)) as standardized_city,
        
        --_loaded_at,
        --CURRENT_TIMESTAMP() as _processed_at
    FROM {{ ref('cnpj_ws') }}
)

SELECT 
    *
FROM cleaned_data
WHERE 
    data_quality_flag = 'VALID'
    AND buyer_tax_id IS NOT NULL
    --AND company_status IN ('Ativa', 'Baixada', 'Suspensa', 'Inapta')

