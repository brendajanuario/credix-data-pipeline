-- Silver layer: Cleaned and standardized data
-- This model performs data quality checks and standardization

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH cleaned_data AS (
    SELECT 
        -- Add your column transformations here
        *,
        -- Example data quality rules
        CASE 
            WHEN amount IS NULL OR amount <= 0 THEN 'INVALID_AMOUNT'
            WHEN transaction_date IS NULL THEN 'MISSING_DATE'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Standardization examples
        UPPER(TRIM(customer_id)) as standardized_customer_id,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', transaction_date) as parsed_transaction_date,
        
        _loaded_at,
        CURRENT_TIMESTAMP() as _processed_at
    FROM {{ ref('bronze_raw_transactions') }}
)

SELECT 
    *
FROM cleaned_data
WHERE data_quality_flag = 'VALID'  -- Only pass valid records to silver
