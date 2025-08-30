-- Bronze layer: Installments data from temp layer
-- This model merges/upserts data from temp to bronze

{{ config(
    materialized='table',
    schema='bronze'
) }}

WITH temp_data AS (
    SELECT 
        *,
        CURRENT_TIMESTAMP() as _loaded_at,
        'business_case_temp.installments' as _source_table
    FROM `{{ var('gcp_project') }}.business_case_temp.installments`
),

existing_bronze AS (
    SELECT *
    FROM `{{ var('gcp_project') }}.business_case_bronze.installments`
    WHERE 1=0 -- This will be empty on first run, but allows the table structure
),

merged_data AS (
    -- Union temp data with existing bronze data (if any)
    -- For this example, we'll just take the latest data from temp
    -- You can modify this logic based on your merge strategy
    SELECT * FROM temp_data
    
    UNION ALL
    
    -- Only include existing bronze data that's not in temp
    -- This is a simple merge strategy - modify as needed
    SELECT * FROM existing_bronze
    WHERE NOT EXISTS (
        SELECT 1 FROM temp_data 
        WHERE existing_bronze.asset_id = temp_data.asset_id 
          AND existing_bronze.invoice_id = temp_data.invoice_id -- Use appropriate composite key
    )
)

SELECT * FROM merged_data
