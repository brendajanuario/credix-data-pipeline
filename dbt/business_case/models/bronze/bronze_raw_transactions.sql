-- Bronze layer: Raw data from external sources
-- This model creates a table from raw parquet data

{{ config(
    materialized='table',
    schema='bronze'
) }}

-- Example bronze model - replace with your actual data structure
SELECT 
    *,
    CURRENT_TIMESTAMP() as _loaded_at,
    '_bronze_raw_transactions' as _source_table
FROM `{{ var('gcp_project') }}.{{ var('raw_dataset') }}.raw_transactions`
WHERE 1=1
    -- Add any basic filtering here
    {% if var('start_date') %}
        AND DATE(_loaded_at) >= '{{ var('start_date') }}'
    {% endif %}
