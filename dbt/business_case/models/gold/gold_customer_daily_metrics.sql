-- Gold layer: Business-ready aggregated data
-- This model creates business metrics and KPIs

{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 
    standardized_customer_id,
    DATE(parsed_transaction_date) as transaction_date,
    
    -- Aggregated metrics
    COUNT(*) as total_transactions,
    SUM(amount) as total_amount,
    AVG(amount) as avg_transaction_amount,
    MIN(amount) as min_transaction_amount,
    MAX(amount) as max_transaction_amount,
    
    -- Time-based metrics
    COUNT(DISTINCT DATE(parsed_transaction_date)) as active_days,
    
    -- Metadata
    MIN(_loaded_at) as first_loaded_at,
    MAX(_loaded_at) as last_loaded_at,
    CURRENT_TIMESTAMP() as _aggregated_at

FROM {{ ref('silver_clean_transactions') }}
GROUP BY 
    standardized_customer_id,
    DATE(parsed_transaction_date)
