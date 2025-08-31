-- Gold layer: Business-ready aggregated data
-- This model creates business metrics and KPIs

{{ config(
    materialized='table',
    schema='gold',
    meta={'dagster': {'group': 'cnpj_pipeline'}}

) }}

SELECT 
    *
FROM {{ ref('cnpj_ws_clean') }}
