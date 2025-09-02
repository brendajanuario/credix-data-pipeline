{{
  config(
    materialized='incremental',
    unique_key='asset_id',
    incremental_strategy='merge',
    meta={'dagster': {'group': 'installments_pipeline'}}
  )
}}

SELECT
  asset_id,
  invoice_id,
  buyer_tax_id,
  buyer_main_tax_id,
  
  -- Convert amounts from cents to currency
  round(original_amount_in_cents / 100.0, 2) as original_amount,
  round(expected_amount_in_cents / 100.0, 2) as expected_amount,
  round(paid_amount_in_cents / 100.0, 2) as paid_amount,
  
  -- Date fields
  due_date,
  paid_date,
  invoice_issue_date,
  
  -- Calculate payment status
  CASE
    WHEN paid_date IS NOT NULL THEN 'PAID'
    WHEN due_date < CURRENT_DATE() THEN 'OVERDUE'
    ELSE 'PENDING'
  END as payment_status,
  
  -- Calculate days from due date
  CASE
    WHEN paid_date IS NOT NULL THEN DATE_DIFF(paid_date, due_date, DAY)
    ELSE DATE_DIFF(CURRENT_DATE(), due_date, DAY)
  END as days_from_due_date,
  
  _loaded_at
  
FROM {{ ref('installments') }}

-- {% if is_incremental() %}
--   WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
-- {% endif %}
