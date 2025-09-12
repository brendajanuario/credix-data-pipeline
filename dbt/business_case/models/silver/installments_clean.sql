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
  CAST(due_date AS DATE) as due_date,
  CAST(paid_date AS DATE) as paid_date,
  CAST(invoice_issue_date AS DATE) as invoice_issue_date,

  -- Calculate payment status
  CASE
    WHEN paid_date IS NOT NULL THEN 'PAID'
    WHEN CAST(due_date AS DATE) < CURRENT_DATE() THEN 'OVERDUE'
    ELSE 'PENDING'
  END as payment_status,
  
  -- Calculate days from due date
  CASE
    WHEN paid_date IS NOT NULL THEN DATE_DIFF(CAST(paid_date AS DATE), CAST(due_date AS DATE), DAY)
    ELSE DATE_DIFF(CURRENT_DATE(), CAST(due_date AS DATE), DAY)
  END as days_from_due_date,
  CURRENT_TIMESTAMP() as _loaded_at


FROM product-reliability-analyzer.business_case_bronze.oltp_business_case_installments
WHERE
  asset_id IS NOT NULL
  AND buyer_tax_id IS NOT NULL
  AND expected_amount_in_cents IS NOT NULL
  AND due_date IS NOT NULL
  -- sanity checks for negative values
  AND (original_amount_in_cents IS NULL OR original_amount_in_cents >= 0)
  AND expected_amount_in_cents >= 0
  AND (paid_amount_in_cents IS NULL OR paid_amount_in_cents >= 0)

-- {% if is_incremental() %}
--   WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
-- {% endif %}
