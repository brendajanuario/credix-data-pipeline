{{
  config(
    materialized = 'incremental',
    unique_key = 'asset_id',
    merge_update_columns = [
      'invoice_id',
      'buyer_tax_id',
      'original_amount_in_cents',
      'expected_amount_in_cents',
      'paid_amount_in_cents',
      'due_date',
      'paid_date',
      'invoice_issue_date',
      'buyer_main_tax_id'
    ],
    schema = 'bronze',
    meta = {'dagster': {'group': 'installments_pipeline'}}
  )
}}

SELECT
  asset_id,
  invoice_id,
  buyer_tax_id,
  original_amount_in_cents,
  expected_amount_in_cents,
  paid_amount_in_cents,
  due_date,
  paid_date,
  invoice_issue_date,
  buyer_main_tax_id,
  CURRENT_TIMESTAMP() as _loaded_at,
  'business_case_installments' as _source_table
FROM 
    {{ source('business_case_temp', 'installments') }}