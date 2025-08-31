{{
  config(
    materialized='incremental',
    unique_key='asset_id',
    on_schema_change='fail',
    incremental_strategy='merge'
  )
}}

with source_data as (
  select
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
    created_at,
    updated_at
  from {{ source('business_case_temp', 'installments') }}
),

final as (
  select
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
    created_at,
    updated_at,
    current_timestamp() as _loaded_at,
    '{{ this }}' as _source_table
  from source_data
  
  {% if is_incremental() %}
    -- Only load new or updated records
    where updated_at > (select max(updated_at) from {{ this }})
       or created_at > (select max(created_at) from {{ this }})
  {% endif %}
)

select * from final
