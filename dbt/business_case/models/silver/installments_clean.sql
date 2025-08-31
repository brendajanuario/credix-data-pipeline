{{
  config(
    materialized='incremental',
    unique_key='asset_id',
    on_schema_change='fail',
    incremental_strategy='merge'
  )
}}

with bronze_data as (
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
    _loaded_at
  from {{ ref('installments') }}
),

cleaned_data as (
  select
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
    case
      when paid_date is not null then 'PAID'
      when due_date < current_date() then 'OVERDUE'
      else 'PENDING'
    end as payment_status,
    
    -- Calculate days overdue/until due
    case
      when paid_date is not null then date_diff(paid_date, due_date, day)
      else date_diff(current_date(), due_date, day)
    end as days_from_due_date,
    
    -- Payment variance
    case
      when paid_amount_in_cents is not null and expected_amount_in_cents is not null
      then round((paid_amount_in_cents - expected_amount_in_cents) / 100.0, 2)
      else null
    end as payment_variance,
    
    -- Time dimensions
    extract(year from due_date) as due_year,
    extract(month from due_date) as due_month,
    extract(quarter from due_date) as due_quarter,
    
    created_at,
    updated_at,
    _loaded_at
  from bronze_data
),

final as (
  select
    *,
    -- Data quality flags
    case
      when asset_id is null or invoice_id is null then true
      else false
    end as has_missing_key_fields,
    
    case
      when original_amount_in_cents < 0 
        or expected_amount_in_cents < 0 
        or (paid_amount_in_cents is not null and paid_amount_in_cents < 0)
      then true
      else false
    end as has_negative_amounts,
    
    case
      when due_date < invoice_issue_date then true
      when paid_date < invoice_issue_date then true
      else false
    end as has_invalid_dates
    
  from cleaned_data
  
  {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
       or created_at > (select max(created_at) from {{ this }})
  {% endif %}
)

select * from final
