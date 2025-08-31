{{
  config(
    materialized='incremental',
    unique_key='asset_id',
    on_schema_change='fail',
    incremental_strategy='merge',
    schema='gold',
    description='Transaction-level payment analytics enriched with company information'
  )
}}

with payment_data as (
  select
    asset_id,
    invoice_id,
    buyer_tax_id,
    buyer_main_tax_id,
    coalesce(buyer_tax_id, buyer_main_tax_id) as primary_tax_id,
    
    -- Amount fields
    original_amount,
    expected_amount,
    paid_amount,
    payment_variance,
    
    -- Date fields
    due_date,
    paid_date,
    invoice_issue_date,
    
    -- Calculated fields
    payment_status,
    days_from_due_date,
    due_year,
    due_month,
    due_quarter,
    
    -- Data quality flags
    has_missing_key_fields,
    has_negative_amounts,
    has_invalid_dates,
    
    -- Timestamps
    created_at,
    updated_at,
    _loaded_at
    
  from {{ ref('installments_clean') }}
  
  {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
       or created_at > (select max(created_at) from {{ this }})
  {% endif %}
),

company_context as (
  select
    buyer_tax_id,
    share_capital,
    company_size,
    legal_nature,
    simples_option,
    is_mei,
    is_main_company,
    company_status,
    is_active,
    zipcode,
    main_cnae,
    standardized_state as state,
    standardized_uf as uf,
    standardized_city as city,
    data_quality_flag as company_data_quality
  from {{ ref('cnpj_ws_clean') }}
  where buyer_tax_id is not null
),

enriched_payments as (
  select
    p.*,
    
    -- Company information
    c.share_capital,
    c.company_size,
    c.legal_nature,
    c.simples_option,
    c.is_mei,
    c.is_main_company,
    c.company_status,
    c.is_active,
    c.zipcode,
    c.main_cnae,
    c.state,
    c.uf,
    c.city,
    c.company_data_quality,
    
    -- Payment behavior indicators
    case
      when p.payment_status = 'PAID' and p.days_from_due_date <= 0 then 'EARLY_PAYMENT'
      when p.payment_status = 'PAID' and p.days_from_due_date between 1 and 5 then 'ON_TIME_PAYMENT'
      when p.payment_status = 'PAID' and p.days_from_due_date between 6 and 30 then 'LATE_PAYMENT'
      when p.payment_status = 'PAID' and p.days_from_due_date > 30 then 'VERY_LATE_PAYMENT'
      when p.payment_status = 'OVERDUE' and p.days_from_due_date between 1 and 30 then 'RECENTLY_OVERDUE'
      when p.payment_status = 'OVERDUE' and p.days_from_due_date > 30 then 'SEVERELY_OVERDUE'
      else p.payment_status
    end as payment_behavior,
    
    -- Payment amount categories
    case
      when p.expected_amount < 100 then 'MICRO'
      when p.expected_amount < 1000 then 'SMALL'
      when p.expected_amount < 10000 then 'MEDIUM'
      when p.expected_amount < 100000 then 'LARGE'
      else 'XLARGE'
    end as payment_size_category,
    
    -- Payment variance indicators
    case
      when abs(p.payment_variance) <= 0.01 then 'EXACT_PAYMENT'
      when p.payment_variance > 0.01 then 'OVERPAYMENT'
      when p.payment_variance < -0.01 then 'UNDERPAYMENT'
      else 'EXACT_PAYMENT'
    end as payment_variance_type,
    
    -- Geographic indicators
    case
      when c.state in ('SÃO PAULO', 'RIO DE JANEIRO', 'MINAS GERAIS') then 'MAJOR_STATE'
      when c.state is not null then 'OTHER_STATE'
      else 'UNKNOWN_STATE'
    end as geographic_tier,
    
    -- Company risk indicators
    case
      when c.is_mei = true then 'MEI'
      when c.company_size = 'MICRO' then 'MICRO_COMPANY'
      when c.company_size = 'SMALL' then 'SMALL_COMPANY'
      when c.company_size in ('MEDIUM', 'LARGE') then 'MEDIUM_LARGE_COMPANY'
      else 'UNKNOWN_SIZE'
    end as company_risk_category,
    
    -- Data quality assessment
    case
      when p.has_missing_key_fields or p.has_negative_amounts or p.has_invalid_dates 
        or c.company_data_quality != 'VALID'
      then 'POOR_QUALITY'
      else 'GOOD_QUALITY'
    end as overall_data_quality
    
  from payment_data p
  left join company_context c on p.primary_tax_id = c.buyer_tax_id
),

final as (
  select
    -- Payment identifiers
    asset_id,
    invoice_id,
    buyer_tax_id,
    buyer_main_tax_id,
    primary_tax_id,
    
    -- Payment amounts and metrics
    original_amount,
    expected_amount,
    paid_amount,
    payment_variance,
    
    -- Payment timing
    due_date,
    paid_date,
    invoice_issue_date,
    payment_status,
    days_from_due_date,
    payment_behavior,
    
    -- Payment categorization
    payment_size_category,
    payment_variance_type,
    
    -- Time dimensions
    due_year,
    due_month,
    due_quarter,
    
    -- Company profile
    share_capital,
    company_size,
    legal_nature,
    simples_option,
    is_mei,
    is_main_company,
    company_status,
    is_active,
    zipcode,
    main_cnae,
    state,
    uf,
    city,
    
    -- Risk and geographic segmentation
    company_risk_category,
    geographic_tier,
    
    -- Data quality indicators
    has_missing_key_fields,
    has_negative_amounts,
    has_invalid_dates,
    company_data_quality,
    overall_data_quality,
    
    -- Metadata
    created_at,
    updated_at,
    _loaded_at,
    current_timestamp() as processed_at
    
  from enriched_payments
)

select * from final
