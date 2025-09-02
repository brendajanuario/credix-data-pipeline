{{
  config(
    meta={'dagster': {'group': 'gold_layer'}},
    materialized='table',
    schema='gold',
    description='Company-level payment analytics and risk scoring',
  )
}}

with company_data as (
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
    data_quality_flag as company_data_quality,
    created_at as company_created_at,
    updated_at as company_updated_at
  from {{ ref('cnpj_ws_clean') }}
  where buyer_tax_id is not null
),

payment_aggregations as (
  select
    coalesce(buyer_tax_id, buyer_main_tax_id) as tax_id,

    -- Payment volume metrics
    count(*) as total_installments,
    count(case when payment_status = 'PAID' then 1 end) as paid_installments,
    count(case when payment_status = 'OVERDUE' then 1 end) as overdue_installments,
    count(case when payment_status = 'PENDING' then 1 end) as pending_installments,

    -- Amount metrics (in currency, not cents)
    sum(original_amount) as total_original_amount,
    sum(expected_amount) as total_expected_amount,
    sum(paid_amount) as total_paid_amount,
    sum(case when payment_status = 'OVERDUE' then expected_amount else 0 end) as total_overdue_amount,

    -- Payment timing metrics
    avg(case when payment_status = 'PAID' then days_from_due_date end) as avg_payment_days,
    min(case when payment_status = 'PAID' then days_from_due_date end) as best_payment_days,
    max(case when payment_status = 'PAID' then days_from_due_date end) as worst_payment_days,

    -- Date ranges
    min(due_date) as earliest_due_date,
    max(due_date) as latest_due_date,
    min(invoice_issue_date) as first_invoice_date,
    max(invoice_issue_date) as last_invoice_date,

    -- Latest data timestamp
    max(_loaded_at) as last_updated

  from {{ ref('installments_clean') }}
  where coalesce(buyer_tax_id, buyer_main_tax_id) is not null
  group by coalesce(buyer_tax_id, buyer_main_tax_id)
),

calculated_metrics as (
  select
    *,
    -- Payment performance ratios
    round(
      case when total_installments > 0 
      then (paid_installments * 100.0) / total_installments 
      else 0 end, 2
    ) as payment_completion_rate,

    round(
      case when total_installments > 0 
      then (overdue_installments * 100.0) / total_installments 
      else 0 end, 2
    ) as overdue_rate,

    -- Financial ratios
    round(
      case when total_expected_amount > 0 
      then (total_paid_amount * 100.0) / total_expected_amount 
      else 0 end, 2
    ) as amount_recovery_rate,

    round(
      case when total_expected_amount > 0 
      then (total_overdue_amount * 100.0) / total_expected_amount 
      else 0 end, 2
    ) as overdue_amount_rate,

    -- Average amounts
    round(
      case when total_installments > 0 
      then total_expected_amount / total_installments 
      else 0 end, 2
    ) as avg_installment_amount,

    -- Since silver layer doesn't include per-row data quality flags, default to 100
    cast(100 as numeric) as data_quality_score

  from payment_aggregations
),

risk_scoring as (
  select
    *,
    -- Risk scoring based on payment behavior
    case
      when payment_completion_rate >= 95 and avg_payment_days <= 5 and overdue_rate <= 5 then 'EXCELLENT'
      when payment_completion_rate >= 85 and avg_payment_days <= 15 and overdue_rate <= 15 then 'GOOD'
      when payment_completion_rate >= 70 and avg_payment_days <= 30 and overdue_rate <= 25 then 'FAIR'
      when payment_completion_rate >= 50 and overdue_rate <= 40 then 'POOR'
      else 'HIGH_RISK'
    end as payment_tier,

    -- Numerical risk score (0-100, higher = better)
    round(
      greatest(0, least(100,
        (payment_completion_rate * 0.4) +
        (case when avg_payment_days <= 0 then 30 
              when avg_payment_days <= 10 then 20 
              when avg_payment_days <= 30 then 10 
              else 0 end) +
        ((100 - overdue_rate) * 0.3)
      )), 2
    ) as risk_score

  from calculated_metrics
),

final as (
  select
    c.buyer_tax_id,

    -- Company profile
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

    -- Payment metrics
    coalesce(r.total_installments, 0) as total_installments,
    coalesce(r.paid_installments, 0) as paid_installments,
    coalesce(r.overdue_installments, 0) as overdue_installments,
    coalesce(r.pending_installments, 0) as pending_installments,

    -- Financial metrics
    coalesce(r.total_original_amount, 0) as total_original_amount,
    coalesce(r.total_expected_amount, 0) as total_expected_amount,
    coalesce(r.total_paid_amount, 0) as total_paid_amount,
    coalesce(r.total_overdue_amount, 0) as total_overdue_amount,

    -- Performance metrics
    coalesce(r.payment_completion_rate, 0) as payment_completion_rate,
    coalesce(r.overdue_rate, 0) as overdue_rate,
    coalesce(r.amount_recovery_rate, 0) as amount_recovery_rate,
    coalesce(r.avg_payment_days, 0) as avg_payment_days,
    coalesce(r.avg_installment_amount, 0) as avg_installment_amount,

    -- Risk assessment
    coalesce(r.payment_tier, 'NO_HISTORY') as payment_tier,
    coalesce(r.risk_score, 0) as risk_score,

    -- Data quality
    c.company_data_quality,
    coalesce(r.data_quality_score, 100) as payment_data_quality_score,

    -- Timestamps
    r.first_invoice_date,
    r.last_invoice_date,
    r.earliest_due_date,
    r.latest_due_date,
    c.company_created_at,
    c.company_updated_at,
    current_timestamp() as processed_at

  from company_data c
  left join risk_scoring r on c.buyer_tax_id = r.tax_id
)

select * from final
