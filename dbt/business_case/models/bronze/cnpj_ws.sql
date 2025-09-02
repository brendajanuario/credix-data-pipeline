{{
  config(
    materialized = 'incremental',
    unique_key = 'buyer_tax_id',
    merge_update_columns = [
        'share_capital',
        'company_size',
        'legal_nature',
        'simples_option',
        'is_mei',
        'is_main_company',
        'company_status',
        'is_active',
        'zipcode',
        'main_cnae',
        'state',
        'uf',
        'city',
        'created_at',
        'updated_at'
    ],
    schema='bronze',
    meta={'dagster': {'group': 'cnpj_pipeline'}}
  )
}}

SELECT
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
    buyer_tax_id,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'business_case_temp.cnpj_ws' as _source_table
FROM
    {{ source('business_case_temp', 'cnpj_ws') }}