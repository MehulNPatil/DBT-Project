-- models/staging/stg_prod_mstr_pna1.sql

with base as (
    select 
        product_id,
        product_pricing,
        product_margin,
        prod_date,
        category_code
    from {{ source('stage_prod_mstr_pna1', 'stage_prod_mstr_pna1') }}
)

select
    product_id,
    product_pricing,
    product_margin,
    prod_date,
    category_code
from base
