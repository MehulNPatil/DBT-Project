-- models/staging/stg_prod_mstr_tpna1.sql

with base as (
    select 
        product_id,
        product_name
    from {{ source('stage_prod_mstr_tpna1', 'stage_prod_mstr_tpna1') }}
)

select
    product_id,
    product_name
from base
