-- -- models/product_master.sql
-- This model will create a materialized view that aggregates data from prod_mstr_pna1
-- and prod_mstr_tpna1
with
    product_data as (
        select
            pna.product_id,
            pna.category_code,
            ptn.product_name,
            pna.product_pricing,
            pna.product_margin,
            pna.prod_date
        from {{ source("stage", "STAGE_PROD_MSTR_PNA1") }} as pna
        join
            {{ source("stage", "STAGE_PROD_MSTR_TPNA1") }} as ptn
            on pna.product_id = ptn.product_id
    )
select
    product_id as product_id,
    product_name as product_name,
    category_code as product_category,
    product_pricing as product_pricing,
    product_margin as product_margin,
    prod_date as product_date,
    case
        when category_code = 'CAT-A'
        then 'Snacks'
        when category_code = 'CAT-B'
        then 'Cereal'
        else 'Other'
    end as product_category_wise
from product_data
