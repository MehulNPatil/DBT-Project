with customer_data as (
    select
        c.CustomerNumber,
        c.Location,
        c.Country,
        t.FIRST_NAME,
        t.LAST_NAME
    from dbt_case_study.public.stg_cust_mstr_kna1 c
    left join dbt_case_study.public.stg_cust_mstr_tkna1 t
    on c.CustomerNumber = t.CUST_NUMBER
),

product_data as (
    select
        s.product_id,          -- Using 's' for stage_prod_mstr_pna1 table (alias for pricing and margin)
        p.product_name,        -- Using 'p' for stage_prod_mstr_tpna1 table (alias for product name)
        s.product_pricing,     -- 's' for stage_prod_mstr_pna1 table which contains 'product_pricing'
        s.product_margin,      -- 's' for stage_prod_mstr_pna1 table which contains 'product_margin'
        s.category_code        -- 's' for stage_prod_mstr_pna1 table which contains 'category_code'
    from dbt_case_study.public.stg_prod_mstr_tpna1 p
    left join dbt_case_study.public.stg_prod_mstr_pna1 s
    on p.product_id = s.product_id
),

invoice_data as (
    select
        i.CUST_NUMBER,
        i.PRODUCT_ID,
        i.transaction_timestamp,
        i.region,
        i.zone,
        i.quantity
    from dbt_case_study.public.stg_invoice_raw i
)

select
    c.CustomerNumber,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.Location,
    c.Country,
    p.product_name,
    p.product_pricing,
    p.product_margin,
    p.category_code,
    sum(i.quantity) as total_quantity,
    sum(i.quantity * p.product_pricing) as total_sales
from customer_data c
join invoice_data i
    on c.CustomerNumber = i.CUST_NUMBER
join product_data p
    on i.PRODUCT_ID = p.product_id
group by
    c.CustomerNumber,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.Location,
    c.Country,
    p.product_name,
    p.product_pricing,
    p.product_margin,
    p.category_code

