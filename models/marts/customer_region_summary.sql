-- models/marts/customer_region_summary.sql

with customer_data as (
    select
        c.CustomerNumber,
        c.Location,
        c.Country,
        t.FIRST_NAME,
        t.LAST_NAME
    from {{ ref('stg_cust_mstr_kna1') }} c
    left join {{ ref('stg_cust_mstr_tkna1') }} t
    on c.CustomerNumber = t.CUST_NUMBER
),

invoice_data as (
    select
        i.CUST_NUMBER,
        i.PRODUCT_ID,
        i.transaction_timestamp,
        i.region,
        i.zone,
        i.quantity
    from {{ ref('stg_invoice_raw') }} i
)

select
    c.CustomerNumber,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.Location,
    c.Country,
    i.region,
    sum(i.quantity) as total_quantity,
    sum(i.quantity * p.product_pricing) as total_sales
from customer_data c
join invoice_data i
    on c.CustomerNumber = i.CUST_NUMBER
join {{ ref('stg_prod_mstr_pna1') }} p
    on i.PRODUCT_ID = p.product_id
group by
    c.CustomerNumber,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.Location,
    c.Country,
    i.region
