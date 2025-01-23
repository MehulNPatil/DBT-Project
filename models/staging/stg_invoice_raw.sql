-- models/staging/stg_invoice_raw.sql

with base as (
    select 
        CUST_NUMBER,
        PRODUCT_ID,
        transaction_timestamp,
        region,
        zone,
        quantity
    from {{ source('stage_invoice_raw', 'stage_invoice_raw') }}
)

select
    CUST_NUMBER,
    PRODUCT_ID,
    transaction_timestamp,
    region,
    zone,
    quantity
from base
