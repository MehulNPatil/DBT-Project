-- models/staging/stg_cust_mstr_tkna1.sql

with base as (
    select 
        CUST_NUMBER,
        FIRST_NAME,
        LAST_NAME
    from {{ source('stage_cust_mstr_tkna1', 'stage_cust_mstr_tkna1') }}
)

select
    CUST_NUMBER,
    FIRST_NAME,
    LAST_NAME
from base
