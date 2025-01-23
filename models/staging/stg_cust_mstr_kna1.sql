with base as (
    select 
        CustomerNumber,
        Location,
        Country
    from {{ source('stage_cust_mstr_kna1', 'stage_cust_mstr_kna1') }}
)

select
    CustomerNumber,
    Location,
    Country
from base