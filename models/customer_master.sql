-- models/customer_master.sql
-- This model will create a materialized view that aggregates data from cust_mstr_kna1
-- and cust_mstr_tkna1.
with
    customer_data as (
        select
            k.customernumber,
            k.location,
            k.country,
            c.first_name || ' ' || c.last_name as full_name
        from testing.public.stage_cust_mstr_kna1 as k
        join
            testing.public.stage_cust_mstr_tkna1 as c
            on k.customernumber = c.cust_number
    ),

    country_code_data as (

        select country, countrydialingcode as country_code
        from {{ ref("country_code") }}
        where countrydialingcode not in ('+7', '+92')
    )
        select
            customer_data.customernumber as customer_number,
            customer_data.location as customer_location,
            customer_data.country as customer_country,
            customer_data.full_name as customer_name,
            ccd.country_code as cust_country_code
        from customer_data
        left join country_code_data ccd on customer_data.country = ccd.country
