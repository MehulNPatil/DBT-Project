-- models/customer_master.sql

-- This model will create a materialized view that aggregates data from cust_mstr_kna1 and cust_mstr_tkna1.
WITH customer_data AS (
    SELECT
        k.CUSTOMERNUMBER,
        k.LOCATION,
        k.COUNTRY,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS full_name
    FROM {{ source('stage', 'STAGE_CUST_MSTR_KNA1') }} AS k
    JOIN {{ source('stage', 'STAGE_CUST_MSTR_TKNA1') }} AS c
        ON k.CUSTOMERNUMBER = c.CUST_NUMBER
)
SELECT
    CUSTOMERNUMBER AS customer_number,
    LOCATION AS customer_location,
    COUNTRY AS customer_country,
    full_name AS customer_name
FROM customer_data
