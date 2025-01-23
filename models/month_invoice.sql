-- models/month_invoice.sql

WITH invoice_data AS (
    SELECT
        DATE(i.TRANSACTION_TIMESTAMP) AS transaction_date,
        k.COUNTRY AS customer_country_code,  -- Using the source table for COUNTRY
        i.REGION,
        i.ZONE,
        i.CUST_NUMBER AS cust_number,  -- Explicitly refer to CUST_NUMBER from stage_invoice_raw
        c.FIRST_NAME || ' ' || c.LAST_NAME AS cust_name,
        k.LOCATION AS cust_location, -- Using LOCATION from stage_cust_mstr_kna1
        ptn.PRODUCT_NAME,  -- Using PRODUCT_NAME from stage_prod_mstr_tpna1
        i.PRODUCT_ID AS product_id,
        pna.CATEGORY_CODE AS product_category,
        SUM(COALESCE(i.QUANTITY, 0)) AS total_quantity,
        SUM(COALESCE(i.QUANTITY, 0) * COALESCE(pna.PRODUCT_PRICING, 0)) AS total_value,
        SUM(COALESCE((i.QUANTITY * pna.PRODUCT_PRICING) * (pna.PRODUCT_MARGIN / 100), 0)) AS total_margin,
        COUNT(*) AS total_order
    FROM {{ source('stage', 'STAGE_INVOICE_RAW') }} AS i
    JOIN {{ source('stage', 'STAGE_CUST_MSTR_TKNA1') }} AS c
        ON i.CUST_NUMBER = c.CUST_NUMBER
    JOIN {{ source('stage', 'STAGE_CUST_MSTR_KNA1') }} AS k
        ON i.CUST_NUMBER = k.CUSTOMERNUMBER
    JOIN {{ source('stage', 'STAGE_PROD_MSTR_PNA1') }} AS pna
        ON i.PRODUCT_ID = pna.PRODUCT_ID
    JOIN {{ source('stage', 'STAGE_PROD_MSTR_TPNA1') }} AS ptn
        ON i.PRODUCT_ID = ptn.PRODUCT_ID
    GROUP BY
        transaction_date,
        customer_country_code,
        i.REGION,
        i.ZONE,
        i.CUST_NUMBER,
        cust_name,
        cust_location,
        ptn.PRODUCT_NAME,
        i.PRODUCT_ID,
        product_category
),

month_invoice_data AS (
    SELECT
        MONTH(transaction_date) AS transaction_month,
        customer_country_code,
        REGION AS region,
        ZONE AS zone,
        cust_number,
        cust_name,
        cust_location,
        product_name,
        product_id,
        product_category,
        SUM(COALESCE(total_quantity, 0)) AS total_quantity,
        SUM(COALESCE(total_value, 0)) AS total_value,
        SUM(COALESCE(total_margin, 0)) AS total_margin,
        SUM(COALESCE(total_order, 0)) AS total_order
    FROM invoice_data
    GROUP BY
        transaction_month,
        customer_country_code,
        region,
        zone,
        cust_number,
        cust_name,
        cust_location,
        product_name,
        product_id,
        product_category
)

SELECT
    transaction_month,
    customer_country_code,
    region,
    zone,
    cust_number,
    cust_name,
    cust_location,
    product_name,
    product_id,
    product_category,
    total_quantity,
    total_value,
    total_margin,
    total_order
FROM month_invoice_data
