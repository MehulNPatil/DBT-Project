-- models/day_invoice.sql

-- models/day_invoice.sql

WITH invoice_data AS (
    SELECT
        date(i.TRANSACTION_TIMESTAMP) AS transaction_date,  -- Keep the full timestamp (date + time)
        k.COUNTRY AS customer_country_code, -- Join with STAGE_CUST_MSTR_KNA1 to get COUNTRY
        i.REGION,
        i.ZONE,
        i.CUST_NUMBER AS cust_number,  -- Explicitly refer to CUST_NUMBER from stage_invoice_raw
        c.FIRST_NAME || ' ' || c.LAST_NAME AS cust_name,
        k.LOCATION AS cust_location, -- Corrected to fetch LOCATION from STAGE_CUST_MSTR_KNA1 (aliased as k)
        ptn.PRODUCT_NAME,  -- Corrected to fetch PRODUCT_NAME from STAGE_PROD_MSTR_TPNA1 (aliased as ptn)
        i.PRODUCT_ID AS product_id,
        COALESCE(pna.CATEGORY_CODE, 'Unknown') AS product_category,
        SUM(COALESCE(i.QUANTITY, 0)) AS total_quantity,
        SUM(COALESCE(i.QUANTITY, 0) * COALESCE(pna.PRODUCT_PRICING, 0)) AS total_value,
        SUM(COALESCE((i.QUANTITY * pna.PRODUCT_PRICING) * (pna.PRODUCT_MARGIN / 100), 0)) AS total_margin,
        COUNT(*) AS total_order
    FROM {{ source('stage', 'STAGE_INVOICE_RAW') }} AS i
    JOIN {{ source('stage', 'STAGE_CUST_MSTR_TKNA1') }} AS c ON i.CUST_NUMBER = c.CUST_NUMBER
    JOIN {{ source('stage', 'STAGE_CUST_MSTR_KNA1') }} AS k ON i.CUST_NUMBER = k.CUSTOMERNUMBER
    JOIN {{ source('stage', 'STAGE_PROD_MSTR_PNA1') }} AS pna ON i.PRODUCT_ID = pna.PRODUCT_ID
    JOIN {{ source('stage', 'STAGE_PROD_MSTR_TPNA1') }} AS ptn ON i.PRODUCT_ID = ptn.PRODUCT_ID
    GROUP BY
        transaction_date,  -- Now using full TRANSACTION_TIMESTAMP
        customer_country_code,
        i.REGION,
        i.ZONE,
        i.CUST_NUMBER,
        cust_name,
        cust_location,
        ptn.PRODUCT_NAME,
        i.PRODUCT_ID,
        product_category
)
SELECT
    transaction_date,  -- This now includes both date and time
    customer_country_code,
    REGION AS region,
    ZONE AS zone,
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
FROM invoice_data

