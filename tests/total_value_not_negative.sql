-- tests/total_value_not_negative.sql

-- Custom Generic Tests

-- Checking negative values
SELECT *
FROM {{ ref('day_invoice') }}
WHERE total_value < 0
