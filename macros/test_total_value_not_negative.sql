-- macros/test_total_value_not_negative.sql

-- macros/test_total_value_not_negative.sql

{% test total_value_not_negative(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endtest %}
