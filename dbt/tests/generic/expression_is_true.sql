{#- Fails for rows where the SQL expression is not true, e.g. "low_price <= high_price". -#}
{% test expression_is_true(model, expression, column_name=None) %}
select * from {{ model }} where not ({{ expression }})
{% endtest %}
