{#- Fails for rows where the column is zero or negative (NULLs are not_null's job). -#}
{% test positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} <= 0
{% endtest %}
