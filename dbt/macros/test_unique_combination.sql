{#- Generic test: fails if the combination of columns is not unique (the table's grain).
    Same idea as dbt_utils.unique_combination_of_columns, without needing the package. -#}
{% test unique_combination_of_columns(model, columns) %}
select {{ columns | join(', ') }}, count(*) as copies
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}