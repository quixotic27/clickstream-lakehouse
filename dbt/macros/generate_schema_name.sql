{#- Use the schema we configure (silver/gold) exactly, instead of dbt's default
    "<target_schema>_<custom_schema>" naming (which would give silver_gold). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}