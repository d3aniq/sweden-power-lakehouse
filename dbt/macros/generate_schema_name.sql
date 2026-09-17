{#
    Utan den här macron döper dbt scheman till <target>_<custom>, alltså
    silver_gold. Vi vill att modellerna hamnar i exakt silver och gold,
    samma scheman som notebookarna skriver till.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
