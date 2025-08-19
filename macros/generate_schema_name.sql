{% macro generate_schema_name(custom_schema_name, node) -%}
    {% if target.name.startswith('pr') %}
        {{ 'dbt_cloud_pr_' ~ target.schema }}
    {% else %}
        {{ custom_schema_name | default(target.schema) }}
    {% endif %}
{%- endmacro %}
