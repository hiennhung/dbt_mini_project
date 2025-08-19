{% macro generate_schema_name(custom_schema_name, node) -%}
    {% if target.name.startswith('pr') %}
        {{ 'test' ~ target.schema }}
    {% else %}
        {{ custom_schema_name | default(target.schema) }}
    {% endif %}
{%- endmacro %}
