{% test rowcount_impact_crossdb(model, prod_database, threshold=0.9) %}

{%- set prod_relation = api.Relation.create(
    database=prod_database,
    schema=model.schema,
    identifier=model.identifier
) -%}

{%- set sql %}
    with current_model as (
        select count(*) as row_count from {{ model }}
    ),
    prod_model as (
        select count(*) as row_count from {{ prod_relation }}
    )
    select
        current_model.row_count as current_row_count,
        prod_model.row_count as prod_row_count,
        case
            when current_model.row_count = 0 then 0
            when prod_model.row_count > 0 and (current_model.row_count / prod_model.row_count) < {{ threshold }} then 0
            else 1
        end as test_result
    from current_model, prod_model
{%- endset %}

{% if execute %}
    {{ return(run_query(sql)) }}
{% endif %}

{% endtest %}
