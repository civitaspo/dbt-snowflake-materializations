{%- macro snowflake_create_or_replace_alert_statement(relation, schedule, action, sql, warehouse) -%}

{{ log("Creating Alert " ~ relation) }}
create or replace alert {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if warehouse is not none %}
    warehouse = {{ warehouse }}
    {% endif %}
    schedule = '{{ schedule }}'
    if( exists(
        {{ sql }}
    ))
    then
        {{ action }}
    ;
{%- endmacro -%}
