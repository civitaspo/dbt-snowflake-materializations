{% macro snowflake_suspend_alert_statement(target_relation) -%}
  {% call statement('suspend_alert') -%}
    alter alert {{ target_relation }} suspend
  {%- endcall %}
{% endmacro %}
