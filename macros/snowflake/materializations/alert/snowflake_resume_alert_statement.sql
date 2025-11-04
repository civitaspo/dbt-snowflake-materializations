{% macro snowflake_resume_alert_statement(target_relation) -%}
  {% call statement('resume_alert') -%}
    alter alert {{ target_relation }} resume
  {%- endcall %}
{% endmacro %}
