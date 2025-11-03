{%- materialization alert, adapter='snowflake' -%}
  {%- set warehouse = config.get('warehouse', none) -%}
  {%- set schedule = config.get('schedule') -%}
  {%- set action = config.get('action') -%}
  {%- set identifier = model['alias'] -%}
  {%- set suspend = config.get('suspend', false) -%}

  {%- set is_serverless = warehouse is none -%}

  -- check config argument
  {%- if not schedule %}
    {{ exceptions.raise_compiler_error("schedule is not specified. e.g. '60 MINUTE', 'USING CRON 0 9-17 * * SUN America/Los_Angeles'") }}
  {%- endif %}
  {%- if not action %}
    {# TODO: Create macros to generate the action. #}
    {%- set send_email_doc = "https://docs.snowflake.com/en/sql-reference/stored-procedures/system_send_email" -%}
    {%- set webhook_doc = "https://docs.snowflake.com/en/user-guide/notifications/webhook-notifications#sending-a-notification-to-a-webhook" -%}
    {{ exceptions.raise_compiler_error("action is not specified. e.g. CALL SYSTEM$SEND_EMAIL('my_email_int', 'first.last@example.com, first2.last2@example.com', 'Email Alert: Task A has finished.', 'Task A has successfully finished.\nStart Time: 10:10:32\nEnd Time: 12:15:45\nTotal Records Processed: 115678'). See " ~ send_email_doc ~ " or " ~ webhook_doc ~ " for more information.") }}
  {%- endif %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- BEGIN happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database) -%}
  {% call statement('pre-check') %}
    -- NOTE: This is a pre-check to ensure that the SQL is valid.
    {{ sql }}
  {% endcall%}
  {% call statement('main') -%}
    {{ dbt_snowflake_custom_materializations.snowflake_create_or_replace_alert_statement(relation=target_relation, schedule=schedule, action=action, sql=sql, warehouse=warehouse) }}
  {%- endcall %}
  {%- if suspend == true %}
    {{ dbt_snowflake_custom_materializations.snowflake_suspend_alert_statement(target_relation) }}
  {% else %}
    {{ dbt_snowflake_custom_materializations.snowflake_resume_alert_statement(target_relation) }}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  -- return
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
