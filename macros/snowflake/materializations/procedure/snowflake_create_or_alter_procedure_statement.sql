{%- macro snowflake_create_or_alter_procedure_statement(
        relation, language, arguments, return_type, execute_as, compiled_code,
        runtime_version, packages, imports, external_access_integrations, secrets, comment
) -%}

{{ log("Creating Procedure " ~ relation) }}
create or alter procedure {{ relation.include(database=(not temporary), schema=(not temporary)) }}(
{%- for arg in arguments %}
  {{ arg['name'] }} {{ arg['type'] }} {% if arg.get('default', none) is not none%}default {{ arg['default'] }}{% endif %}
  {%- if not loop.last %},{% endif %}
{%- endfor %}
)
returns {{ return_type }}
language {{ language }}
{%- if language == 'python' %}
runtime_version = '{{ runtime_version }}'
  {%- if packages | length > 0 %}
packages = (
    {%- for p in packages %}
  '{{ p }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
  {%- endif %}
  {%- if imports | length > 0 %}
imports = (
    {%- for i in imports %}
  '{{ i }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
  {%- endif %}
handler = 'main'
  {%- if external_access_integrations | length > 0 %}
external_access_integrations = (
    {%- for e in external_access_integrations %}
  {{ e }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
  {%- endif %}
  {%- if secrets | length > 0 %}
secrets = (
    {%- for k, v in secrets.items() %}
  '{{ k }}' = {{ v }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
  {%- endif %}
{%- endif %}
comment = $$
{{ comment }}
$$
execute as {{ execute_as }}
AS
$$

{{ compiled_code }}

{% if language == 'python' %}
def main(session, *args, **kwargs):
    dbt = dbtObj(session.table)
    # NOTE: model function should have two args, `dbt` and a session to current warehouse.
    #       So, we define procedure function to accept Snowflake Procedure arguments.
    #       ref. https://github.com/dbt-labs/dbt-core/blob/3f56cbce5f30477e5deea2f649943d7241f3aa17/core/dbt/parser/models.py#L45-L48
    model(dbt, session)
    return procedure(dbt, session, *args, **kwargs)

{% endif %}

$$
;

{%- endmacro -%}
