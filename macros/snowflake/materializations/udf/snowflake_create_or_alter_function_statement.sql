{%- macro snowflake_create_or_alter_function_statement(
        relation, language, secure, immutable, arguments, return_type, return_not_null, compiled_code,
        target_path, memoizable, runtime_version, handler_name, packages, imports, external_access_integrations, secrets,
        null_input_behavior
) -%}

{{ log("Creating Function " ~ relation) }}
create or alter{% if secure %} secure{% endif %} function {{ relation.include(database=(not temporary), schema=(not temporary)) }}(
{%- for arg in arguments %}
  {{ arg['name'] }} {{ arg['type'] }} {% if arg.get('default', none) is not none%}default {{ arg['default'] }}{% endif %}
  {%- if not loop.last %},{% endif %}
{%- endfor %}
)
returns {{ return_type }}
{%- if return_not_null %}
  NOT NULL
{%- elif return_not_null is boolean %}
  NULL
{%- endif %}
language {{ language }}
{%- if immutable %}
immutable
{%- else %}
volatile
{%- endif %}
{%- if runtime_version is not none %}
runtime_version = '{{ runtime_version }}'
{%- endif %}
{%- if imports | length > 0 %}
imports = (
    {%- for i in imports %}
  '{{ i }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
{%- if packages | length > 0 %}
packages = (
    {%- for p in packages %}
  '{{ p }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
{%- if language | lower == 'python' %}
handler = 'main'
{%- endif %}
{%- if external_access_integrations | length > 0 %}
external_access_integrations = (
    {%- for e in external_access_integrations %}
  {{ e }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
{%- if secrets | length > 0 %}
secrets = (
    {%- for k, v in secrets %}
  '{{ k }}' = {{ v }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
{%- if memoizable %}
memoizable
{%- endif %}
AS
$$

{{ compiled_code }}

{% if language == 'python' %}

# Snowflake UDF handle snowpark session, so we need to create a new class to create dbt objects without session.
class pseudoDbtObj:
    def __init__(self) -> None:
        self.source = lambda *args: self._raise_exception("source is not supported")
        self.ref = lambda *args, **kwargs: self._raise_exception("ref is not supported")
        self.config = config
        self.this = this()
        self.is_incremental = False

    def _raise_exception(self, message):
        raise Exception(message)

def main(*args, **kwargs):
    dbt = pseudoDbtObj()
    model(dbt, session=None)
    return udf(dbt, *args, **kwargs)

{% endif %}
$$

{%- endmacro -%}
