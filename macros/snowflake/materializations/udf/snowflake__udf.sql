{%- materialization udf, adapter='snowflake', supported_languages=['sql', 'python'] -%}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set compiled_code = model['compiled_code'] -%}
  /* common parameters */
  {%- set arguments = config.get('arguments', default=[]) -%}
  /* ref. https://docs.snowflake.com/developer-guide/secure-udf-procedure */
  {%- set secure = config.get('secure', default=false) -%}
  /* ref. https://docs.snowflake.com/ja/sql-reference/sql/create-function#optional-parameters */
  {%- set immutable = config.get('immutable', default=false) -%}
  /* return user defined function properties */
  {%- set return_type = config.get('return_type', default=none) -%}
  {%- set return_type_table = config.get('return_type_table', default=[]) -%}
  {%- set return_not_null = config.get('return_not_null', default=none) -%}
  /* end common parameters */

  /* sql only properties*/
  {%- set memoizable = config.get('memoizable', default=false) -%}
  /* end sql*/

  /* python*/
  {%- set runtime_version = config.get('runtime_version', default=none) -%}
  {%- set packages = config.get('packages', default=[]) -%}
  {%- set imports = config.get('imports', default=[]) -%}
  {%- set external_access_integrations = config.get('external_access_integrations', default=[]) %}
  {%- set secrets = config.get('secrets', default={}) -%}
  /* end python*/

  {%- set null_input_behavior = config.get('null_input_behavior', 'called on null input')%}

  /* NOTE: This property is used to create the same function with different aliases. */
  {%- set function_alias = config.get('function_alias', default=none) -%}

  /* if specified 'return_type' */
  {%- if return_type is none and (return_type_table | length <= 0) -%}
    {{- exceptions.raise_compiler_error("'return_type' or 'return_type_table' must be specified.") -}}
  {%- endif -%}
  {%- if return_type is none and (return_type_table | length > 0) -%}
    {%- set columns = [] -%}
    {%- for col in return_type_table -%}
      {%- if not col.get('name', none) %}
        {{- exceptions.raise_compiler_error("'return_type_table' must contain 'name' key.") -}}
      {%- endif -%}
      {%- if not col.get('type', none) -%}
        {{- exceptions.raise_compiler_error("'return_type_table' must contain 'type' key.") -}}
      {%- endif -%}
      {%- do columns.append(col.name ~ ' ' ~ col.type) -%}
    {%- endfor -%}
    {%- set return_type = 'table(' ~ ','.join(columns) ~ ')' -%}
  {%- endif -%}

  {%- if return_type is not string -%}
    {{- exceptions.raise_compiler_error("'return_type' must be a string representing the data type, e.g., 'VARCHAR', 'TABLE(x INTEGER, y INTEGER)'.") -}}
  {%- endif -%}

  /* validate type of 'return_not_null' */
  {%- if return_not_null is not none and return_not_null is not boolean -%}
    {{- exceptions.raise_compiler_error("'return_not_null' must be a boolean value (True or False).") -}}
  {%- endif -%}

  /* validate input parameters */
  {%- for arg in arguments -%}
    {%- if not arg.get('name', None) -%}
      {{ exceptions.raise_compiler_error("Argument 'name' is required") }}
    {%- endif -%}
    {%- if not arg.get('type', None) -%}
      {{ exceptions.raise_compiler_error("Argument 'type' is required") }}
    {%- endif -%}
  {%- endfor -%}

  /* validate language properties */
  {% set languages = {
    'python': {
      'name': 'Python',
      'supported_runtime_versions': ['3.8', '3.9', '3.10', '3.11'],
      'supports_runtime_version': true,
      'supports_packages': true,
      'supports_imports': true,
      'supports_external_access_integrations': true,
      'supports_secrets': true,
      'supports_memoizable': false
    },
    'sql': {
      'name': 'SQL',
      'supports_runtime_version': false,
      'supports_packages': false,
      'supports_imports': false,
      'supports_external_access_integrations': false,
      'supports_secrets': false,
      'supports_memoizable': true
    }
  } %}
  {% set language_lower = language | lower %}
  {% if language_lower in languages %}
    {% set language_info = languages[language_lower] %}
    {{ dbt_snowflake_custom_materializations.validate_runtime_version(language_info, runtime_version) }}
    {{ dbt_snowflake_custom_materializations.validate_packages(language_info, packages) }}
    {{ dbt_snowflake_custom_materializations.validate_imports(language_info, imports) }}
    {{ dbt_snowflake_custom_materializations.validate_external_access_integrations(language_info, external_access_integrations) }}
    {{ dbt_snowflake_custom_materializations.validate_secrets(language_info, secrets) }}
    {{ dbt_snowflake_custom_materializations.validate_memoizable(language_info, memoizable) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported language: " ~ language) }}
  {% endif %}

  /* validate secrets and external_access_integrations */
  {%- if secrets %}
      {%- if not external_access_integrations %}
          {{ exceptions.raise_compiler_error("When 'secrets' is not empty, 'external_access_integrations' must also not be empty.") }}
      {%- endif %}
  {%- endif %}

  {%- set target_relation = api.Relation.create( identifier=identifier, schema=schema, database=database) -%}
  {%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- BEGIN happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set sql_statement %}
    SELECT
      TRANSFORM(
        -- ARGUMENT_SIGNATURE has argument names and type with parentheses.
        -- e.g. '(CHANNEL VARCHAR, MESSAGE VARCHAR, QUERY_ID VARCHAR)'
        FILTER(SPLIT(REGEXP_REPLACE(ARGUMENT_SIGNATURE, '^\\((.*)\\)$', '\\1'), ', '), v -> v != ''),
        v -> OBJECT_CONSTRUCT(
          'name', LOWER(SPLIT_PART(v, ' ', 1)),
          'type', LOWER(SPLIT_PART(v, ' ', 2))
        )
      ) AS STRUCTURED_ARGUMENT_SIGNATURE
    FROM {{ database }}.INFORMATION_SCHEMA.FUNCTIONS
    WHERE LOWER(FUNCTION_NAME) = '{{ identifier | lower }}' AND LOWER(FUNCTION_SCHEMA) = '{{ schema | lower }}';
  {% endset %}
  {% set adapter_response, data = adapter.execute(sql_statement, fetch=true) %}
  {% for row in data.rows %}
    {% set ns = namespace(is_drop_function_required=false) %}
    {% set existing_arguments = fromjson(row.STRUCTURED_ARGUMENT_SIGNATURE) %}
    {% if existing_arguments | length != arguments | length %}
      {% set ns.is_drop_function_required = true %}
    {% else %}
      {% for idx in range(arguments | length) %}
        {% if (
          arguments[idx].get('name').lower() != existing_arguments[idx].get('name').lower()
          or
          arguments[idx].get('type').lower() != existing_arguments[idx].get('type').lower()
        ) %}
          {% set ns.is_drop_function_required = true %}
          {{ log("[" ~ model["alias"] ~ "] existing: " ~ existing_arguments[idx].get('name').lower() ~ " " ~ existing_arguments[idx].get('type').lower() ~ ", desired: " ~ arguments[idx].get('name').lower() ~ " " ~ arguments[idx].get('type').lower(), info=True) }}
          {{ break }}
        {% endif %}
      {% endfor %}
    {% endif %}
    {% if ns.is_drop_function_required %}
      {{ log("[" ~ model["alias"] ~ "] New function signature does not have backward compatibility with existing function. Dropping function " ~ database ~ "." ~ schema ~ "." ~ identifier ~ "(" ~ existing_arguments | map(attribute='type') | join(',') ~ ")", info=True) }}
      {% call statement('drop_function[' ~ idx ~ ']') -%}
        DROP FUNCTION IF EXISTS {{ database }}.{{ schema }}.{{ identifier }}({{ existing_arguments | map(attribute='type') | join(',') }});
      {% endcall %}
      {% if function_alias %}
        {{ log("[" ~ model["alias"] ~ "] New function signature does not have backward compatibility with existing function. Dropping function " ~ database ~ "." ~ schema ~ "." ~ function_alias ~ "(" ~ existing_arguments | map(attribute='type') | join(',') ~ ")", info=True) }}
        {% call statement('drop_function_alias[' ~ idx ~ ']') -%}
          DROP FUNCTION IF EXISTS {{ database }}.{{ schema }}.{{ function_alias }}({{ existing_arguments | map(attribute='type') | join(',') }});
        {% endcall %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% call statement('main') -%}
    {{
      dbt_snowflake_custom_materializations.snowflake_create_or_alter_function_statement(
        target_relation, language, secure, immutable, arguments, return_type, return_not_null, compiled_code,
        target_path, memoizable, runtime_version, handler_name, packages, imports, external_access_integrations, secrets,
        null_input_behavior
      )
    }}
  {%- endcall %}

  {% if function_alias %}
    {%- set target_relation_alias = api.Relation.create( identifier=function_alias, schema=schema, database=database) -%}
    {% call statement('main') -%}
      {{
        dbt_snowflake_custom_materializations.snowflake_create_or_alter_function_statement(
          target_relation_alias, language, secure, immutable, arguments, return_type, return_not_null, compiled_code,
          target_path, memoizable, runtime_version, handler_name, packages, imports, external_access_integrations, secrets,
          null_input_behavior
        )
      }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  -- return
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
