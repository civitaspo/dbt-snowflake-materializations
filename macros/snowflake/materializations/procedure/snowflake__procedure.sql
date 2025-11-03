{%- materialization procedure, adapter='snowflake', supported_languages=['sql', 'python'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}
  {%- set compiled_code = model['compiled_code'] -%}
  {%- set comment = model['description'] -%}

  {%- set arguments = config.get('arguments', default=[]) -%} -- name, type, default
  {%- set return_type = config.get('return_type', default=none) -%}
  {%- set return_type_table = config.get('return_type_table', default=[]) -%}
  {%- set execute_as = config.get('execute_as', default='caller' ) -%}
  {%- set runtime_version = config.get('runtime_version', default='3.11' ) -%}
  {%- set packages = config.get('packages', default=[]) -%}
  {%- set imports = config.get('imports', default=[]) -%}
  {%- set external_access_integrations = config.get('external_access_integrations', default=[]) -%}
  {%- set secrets = config.get('secrets', default={}) -%}

  /* NOTE: This property is used to create the same procedure with different aliases. */
  {%- set procedure_alias = config.get('procedure_alias', default=none) -%}

  {%- if return_type is none and (return_type_table | length <= 0) -%}
    /* NOTE: Set Default return_type = varchar for backward compatibility. */
    {%- set return_type = 'varchar' -%}
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
  {%- if language == 'python' -%}
    {%- for arg in arguments -%}
      {%- if not arg.get('name', None) -%}
        {{ exceptions.raise_compiler_error("Argument 'name' is required for Python procedures") }}
      {%- endif -%}
      {%- if not arg.get('type', None) -%}
        {{ exceptions.raise_compiler_error("Argument 'type' is required for Python procedures") }}
      {%- endif -%}
    {%- endfor -%}
    {%- if 'snowflake-snowpark-python' not in packages -%}
      {{ exceptions.raise_compiler_error("Package 'snowflake-snowpark-python' is required for Python procedures") }}
    {%- endif -%}
    -- https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-python#prerequisites-for-writing-stored-procedures-locally
    {%- set supported_python_runtime_versions = ['3.8', '3.9', '3.10', '3.11'] -%}
    {%- if runtime_version not in supported_python_runtime_versions -%}
      {{ exceptions.raise_compiler_error("Runtime version '" ~ runtime_version ~ "' is not supported for Python procedures. Supported versions: " ~ supported_python_runtime_versions | join(', ')) }}
    {%- endif -%}
  {%- endif -%}

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
    FROM {{ database }}.INFORMATION_SCHEMA.PROCEDURES
    WHERE LOWER(PROCEDURE_NAME) = '{{ identifier | lower }}' AND LOWER(PROCEDURE_SCHEMA) = '{{ schema | lower }}';
  {% endset %}
  {% set adapter_response, data = adapter.execute(sql_statement, fetch=true) %}
  {% for row in data.rows %}
    {% set ns = namespace(is_drop_procedure_required=false) %}
    {% set existing_arguments = fromjson(row.STRUCTURED_ARGUMENT_SIGNATURE) %}
    {% if existing_arguments | length != arguments | length %}
      {{ log("[" ~ model["alias"] ~ "] existing: " ~ tojson(existing_arguments) ~ " , desired: " ~ tojson(arguments), info=True) }}
      {% set ns.is_drop_procedure_required = true %}
    {% else %}
      {% for idx in range(arguments | length) %}
        {% if (
          arguments[idx].get('name').lower() != existing_arguments[idx].get('name').lower()
          or
          arguments[idx].get('type').lower() != existing_arguments[idx].get('type').lower()
        ) %}
          {% set ns.is_drop_procedure_required = true %}
          {{ log("[" ~ model["alias"] ~ "] existing: " ~ existing_arguments[idx].get('name').lower() ~ " " ~ existing_arguments[idx].get('type').lower() ~ ", desired: " ~ arguments[idx].get('name').lower() ~ " " ~ arguments[idx].get('type').lower(), info=True) }}
          {{ break }}
        {% endif %}
      {% endfor %}
    {% endif %}
    {% if ns.is_drop_procedure_required %}
      {{ log("[" ~ model["alias"] ~ "] New procedure signature does not have backward compatibility with existing procedure. Dropping procedure " ~ database ~ "." ~ schema ~ "." ~ identifier ~ "(" ~ existing_arguments | map(attribute='type') | join(',') ~ ")", info=True) }}
      {% call statement('drop_procedure[' ~ idx ~ ']') -%}
        DROP PROCEDURE IF EXISTS {{ database }}.{{ schema }}.{{ identifier }}({{ existing_arguments | map(attribute='type') | join(',') }});
      {% endcall %}
      {% if procedure_alias %}
        {{ log("[" ~ model["alias"] ~ "] New procedure signature does not have backward compatibility with existing procedure. Dropping procedure " ~ database ~ "." ~ schema ~ "." ~ procedure_alias ~ "(" ~ existing_arguments | map(attribute='type') | join(',') ~ ")", info=True) }}
        {% call statement('drop_procedure_alias[' ~ idx ~ ']') -%}
          DROP PROCEDURE IF EXISTS {{ database }}.{{ schema }}.{{ procedure_alias }}({{ existing_arguments | map(attribute='type') | join(',') }});
        {% endcall %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% call statement('main') -%}
    {{ dbt_snowflake_custom_materializations.snowflake_create_or_alter_procedure_statement(
        relation=target_relation,
        language=language,
        arguments=arguments,
        return_type=return_type,
        execute_as=execute_as,
        runtime_version=runtime_version,
        packages=packages,
        imports=imports,
        external_access_integrations=external_access_integrations,
        secrets=secrets,
        comment=comment,
        compiled_code=compiled_code
      ) }}
  {%- endcall %}

  {% if procedure_alias %}
    {%- set target_relation_alias = api.Relation.create( identifier=procedure_alias, schema=schema, database=database) -%}
    {% call statement('main_alias') -%}
      {{ dbt_snowflake_custom_materializations.snowflake_create_or_alter_procedure_statement(
          relation=target_relation_alias,
          language=language,
          arguments=arguments,
          return_type=return_type,
          execute_as=execute_as,
          runtime_version=runtime_version,
          packages=packages,
          imports=imports,
          external_access_integrations=external_access_integrations,
          secrets=secrets,
          comment=comment,
          compiled_code=compiled_code
        ) }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  -- return
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
