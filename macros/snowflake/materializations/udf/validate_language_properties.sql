  {% macro validate_runtime_version(language_info, runtime_version) %}
    {% if language_info.supports_runtime_version %}
      {% if not runtime_version or runtime_version is not string %}
        {{ exceptions.raise_compiler_error("'runtime_version' must be specified for " ~ language_info.name ~ " functions.") }}
      {% elif runtime_version not in language_info.supported_runtime_versions %}
        {{ exceptions.raise_compiler_error("'runtime_version' must be one of the following for " ~ language_info.name ~ ": " ~ language_info.supported_runtime_versions | join(', ') ~ ".") }}
      {% endif %}
    {% else %}
      {% if runtime_version %}
        {{ exceptions.raise_compiler_error("'runtime_version' is not supported for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% endif %}
  {% endmacro %}

  {% macro validate_packages(language_info, packages) %}
    {% if language_info.supports_packages %}
      {% if packages is not sequence %}
        {{ exceptions.raise_compiler_error("'packages' must be a list of paths for " ~ language_info.name ~ " functions. e.g. ['snowflake-snowpark-python']") }}
      {% endif %}
    {% else %}
      {% if packages %}
        {{ exceptions.raise_compiler_error("'packages' is not supported for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% endif %}
  {% endmacro %}

  {% macro validate_imports(language_info, imports) %}
    {% if language_info.supports_imports %}
      {% if imports is not sequence %}
        {{ exceptions.raise_compiler_error("'imports' must be a list of paths for " ~ language_info.name ~ " functions. e.g. ['snowflake-snowpark-python']") }}
      {% endif %}
    {% else %}
      {% if imports %}
        {{ exceptions.raise_compiler_error("'imports' is not supported for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% endif %}
  {% endmacro %}

  {% macro validate_external_access_integrations(language_info, external_access_integrations) %}
    {% if language_info.supports_external_access_integrations %}
      {% if external_access_integrations is not sequence %}
        {{ exceptions.raise_compiler_error("'external_access_integrations' must be list of string for " ~ language_info.name ~ " functions. e.g. ['integration_name']") }}
      {% endif %}
    {% else %}
      {% if external_access_integrations %}
        {{ exceptions.raise_compiler_error("'external_access_integrations' is not supported for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% endif %}
  {% endmacro %}

  {% macro validate_secrets(language_info, secrets) %}
    {% if language_info.supports_secrets %}
      {% if secrets is not mapping %}
        {{ exceptions.raise_compiler_error("'secrets' must be a mapping for " ~ language_info.name ~ " functions. e.g. {'secret_identifier': 'reference_to_secret'}") }}
      {% endif %}
    {% else %}
      {% if secrets %}
        {{ exceptions.raise_compiler_error("'secrets' is not supported for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% endif %}
  {% endmacro %}

  {% macro validate_memoizable(language_info, memoizable) %}
    {% if language_info.supports_memoizable %}
      {% if memoizable is not sameas true and memoizable is not sameas false %}
        {{ exceptions.raise_compiler_error("'memoizable' must be a boolean for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% else %}
      {% if memoizable %}
        {{ exceptions.raise_compiler_error("'memoizable' is not supported for " ~ language_info.name ~ " functions.") }}
      {% endif %}
    {% endif %}
  {% endmacro %}
