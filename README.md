# dbt-snowflake-materializations

This dbt package ships Snowflake-specific custom materializations so you can deploy database objects that are not covered by the core adapter. It currently supports three materializations: `alert`, `procedure`, and `udf`. Source: `macros/snowflake/materializations/alert/snowflake__alert.sql`, `macros/snowflake/materializations/procedure/snowflake__procedure.sql`, `macros/snowflake/materializations/udf/snowflake__udf.sql`

## Installation

1. Add this repository to your project's `packages.yml`.

   ```yaml
   packages:
     - git: "https://github.com/civitaspo/dbt-snowflake-materializations.git"
       revision: 0.0.1
   ```

2. Install dependencies.

   ```bash
   dbt deps
   ```

This package is tested with dbt versions >=1.9.0 and <2.0.0. Source: `dbt_project.yml`

## Supported materializations

- **alert** — Creates or replaces a Snowflake alert. `config(schedule=...)` and `config(action=...)` are mandatory. Optional `config(warehouse=...)` routes alert execution to a warehouse; if omitted, the alert is serverless. `config(suspend=true)` suspends the alert right after deployment. Source: `macros/snowflake/materializations/alert/snowflake__alert.sql`
- **procedure** — Builds SQL or Python stored procedures. Accepts `config(arguments=[{name, type, default?}, ...])`, `config(return_type='VARCHAR')` or `config(return_type_table=[...])`, and runtime controls such as `config(execute_as='caller'|'owner')`. Python procedures require `config(packages=['snowflake-snowpark-python', ...])` and a supported runtime (3.8–3.11). When the signature changes, the existing procedure is dropped and recreated. Source: `macros/snowflake/materializations/procedure/snowflake__procedure.sql`
- **udf** — Manages SQL or Python user-defined functions. Requires `config(arguments=[{name, type}, ...])` plus `config(return_type=...)` or `config(return_type_table=...)`. Python UDFs validate `runtime_version`, `packages`, `imports`, `external_access_integrations`, and `secrets`, raising compile errors if they break Snowflake rules. Common options include `config(secure=true)`, `config(immutable=true)`, and `config(function_alias='alias')`. Source: `macros/snowflake/materializations/udf/snowflake__udf.sql`

Both the procedure and UDF materializations support alias deployment via `config(procedure_alias=...)` and `config(function_alias=...)`, enabling multiple entry points for the same implementation. Source: `macros/snowflake/materializations/procedure/snowflake__procedure.sql`, `macros/snowflake/materializations/udf/snowflake__udf.sql`
