-----------------------------------------------------------------------------------------------
-- TRY_CAST or SAFE_CAST is used instead of CAST to safely convert values to INTEGER.
-- If a value cannot be converted (for example due to unexpected strings or malformed data),
-- TRY_CAST or SAFE_CAST returns NULL instead of raising a runtime error.
-- This ensures that the transformation process remains robust and does not fail
-- because of data quality issues in the raw source table.
-----------------------------------------------------------------------------------------------

{% macro safe_cast(column, type) %}
    -- Defines a reusable dbt macro named "safe_cast".
    -- It takes two parameters:
    --   column → the column name to cast
    --   type   → the target data type (e.g., INTEGER, TIMESTAMP, etc.)

    {% if target.type == 'bigquery' %}
        -- Checks if the current dbt target (warehouse defined in profiles.yml)
        -- is BigQuery. The "target" object is automatically available in dbt.

        SAFE_CAST({{ column }} AS {{ type }})
        -- Uses BigQuery's SAFE_CAST function.
        -- SAFE_CAST returns NULL instead of throwing an error
        -- if the conversion fails (e.g., invalid numeric value).

    {% elif target.type == 'duckdb' %}
        -- If the target warehouse is DuckDB.
        -- DuckDB does not have a SAFE_CAST function, but it has TRY_CAST which serves a similar purpose.

        TRY_CAST({{ column }} AS {{ type }})
        -- Uses DuckDB's TRY_CAST function.
        -- TRY_CAST behaves similarly to SAFE_CAST:
        -- it returns NULL instead of failing when casting is invalid.

    {% endif %}
    -- Ends the conditional logic block.
    -- Only the SQL corresponding to the active target will be compiled.

{% endmacro %}
-- Ends the macro definition.
-- The macro can now be reused in any model via:
-- {{ safe_cast('column_name', 'INTEGER') }}