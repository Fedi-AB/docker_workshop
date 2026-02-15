{% macro generate_surrogate_key(columns) %}
    -- Simple surrogate key using MD5 concat of the columns
    md5(
        concat(
            {% for col in columns %}
                {{ col }}{% if not loop.last %} || '|' || {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
