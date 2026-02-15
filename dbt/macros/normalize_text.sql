{% macro normalize_text(column_name) %}
    upper(regexp_replace(trim({{ column_name }}), '\s+', ' ', 'g'))
{% endmacro %}
