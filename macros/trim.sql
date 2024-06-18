{% macro trim(str, chars_to_trim, direction='both')%}

{{ return(adapter.dispatch('trim', 'customer360')(str, chars_to_trim, direction)) }}

{% endmacro %}

{% macro default__trim(str, chars_to_trim, direction) %}
    {% if direction|lower == 'both' -%}
        trim( {{ str }} , '{{ chars_to_trim }}')
    {%- elif direction|lower == 'leading' -%}
        ltrim( {{ str }} , '{{ chars_to_trim }}')
    {%- elif direction|lower == 'trailing' -%}
        rtrim( {{ str }} , '{{ chars_to_trim }}')
    {%- endif -%}
{% endmacro %}

{% macro redshift__trim(str, chars_to_trim, direction) %}
    trim({{ direction}} '{{ chars_to_trim }}' from {{ str }})
{% endmacro %}

{% macro spark__trim(str, chars_to_trim, direction) %}
    trim({{ direction}} '{{ chars_to_trim }}' from {{ str }})
{% endmacro %}