{%- macro get_lowest_grain() %}

{%- set sources = ['zendesk', 'marketo', 'stripe'] -%}

{% for source in sources %}
    
    {% if var('customer360_grain_' ~ source, 'individual') == 'individual' %}
    {{ return('individual') }}
    {% endif %}

{% endfor %}

{{ return('organization') }}

{%- endmacro %}