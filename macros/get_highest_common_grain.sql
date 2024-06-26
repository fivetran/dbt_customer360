{%- macro get_highest_common_grain() %}

{%- set sources = ['zendesk', 'marketo', 'stripe'] -%}

{% for source in sources %}
    
    {% if var('customer360_grain_' ~ source, 'individual') == 'organization' %}
    {{ return('organization') }}
    {% endif %}

{% endfor %}

{{ return('individual') }}

{%- endmacro %}