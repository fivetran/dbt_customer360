{%- macro org_suffixes(org) -%}

{%- set suffixes = [
    'Agency',
    'And',
    'Assn',
    'Assoc',
    'Associates',
    'Association',
    'Bank',
    'Bv',
    'Co',
    'Comp',
    'Org',
    'Corp',
    'Corporation',
    'Dmd',
    'Enterprises',
    'Gmbh',
    'Group',
    'Hotel',
    'Hotels',
    'Inc',
    'Incorporated',
    'International',
    'Intl',
    'Limited',
    'Llc',
    'Llp',
    'Lp',
    'Ltd',
    'Manufacturing',
    'Mfg',
    'Pa',
    'Pc',
    'Pharmacy',
    'Plc',
    'Pllc',
    'Restaurant',
    'Sa',
    'Sales',
    'Service',
    'Services',
    'Store',
    'Svcs',
    'Travel',
    'Unlimited',
    'Ultd',
    'Unltd'
] -%}

case
{% for suffix in suffixes %}
    when lower({{ org }}) like '% {{ suffix|lower }}' then replace(replace(replace({{ org }}, ' {{ suffix }}', ''), ' {{ suffix|upper }}', ''), ' {{ suffix|lower }}', '')
    when lower({{ org }}) like '%_{{ suffix|lower }}' then replace(replace(replace({{ org }}, '_{{ suffix }}', ''), '_{{ suffix|upper }}', ''), '_{{ suffix|lower }}', '')
    when lower({{ org }}) like '% {{ suffix|lower }}.' then replace(replace(replace({{ org }}, ' {{ suffix }}.', ''), ' {{ suffix|upper }}.', ''), ' {{ suffix|lower }}.', '')
    when lower({{ org }}) like '%,{{ suffix|lower }}' then replace(replace(replace({{ org }}, ',{{ suffix }}', ''), ',{{ suffix|upper }}', ''), ',{{ suffix|lower }}', '')
    when lower({{ org }}) like '%, {{ suffix|lower }}' then replace(replace(replace({{ org }}, ', {{ suffix }}', ''), ', {{ suffix|upper }}', ''), ', {{ suffix|lower }}', '')
{%- endfor -%}
    else {{ org }} 
end as {{ org }}_no_suffix,

case
{% for suffix in suffixes %}
    when lower({{ org }}) like '% {{ suffix }}' 
        or lower({{ org }}) like '%_{{ suffix }}' 
        or lower({{ org }}) like '% {{ suffix }}.' 
        or lower({{ org }}) like '%,{{ suffix }}' 
        or lower({{ org }}) like '%, {{ suffix }}' 
    then '{{ suffix }}'
{%- endfor -%}
    else null 
end as {{ org }}_suffix

{%- endmacro -%}