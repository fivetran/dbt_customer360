{%- macro name_suffixes(name) -%}

{%- set suffixes = [
    'Jr',
    'Junior',
    'Sr',
    'Senior',
    'i',
    'ii',
    'iii',
    'iv',
    'v',
    'vi',
    'vii',
    'viii',
    'ix',
    'x',
    'esq',
    'esquire'
] -%}

case
{% for suffix in suffixes %}
    when lower({{ name }}) like '% % {{ suffix|lower }}' then replace(replace(replace({{ name }}, ' {{ suffix }}', ''), ' {{ suffix|lower }}', ''), ' {{ suffix|upper }}', '')
    when lower({{ name }}) like '% %, {{ suffix|lower }}' or {{ name }} like '% %,{{ suffix }}' then replace({{ name }}, ',{{ suffix }}', '')
{%- endfor -%}
    else {{ name }} 
end as {{ name }}_no_suffix,

case
{% for suffix in suffixes %}
    when lower({{ name }}) like '% % {{ suffix|lower }}' or lower({{ name }}) like '% %,{{ suffix|lower }}' or lower({{ name }}) like '% %,{{ suffix|lower }}' then '{{ suffix }}'
{%- endfor -%}
    else null 
end as {{ name }}_suffix

{%- endmacro -%}