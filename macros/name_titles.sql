{%- macro name_titles(name) -%}

{%- set titles = [
    'Mr',
    'Mrs',
    'Master',
    'Mister',
    'Miss',
    'Ms',
    'Mx',
    'Dr',
    'Doctor',
    'Admiral',
    'Air Comm',
    'Ambassador',
    'Baron',
    'Baroness',
    'Brig Gen',
    'Brig. Gen',
    'Brigadier',
    'Brother',
    'Canon',
    'Capt',
    'Chief',
    'Cllr',
    'Col',
    'Commander',
    'Consul',
    'Consul General',
    'Count',
    'Countess',
    'Countess of',
    'Cpl',
    'Dame',
    'Deputy',
    'Drs',
    'Duchess',
    'Duke',
    'Earl',
    'Father',
    'General',
    'Gräfin',
    'HE',
    'HMA',
    'Her Grace',
    'His Excellency',
    'Ing',
    'Judge',
    'Justice',
    'Lady',
    'Lic',
    'Llc',
    'Lord',
    'Lord & Lady',
    'Lt',
    'Lt Col',
    'Lt Cpl',
    'Lt. Col',
    'Lt. Cpl',
    'M',
    'Madam',
    'Madame',
    'Major',
    'Major General',
    'Marchioness',
    'Marquis',
    'Minister',
    'Mme',
    'Prince',
    'Princess',
    'Professor',
    'Prof',
    'Prof Dame',
    'Prof Dr',
    'Prof. Dame',
    'Prof. Dr',
    'Pvt',
    'Rabbi',
    'Rear Admiral',
    'Rev',
    'Rev Canon',
    'Rev Dr',
    'Rev. Canon',
    'Rev. Dr',
    'Senator',
    'Sgt',
    'Sheriff',
    'Sir',
    'Sister',
    'Sqr Leader',
    'Sqr. Leader',
    'The Earl of',
    'The Hon',
    'The Hon Dr',
    'The Hon Lady',
    'The Hon Lord',
    'The Hon Mrs',
    'The Hon Sir',
    'The Hon. Dr',
    'The Hon. Lady',
    'The Hon. Lord',
    'The Hon. Mrs',
    'The Hon. Sir',
    'The Honourable',
    'The Rt Hon',
    'The Rt Hon Dr',
    'The Rt Hon Lord',
    'The Rt Hon Sir',
    'The Rt Hon Visc',
    'The Rt. Hon',
    'The Rt. Hon. Dr',
    'The Rt. Hon. Lord',
    'The Rt. Hon. Sir',
    'The Rt. Hon. Visc',
    'Viscount'
] -%}

case
{% for title in titles %}
    when {{ name }} like '{{ title }} % %' then replace({{ name }}, '{{ title }} ', '')
    when {{ name }} like '{{ title }}. % %' then replace({{ name }}, '{{ title }}. ', '')
{%- endfor -%}
    else {{ name }} 
end as {{ name }}_no_title,

case
{% for title in titles %}
    when {{ name }} like '{{ title }} % %' or {{ name }} like '{{ title }}. % %' then '{{ title }}'
{%- endfor -%}
    else null 
end as {{ name }}_title

{%- endmacro -%}