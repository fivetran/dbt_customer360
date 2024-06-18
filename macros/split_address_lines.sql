{%- macro split_address_lines(address) -%}

-- Secondry unit types: do they require a unit number
{%- set secondary_units = {
    'Apartment': True, 'APT': True,
    'Basement': False, 'BSMT': False,
    'Building': True, 'BLDG': True,
    'Department': True, 'DEPT': True,
    'Floor': True, 'FL': True,
    'Front': False, 'FRNT': False,
    'Hanger': True, 'HNGR': True,
    'Key': True,
    'Lobby': False, 'LBBY': False,
    'Lot': True,	
    'Lower': False, 'LOWR': False,
    'Office': False, 'OFC': False,
    'Penthouse': False, 'PH': False,
    'Pier': True,
    'Rear': False,
    'Room': True, 'RM': True,
    'Side': False,
    'Slip': True,	
    'Space': True, 'SPC': True,
    'Stop': True,
    'Suite': True, 'STE': True,
    'Trailer': True, 'TRLR': True,
    'Unit': True, 	
    'Upper': False, 'UPPR': False,
    '#': True
}
%}

case
    when {{ address }} like '%, %' then {{ dbt.split_part(address, "', '", 1) }}
    when {{ address }} like '%,%' then {{ dbt.split_part(address, "','", 1) }}
{% for unit_type, needs_unit_number in secondary_units.items() %}
    when {{ address }} like '% {{ unit_type ~ " %" if needs_unit_number else '' }}'
        then {{ dbt.split_part(address, "'" ~ unit_type ~ "'", 1) }} 
    when {{ address }} like lower('% {{ unit_type ~ " %" if needs_unit_number else '' }}') 
        then {{ dbt.split_part(address, "'" ~ unit_type|lower ~ "'", 1) }} 
    when {{ address }} like upper('% {{ unit_type ~ " %" if needs_unit_number else '' }}') 
        then {{ dbt.split_part(address, "'" ~ unit_type|upper ~ "'", 1) }} 
    when {{ address }}  like '% {{ unit_type ~ "." ~ " %" if needs_unit_number else '' }}'
        then {{ dbt.split_part(address, "'" ~ unit_type ~ ".'", 1) }} 
    when {{ address }} like lower('% {{ unit_type ~ "." ~ " %" if needs_unit_number else '' }}') 
        then {{ dbt.split_part(address, "'" ~ unit_type|lower ~ ".'", 1) }} 
    when {{ address }} like upper('% {{ unit_type ~ "." ~ " %" if needs_unit_number else '' }}') 
        then {{ dbt.split_part(address, "'" ~ unit_type|upper ~ ".'", 1) }} 
{%- endfor -%}
    else {{ address }}
end as {{ address }}_line_1,

case
    when {{ address }} like '%, %' then {{ dbt.split_part(address, "', '", 2) }}
    when {{ address }} like '%,%' then {{ dbt.split_part(address, "','", 2) }}
{% for unit_type, needs_unit_number in secondary_units.items() %}
    when lower( {{ address }} ) like lower('% {{ unit_type ~ " %" if needs_unit_number else '' }}') 
        then '{{ unit_type }}' || {{ dbt.split_part(address, "'" ~ unit_type ~ "'", 2) }} 
    when lower( {{ address }} ) like lower('% {{ unit_type ~ "." ~ " %" if needs_unit_number else '' }}') 
        then '{{ unit_type }}' || {{ dbt.split_part(address, "'" ~ unit_type ~ ".'", 2) }} 
{%- endfor -%}
end as {{ address }}_line_2

{%- endmacro -%}