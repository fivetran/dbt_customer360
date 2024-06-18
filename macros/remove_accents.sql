{% macro remove_accents(s) %}

{%- set accent_mapping = {
    'à': 'a',
    'á': 'a',
    'â': 'a',
    'ä': 'a',
    'æ': 'a',
    'ã': 'a',
    'å': 'a',
    'ā': 'a',
    'è': 'e',
    'é': 'e',
    'ê': 'e',
    'ë': 'e',
    'ě': 'e',
    'ẽ': 'e',
    'ē': 'e',
    'ė': 'e',
    'ę': 'e',
    'ì': 'i',
    'í': 'i',
    'î': 'i',
    'ï': 'i',
    'ǐ': 'i',
    'ĩ': 'i',
    'ī': 'i',
    'ı': 'i',
    'į': 'i',
    'ò': 'o',
    'ó': 'o',
    'ô': 'o',
    'ö': 'o',
    'ǒ': 'o',
    'œ': 'o',
    'ø': 'o',
    'õ': 'o',
    'ō': 'o',
    'ù': 'u',
    'ú': 'u',
    'û': 'u',
    'ü': 'u',
    'ǔ': 'u',
    'ũ': 'u',
    'ū': 'u',
    'ű': 'u',
    'ů': 'u',
    'ñ': 'n',
    'ń': 'n',
    'ņ': 'n',
    'ň': 'n',
    'ç': 'c',
    'ć': 'c',
    'č': 'c',
    'ċ': 'c',
    'ź': 'z',
    'ž': 'z',
    'ż': 'z',
    'ß': 's',
    'ş': 's',
    'ș': 's',
    'ś': 's',
    'š': 's',
    'ď': 'd',
    'ð': 'd',
    'ğ': 'g',
    'ġ': 'g',
    'ħ': 'h',
    'ķ': 'k',
    'ł': 'l',
    'ļ': 'l',
    'ľ': 'l',
    'ŵ': 'w',
    'ř': 'r',
    'ț': 't',
    'ť': 't',
    'þ': 't',
    'ý': 'y',
    'ŷ': 'y',
    'ÿ': 'y'
} -%}

{% set replace_call = '' %}
{% for accent, base in accent_mapping.items() %}
{% if loop.first %}
    {% set replace_call = "replace(replace({{ s }}, '{{ accent }}', '{{ base }}'), '{{ accent|upper }}', '{{ base|upper }}')" %}
{% else %}
    {% set replace_call = "replace(replace(" ~ replace_call ~ ", {{ accent }}, '{{ base}}'), '{{ accent|upper }}', '{{ base|upper }}')" %}
{% endif %}
{%- endfor -%}

{{ return(replace_call) }}


{% endmacro %}