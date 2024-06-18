{% macro levenshtein_distance(str1, str2) %}

{{ return(adapter.dispatch('levenshtein_distance', 'customer360')(str1, str2)) }}

{% endmacro %}

{% macro default__levenshtein_distance(str1, str2) %}
    -- For non-supported warehouses
    {{ exceptions.raise_compiler_error("The Customer360 data model (and therefore the levenshtein macro) is not yet implemented for this adapter.") }}
{% endmacro %}

{% macro snowflake__levenshtein_distance(str1, str2) %}
    -- Built-in support for Snowflake
    1.0 - EDITDISTANCE({{ str1 }}, {{ str2 }}) / greatest(length({{ str1 }}), length({{ str2 }}))
{% endmacro %}

{% macro bigquery__levenshtein_distance(str1, str2) %}
    -- Built-in for BigQuery
    1.0 - EDIT_DISTANCE({{ str1 }}, {{ str2 }}) / greatest(length({{ str1 }}), length({{ str2 }}))
{% endmacro %}

{% macro postgres__levenshtein_distance(str1, str2) %}
    -- Built-in for Postgres
    1.0 - levenshtein({{ str1 }}, {{ str2 }}) / greatest(length({{ str1 }}), length({{ str2 }}))
{% endmacro %}

{% macro spark__levenshtein_distance(str1, str2) %}
    -- Built-in for Databricks
    1.0 - levenshtein({{ str1 }}, {{ str2 }}) / greatest(length({{ str1 }}), length({{ str2 }}))
{% endmacro %}

{% macro redshift__levenshtein_distance(str1, str2) %}
-- use UDF we created for Redshift below
fivetran_customer360_levenshtein_distance({{ str1, str2}} )
{% endmacro %}

{% macro create_levenshtein_udf() %}
-- executed in on-run-start
-- only necessary for redshift, as the other WHs have native distance functions

-- taken from https://github.com/aws-samples/amazon-redshift-udfs/blob/master/python-udfs/f_fuzzy_string_match(varchar%2Cvarchar)/function.sql
CREATE OR REPLACE FUNCTION fivetran_customer360_levenshtein_distance (str1 VARCHAR, str2 VARCHAR) 
RETURNS FLOAT IMMUTABLE AS $$
    from thefuzz import fuzz 

    return fuzz.ratio (str1, str2) / 100.0
$$ LANGUAGE plpythonu;

{# CREATE OR REPLACE FUNCTION fivetran_customer360_levenshtein_distance(str1 VARCHAR(greatest), str2 VARCHAR(greatest))
RETURNS INT
IMMUTABLE
AS $$
    if length({{ str1 }}) < length({{ str2 }}):
        return levenshtein_distance(str2, str1)

    if length({{ str2 }}) == 0:
        return length({{ str1 }})

    previous_row = range(length({{ str2 }}) + 1)
    for i, c1 in enumerate(str1):
        current_row = [i + 1]
        for j, c2 in enumerate(str2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]
$$ LANGUAGE plpythonu; #}

{% endmacro %}

{# Let's consider letters with and without accents to be the same in terms of distance #}
{# {%- set accent_mapping = {
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

{% set m = str1 | length %}
{% set n = str2 | length %}

{% set d = [ range(0, m) ] %}
{% for j in range(1, n+1) %}
    {% set d = d + [[ j ]] %}
{% endfor %}

{% for j in range(1, n+1) %}
    {% for i in range(1, m+1) %}
        {% set cost = 0 if (str1[i-1] == str2[j-1] or accent_mapping.get(str1[i-1])==str2[j-1] or str1[i-1]==accent_mapping.get(str2[j-1])) else 1 %}
        {% set d = d[:i] + [(min(d[i-1]+1, d[i]+1, d[-m+i-1]+cost))] + d[i+1:] %}
    {% endfor %}
    {% set d = d[-m:] + d[m:] %}
{% endfor %}

{{ d[-1][-1] }}
{% endmacro %} #}