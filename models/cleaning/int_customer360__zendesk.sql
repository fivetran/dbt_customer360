{{ config(enabled=var('customer360__using_zendesk', true)) }}

{%- set match_id_list = [] -%}

with users as (

    select stg_zendesk__user.*

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.zendesk and match_set.zendesk.source|lower == 'user' %}
                {%- if match_set.zendesk.map_table %}
                , map.{{ match_set.zendesk.match_key }}
                {%- else %}
                , {{ match_set.zendesk.match_key }}
                {%- endif %}
            {%- else %}
                , null 
            {%- endif %} as {{ match_set.name }}
            {% do match_id_list.append(match_set.name) -%}
        {%- endfor %}
    {% endif %}

    from {{ ref('stg_zendesk__user') }}

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.zendesk and match_set.zendesk.source|lower == 'user'%}
                {%- if match_set.zendesk.map_table %}

    left join {{ fivetran_utils.wrap_in_quotes(match_set.zendesk.map_table) }} as map 
        on map.{{ match_set.zendesk.map_table_join_on }} = stg_zendesk__user.{{ match_set.zendesk.join_with_map_on }}

                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}

    where stg_zendesk__user.role = 'end-user'
),

orgs as (

    select stg_zendesk__organization.*

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.zendesk and match_set.zendesk.source|lower == 'organization' %}
                {%- if match_set.zendesk.map_table %}
                , map.{{ match_set.zendesk.match_key }}
                {%- else %}
                , {{ match_set.zendesk.match_key }}
                {%- endif %} as {{ match_set.name }}
            {%- elif match_set.zendesk.source|lower != 'user' %}
                , null as {{ match_set.name }}
            {%- endif %}
            {%- if match_set.name not in match_id_list -%}
                {% do match_id_list.append(match_set.name) -%}
            {%- endif %}
        {%- endfor %}
    {% endif %}

    from {{ ref('stg_zendesk__organization') }}

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.zendesk and match_set.zendesk.source|lower == 'organization'%}
                {%- if match_set.zendesk.map_table %}

    left join {{ fivetran_utils.wrap_in_quotes(match_set.zendesk.map_table) }} as map 
        on map.{{ match_set.zendesk.map_table_join_on }} = stg_zendesk__organization.{{ match_set.zendesk.join_with_map_on }}

                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}
),

standardize as (

    select
        user_id,
        users.organization_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        lower(email) as email,

        -- full  name
        users.name as full_name,

        -- organization name
        orgs.name as organization_name,

        -- phone
        -- remove non-alphanumeric characters and standardize extension format 
        replace(lower(REGEXP_REPLACE(replace(phone, '#', 'ext'), r'[^a-zA-Z0-9]', '')), 'extension', 'ext') as phone,

        -- no address data

        -- no ip address

        -- status
        is_active,
        not coalesce(is_active, true) as is_deleted,
        is_suspended,

        -- timestamps
        users.updated_at as updated_at,
        users.created_at as created_at,
        orgs.updated_at as organization_updated_at,
        orgs.created_at as organization_created_at

    from users
    left join orgs 
        on users.organization_id = orgs.organization_id
),

tokenize as (

    select 
        user_id,
        organization_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        {{ dbt.split_part('email', "'@'", 1) }} as email_address,
        {{ dbt.split_part('email', "'@'", 2) }} as email_domain,
        
        -- full name
        full_name,
        {{ customer360.name_titles('full_name') }},

        -- organization name
        organization_name,
        {{ customer360.org_suffixes('organization_name') }},

        -- phone
        {{ dbt.split_part('phone', "'ext'", 1) }} as phone_number,
        {{ dbt.split_part('phone', "'ext'", 2) }} as phone_extension,

        -- no address

        -- no ip address

        -- status
        is_active,
        is_deleted,
        is_suspended,

        -- timestamps
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at

    from standardize
),

restandardize as (

    select 
        user_id,
        organization_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        -- remove non alpha-numeric characters for matching
        REGEXP_REPLACE(email_address, r'[^a-zA-Z0-9]', '') as email_address_stripped,
        email_domain,
        
        -- full name
        full_name,
        full_name_no_title,
        full_name_title,
        {{ customer360.name_suffixes('full_name_no_title') }},

        -- company name
        organization_name,
        organization_name_no_suffix,
        organization_name_suffix,

        -- phone
        case when phone_number is null or phone_number = '' or phone_number = 'NA' then null else  
            '+' || case when length(phone_number) = 10 then '1' else '' end || REGEXP_REPLACE(phone_number, '[^0-9]', '') 
        end as phone,
        REGEXP_REPLACE(phone_extension, r'[^a-zA-Z0-9]', '') as phone_extension,

        -- no address data

        -- no ip address

        -- status
        is_active,
        is_deleted,
        is_suspended,

        -- timestamps
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at

    from tokenize 
),

split_first_name as (

    select 
        user_id,
        organization_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        email_address_stripped,
        email_domain,
        
        -- full name
        full_name,
        full_name_no_title_no_suffix as full_name_clean,
        full_name_no_title_suffix as name_suffix,
        full_name_title as name_title,
        case 
            when full_name_no_title_no_suffix like '%, %' then {{ dbt.split_part('full_name_no_title_no_suffix', "', '", 2) }}
            else {{ dbt.split_part('full_name_no_title_no_suffix', "' '", 1) }} 
        end as first_name,
        case 
            when full_name_no_title_no_suffix like '%, %' then {{ dbt.split_part('full_name_no_title_no_suffix', "', '", 1) }}
            else {{ dbt.split_part('full_name_no_title_no_suffix', "' '", 2) }} 
        end as last_name,
        -- company name
        organization_name,
        organization_name_no_suffix,
        organization_name_suffix,

        -- phone
        phone,
        phone_extension,

        -- no address

        -- no ip address

        -- status
        is_active,
        is_deleted,
        is_suspended,

        -- timestamps
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at

    from restandardize
),

add_nicknames as (

    select 
        user_id,
        organization_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        email_address_stripped,
        email_domain,
        
        -- full name
        full_name,
        coalesce(first_name, '') || case when first_name is not null and last_name is not null then ' ' else '' end || coalesce(last_name, '') as full_name_clean,
        name_suffix,
        name_title,
        
        -- company name
        organization_name,
        organization_name_no_suffix,
        organization_name_suffix,

        -- phone
        phone,
        phone_extension,

        -- no address

        -- no ip address

        -- status
        is_active,
        is_deleted,
        is_suspended,

        -- timestamps
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at,

        {{ fivetran_utils.string_agg(field_to_agg="distinct nicknames.canonical_name", delimiter="', '") }} as possible_alt_first_names

    from split_first_name
    left join {{ ref('customer360__nicknames') }} as nicknames 
        on lower(split_first_name.first_name) = nicknames.nickname 

    {{ dbt_utils.group_by(n=21 + match_id_list|length) }}
),

final as (

    select 
        user_id,
        organization_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        case when email = '' then null else email end as email,
        case when email_address_stripped = '' then null else email_address_stripped end as email_address_stripped,
        case when email_domain = '' then null else email_domain end as email_domain,
        
        -- full name
        case when full_name = '' then null else full_name end as full_name,
        case when full_name_clean = '' then null else full_name_clean end as full_name_clean,
        case when name_title = '' then null else name_title end as name_title,
        case when name_suffix = '' then null else name_suffix end as name_suffix,
        
        -- company name
        case when organization_name = '' then null else organization_name end as organization_name,
        case when organization_name_no_suffix = '' then null else organization_name_no_suffix end as organization_name_no_suffix,
        case when organization_name_suffix = '' then null else organization_name_suffix end as organization_name_suffix,

        -- phone
        case when phone = '' then null else phone end as phone,
        case when phone_extension = '' then null else phone_extension end as phone_extension,

        -- no address

        -- no ip address

        -- status
        is_active,
        is_deleted,
        is_suspended,

        -- timestamps
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at,

        possible_alt_first_names

    from add_nicknames
)

select *
from final