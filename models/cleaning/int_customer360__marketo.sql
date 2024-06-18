{{ config(enabled=var('customer360__using_marketo', true)) }}

{%- set match_id_list = [] -%}

with lead as (

    select marketo__leads.*

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.marketo %}
                {%- if match_set.marketo.map_table %}
                , map.{{ match_set.marketo.match_key }}
                {%- else %}
                , {{ match_set.marketo.match_key }}
                {%- endif %}
            {%- else %}
                , null
            {%- endif %} as {{ match_set.name }}
            {% do match_id_list.append(match_set.name) -%}
        {%- endfor %}
    {% endif %}

    from {{ ref('marketo__leads') }}

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.marketo %}
                {%- if match_set.marketo.map_table %}

    left join {{ fivetran_utils.wrap_in_quotes(match_set.marketo.map_table) }} as map 
        on map.{{ match_set.marketo.map_table_join_on }} = marketo__leads.{{ match_set.marketo.join_with_map_on }}

                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}
),

clean_state_and_country as (

    select 
        *,
        -- sometimes the state_code is the code, sometimes the longform name
        -- most state codes are 2-characters, but a handful or international states are 3-char
        case 
            when coalesce(state_code, '#REF!') != '#REF!' and length(state_code) <= 3 then upper(state_code)
            when coalesce(state, '#REF!') != '#REF!' and length(state) <= 3 then upper(state)
            else null
        end as true_state_code,
        case 
            when coalesce(state, '#REF!') != '#REF!' and length(state) > 3 then state
            when coalesce(state_code, '#REF!') != '#REF!' and length(state_code) > 3 then state_code
            else null
        end as true_state_long,
        case 
            when coalesce(country_code, '#REF!') != '#REF!' and length(country_code) = 2 then upper(country_code)
            when coalesce(country, '#REF!') != '#REF!' and length(country) = 2 then upper(country)
            else null
        end as true_country_code,
        case 
            when coalesce(country, '#REF!') != '#REF!' and length(country) > 2 then country
            when coalesce(country_code, '#REF!') != '#REF!' and length(country_code) > 2 then country_code
            else null
        end as true_country_long,

        case 
            when coalesce(billing_state_code, '#REF!') != '#REF!' and length(billing_state_code) <= 3 then upper(billing_state_code)
            when coalesce(billing_state, '#REF!') != '#REF!' and length(billing_state) <= 3 then upper(state)
            else null
        end as true_billing_state_code,
        case 
            when coalesce(billing_state, '#REF!') != '#REF!' and length(billing_state) > 3 then billing_state
            when coalesce(billing_state_code, '#REF!') != '#REF!' and length(billing_state_code) > 3 then billing_state_code
            else null
        end as true_billing_state_long,
        case 
            when coalesce(billing_country_code, '#REF!') != '#REF!' and length(billing_country_code) = 2 then upper(billing_country_code)
            when coalesce(billing_country, '#REF!') != '#REF!' and length(billing_country) = 2 then upper(billing_country)
            else null
        end as true_billing_country_code,
        case 
            when coalesce(billing_country, '#REF!') != '#REF!' and length(billing_country) > 2 then billing_country
            when coalesce(billing_country_code, '#REF!') != '#REF!' and length(billing_country_code) > 2 then billing_country_code
            else null
        end as true_billing_country_long,

        case 
            when coalesce(inferred_state_region, '#REF!') != '#REF!' and length(inferred_state_region) <= 3 then upper(inferred_state_region)
            else null
        end as true_inferred_state_code,
        case 
            when coalesce(inferred_state_region, '#REF!') != '#REF!' and length(inferred_state_region) > 3 then inferred_state_region
            else null
        end as true_inferred_state_long,
        case 
            when coalesce(inferred_country, '#REF!') != '#REF!' and length(inferred_country) = 2 then upper(inferred_country)
            else null
        end as true_inferred_country_code,
        case 
            when coalesce(inferred_country, '#REF!') != '#REF!' and length(inferred_country) > 2 then inferred_country
            else null
        end as true_inferred_country_long

    from lead
),

standardize as (

    select
        lead_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        lower(email) as email,

        -- full  name
        first_name,
        last_name,

        -- organization name
        company as organization_name,
        inferred_company as inferred_organization_name,

        -- phone
        -- remove non-alphanumeric characters and standardize extension format 
        replace(lower(REGEXP_REPLACE(replace(phone, '#', 'ext'), r'[^a-zA-Z0-9]', '')), 'extension', 'ext') as phone,
        replace(lower(REGEXP_REPLACE(replace(main_phone, '#', 'ext'), r'[^a-zA-Z0-9]', '')), 'extension', 'ext') as company_phone,
        replace(lower(REGEXP_REPLACE(replace(mobile_phone, '#', 'ext'), r'[^a-zA-Z0-9]', '')), 'extension', 'ext') as mobile_phone,

        -- address
        coalesce(address, address_lead) as address,
        city,
        -- sometimes marketo.lead.state_code is actually the longform state name and not a code
        coalesce(true_state_code, lead_state_codes.state_code) as state_code,
        coalesce(true_state_long, lead_state_codes.state_territory) as state_long,
        coalesce(true_country_code, lead_country_codes.country_code_alpha_2) as country_code,
        coalesce(true_country_long, lead_country_codes.country_name) as country_long,
        lead_country_codes.alternative_country_name as country_long_alt,
        REGEXP_REPLACE(cast(postal_code as {{ dbt.type_string() }}), '[^0-9]', '') as postal_code,

        billing_street as billing_address,
        billing_city,
        coalesce(true_billing_state_code, billing_state_codes.state_code) as billing_state_code,
        coalesce(true_billing_state_long, billing_state_codes.state_territory) as billing_state_long,
        coalesce(true_billing_country_code, billing_country_codes.country_code_alpha_2)  as billing_country_code,
        coalesce(true_billing_country_long, billing_country_codes.country_name) as billing_country_long,
        billing_country_codes.alternative_country_name as billing_country_long_alt,
        REGEXP_REPLACE(cast(billing_postal_code as {{ dbt.type_string() }}), '[^0-9]', '') as billing_postal_code,

        inferred_city,
        coalesce(true_inferred_state_code, inferred_state_codes.state_code) as inferred_state_code,
        coalesce(true_inferred_state_long, inferred_state_codes.state_territory) as inferred_state_long,
        coalesce(true_inferred_country_code, inferred_country_codes.country_code_alpha_2) as inferred_country_code,
        coalesce(true_inferred_country_long, inferred_country_codes.country_name) as inferred_country_long,
        inferred_country_codes.alternative_country_name as inferred_country_long_alt,
        REGEXP_REPLACE(cast(inferred_postal_code as {{ dbt.type_string() }}), '[^0-9]', '') as inferred_postal_code,

        -- ip address
        anonymous_ip as ip_address,

        -- status
        is_unsubscribed,
        is_email_invalid,
        do_not_call,

        -- timestamps
        updated_timestamp as updated_at,
        created_timestamp as created_at

    from clean_state_and_country

    -- grab countries first since we need them to join in states
    ---- also grab alternative country names
    left join {{ ref('customer360__country_codes') }} as lead_country_codes
        on (clean_state_and_country.true_country_code = lead_country_codes.country_code_alpha_2 
            and clean_state_and_country.true_country_long is null) -- sometimes country_code completely != country_long 
        or clean_state_and_country.true_country_long = lead_country_codes.country_name
        or clean_state_and_country.true_country_long = lead_country_codes.alternative_country_name
    left join {{ ref('customer360__country_codes') }} as billing_country_codes
        on (clean_state_and_country.true_billing_country_code = billing_country_codes.country_code_alpha_2
            and clean_state_and_country.true_billing_country_long is null) -- sometimes country_code completely != country_long 
        or clean_state_and_country.true_billing_country_long = billing_country_codes.country_name
        or clean_state_and_country.true_billing_country_long = billing_country_codes.alternative_country_name

    -- grab as many state names as we can
    left join {{ ref('customer360__state_territory_codes') }} as lead_state_codes
        on (clean_state_and_country.true_state_code = lead_state_codes.state_code
            or clean_state_and_country.true_state_long = lead_state_codes.state_territory)
        and coalesce(lead_country_codes.country_code_alpha_2, clean_state_and_country.true_country_code) = lead_state_codes.alpha2_country_code
    left join {{ ref('customer360__state_territory_codes') }} as billing_state_codes
        on (clean_state_and_country.true_billing_state_code = billing_state_codes.state_code
            or clean_state_and_country.true_billing_state_long = billing_state_codes.state_territory)
        and coalesce(billing_country_codes.country_code_alpha_2, clean_state_and_country.true_billing_country_code) = billing_state_codes.alpha2_country_code

    -- grab state and country names for inferred addresses (which are inconplete and not to be used for matching)
    left join {{ ref('customer360__country_codes') }} as inferred_country_codes
        on (clean_state_and_country.true_inferred_country_code = inferred_country_codes.country_code_alpha_2
            and clean_state_and_country.true_inferred_country_long is null) -- sometimes country_code completely != country_long 
        or clean_state_and_country.true_inferred_country_long = inferred_country_codes.country_name
        or clean_state_and_country.true_inferred_country_long = inferred_country_codes.alternative_country_name
    left join {{ ref('customer360__state_territory_codes') }} as inferred_state_codes
        on (clean_state_and_country.true_inferred_state_code = inferred_state_codes.state_code
            or clean_state_and_country.true_inferred_state_long = inferred_state_codes.state_territory) 
        and coalesce(inferred_country_codes.country_code_alpha_2, clean_state_and_country.true_inferred_country_code) = inferred_state_codes.alpha2_country_code
),

tokenize as (

    select 
        lead_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}
        
        -- email
        email,
        {{ dbt.split_part('email', "'@'", 1) }} as email_address,
        {{ dbt.split_part('email', "'@'", 2) }} as email_domain,
        
        -- full name
        first_name,
        last_name,
        {{ customer360.name_titles('first_name') }},
        {{ customer360.name_suffixes('last_name') }},

        -- organization name
        organization_name,
        {{ customer360.org_suffixes('organization_name') }},
        inferred_organization_name,
        {{ customer360.org_suffixes('inferred_organization_name') }}, 

        -- phone
        {{ dbt.split_part('phone', "'ext'", 1) }} as phone_number,
        {{ dbt.split_part('phone', "'ext'", 2) }} as phone_extension,
        {{ dbt.split_part('company_phone', "'ext'", 1) }} as company_phone_number,
        {{ dbt.split_part('company_phone', "'ext'", 2) }} as company_phone_extension,
        {{ dbt.split_part('mobile_phone', "'ext'", 1) }} as mobile_phone_number,
        {{ dbt.split_part('mobile_phone', "'ext'", 2) }} as mobile_phone_extension,

        -- address
        address as full_address,
        {{ customer360.split_address_lines('address') }},
        city,
        state_code,
        state_long,
        country_code,
        country_long,
        country_long_alt,
        postal_code,

        billing_address as full_billing_address,
        {{ customer360.split_address_lines('billing_address') }},
        billing_city,
        billing_state_code,
        billing_state_long,
        billing_country_code,
        billing_country_long,
        billing_country_long_alt,
        billing_postal_code,

        inferred_city,
        inferred_state_code,
        inferred_state_long,
        inferred_country_code,
        inferred_country_long,
        inferred_country_long_alt,
        inferred_postal_code,

        -- ip address
        ip_address,

        -- status
        is_unsubscribed,
        is_email_invalid,
        do_not_call,

        -- timestamps
        updated_at,
        created_at

    from standardize
),

restandardize as (

    select 
        lead_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        -- remove non alpha-numeric characters for matching
        REGEXP_REPLACE(email_address, r'[^a-zA-Z0-9]', '') as email_address_stripped,
        email_domain,    
        
        -- full name
        coalesce(first_name_no_title, '') || case when first_name_no_title is not null and last_name_no_suffix is not null then ' ' else '' end || coalesce(last_name_no_suffix, '') as full_name_clean,
        first_name_no_title as first_name_clean,
        last_name_no_suffix as last_name_clean,
        first_name_title as name_title,
        last_name_suffix as name_suffix,
        coalesce(first_name, '') || case when first_name is not null and last_name is not null then ' ' else '' end || coalesce(last_name, '') as full_name,
        first_name,
        last_name,

        -- company name
        organization_name,
        organization_name_no_suffix,
        organization_name_suffix,
        inferred_organization_name,
        inferred_organization_name_no_suffix,
        inferred_organization_name_suffix,

        -- phone
        case when phone_number is null or phone_number = '' then null else  
            '+' || case when length(phone_number) = 10 then '1' else '' end || REGEXP_REPLACE(phone_number, '[^0-9]', '') 
        end as phone,
        REGEXP_REPLACE(phone_extension, r'[^a-zA-Z0-9]', '') as phone_extension,

        case when company_phone_number is null or company_phone_number = '' then null else
            '+' || case when length(company_phone_number) = 10 then '1' else '' end || REGEXP_REPLACE(company_phone_number, '[^0-9]', '') 
        end as company_phone,
        REGEXP_REPLACE(company_phone_extension, r'[^a-zA-Z0-9]', '') as company_phone_extension,

        case when mobile_phone_number is null or mobile_phone_number = '' then null else
            '+' || case when length(mobile_phone_number) = 10 then '1' else '' end || REGEXP_REPLACE(mobile_phone_number, '[^0-9]', '') 
        end as mobile_phone,
        REGEXP_REPLACE(mobile_phone_extension, r'[^a-zA-Z0-9]', '') as mobile_phone_extension,

        -- address -- todo do st suffixes here
        full_address,
        address_line_1,
        {{ customer360.street_suffixes('address_line_1') }},
        address_line_2,
        city,
        state_code,
        state_long,
        country_code,
        country_long,
        country_long_alt,
        REGEXP_REPLACE(postal_code, '[^0-9]', '') as postal_code,

        full_billing_address,
        billing_address_line_1,
        {{ customer360.street_suffixes('billing_address_line_1') }},
        billing_address_line_2,
        billing_city,
        billing_state_code,
        billing_state_long,
        billing_country_code,
        billing_country_long,
        billing_country_long_alt,
        REGEXP_REPLACE(billing_postal_code, '[^0-9]', '') as billing_postal_code,

        inferred_city,
        inferred_state_code,
        inferred_state_long,
        inferred_country_code,
        inferred_country_long,
        inferred_country_long_alt,
        REGEXP_REPLACE(inferred_postal_code, '[^0-9]', '') as inferred_postal_code,

        -- ip address
        ip_address,

        -- status
        is_unsubscribed,
        is_email_invalid,
        do_not_call,

        -- timestamps
        updated_at,
        created_at

    from tokenize 
),

final as (

    select 
        lead_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        case when email = '' then null else email end as email,
        case when email_address_stripped = '' then null else email_address_stripped end as email_address_stripped,
        case when email_domain = '' then null else email_domain end as email_domain,
        
        -- full name
        case when full_name_clean = '' then null else full_name_clean end as full_name_clean,
        case when first_name_clean = '' then null else first_name_clean end as first_name_clean,
        case when last_name_clean = '' then null else last_name_clean end as last_name_clean,
        case when name_title = '' then null else name_title end as name_title,
        case when name_suffix = '' then null else name_suffix end as name_suffix,
        case when full_name = '' then null else full_name end as full_name,
        case when first_name = '' then null else first_name end as first_name,
        case when last_name = '' then null else last_name end as last_name,
        
        -- company name
        case when organization_name = '' then null else organization_name end as organization_name,
        case when organization_name_no_suffix = '' then null else organization_name_no_suffix end as organization_name_no_suffix,
        case when organization_name_suffix = '' then null else organization_name_suffix end as organization_name_suffix,
        case when inferred_organization_name = '' then null else inferred_organization_name end as inferred_organization_name,
        case when inferred_organization_name_no_suffix = '' then null else inferred_organization_name_no_suffix end as inferred_organization_name_no_suffix,
        case when inferred_organization_name_suffix = '' then null else inferred_organization_name_suffix end as inferred_organization_name_suffix,

        -- phone
        case when phone = '' then null else phone end as phone,
        case when phone_extension = '' then null else phone_extension end as phone_extension,
        case when company_phone = '' then null else company_phone end as company_phone,
        case when company_phone_extension = '' then null else company_phone_extension end as company_phone_extension,
        case when mobile_phone = '' then null else mobile_phone end as mobile_phone,
        case when mobile_phone_extension = '' then null else mobile_phone_extension end as mobile_phone_extension,

        -- address
        case when address_line_1 = '' then null else {{ customer360.trim(str="address_line_1", chars_to_trim=" ,") }} end as address_line_1,
        case when address_line_1_long = '' then null else {{ customer360.trim(str="address_line_1_long", chars_to_trim=" ,") }} end as address_line_1_long,
        case when address_line_2 = '' then null else {{ customer360.trim(str="address_line_2", chars_to_trim=" ,") }} end as address_line_2,

        case when full_address = '' then null else full_address end as full_address,
        case when city = '' then null else city end as city,
        case when state_code = '' then null else state_code end as state_code,
        case when state_long = '' then null else state_long end as state_long,
        case when country_code = '' then null else country_code end as country_code,
        case when country_long = '' then null else country_long end as country_long,
        case when country_long_alt = '' then null else country_long_alt end as country_long_alt,
        case when postal_code = '' then null else postal_code end as postal_code,

        case when billing_address_line_1 = '' then null else {{ customer360.trim(str="billing_address_line_1", chars_to_trim=" ,") }} end as billing_address_line_1,
        case when billing_address_line_1_long = '' then null else {{ customer360.trim(str="billing_address_line_1_long", chars_to_trim=" ,") }} end as billing_address_line_1_long,
        case when billing_address_line_2 = '' then null else {{ customer360.trim(str="billing_address_line_2", chars_to_trim=" ,") }} end as billing_address_line_2,

        case when full_billing_address = '' then null else full_billing_address end as full_billing_address,
        case when billing_city = '' then null else billing_city end as billing_city,
        case when billing_state_code = '' then null else billing_state_code end as billing_state_code,
        case when billing_state_long = '' then null else billing_state_long end as billing_state_long,
        case when billing_country_code = '' then null else billing_country_code end as billing_country_code,
        case when billing_country_long = '' then null else billing_country_long end as billing_country_long,
        case when billing_country_long_alt = '' then null else billing_country_long_alt end as billing_country_long_alt,
        case when billing_postal_code = '' then null else billing_postal_code end as billing_postal_code,

        case when inferred_city = '' then null else inferred_city end as inferred_city,
        case when inferred_state_code = '' then null else inferred_state_code end as inferred_state_code,
        case when inferred_state_long = '' then null else inferred_state_long end as inferred_state_long,
        case when inferred_country_code = '' then null else inferred_country_code end as inferred_country_code,
        case when inferred_country_long = '' then null else inferred_country_long end as inferred_country_long,
        case when inferred_country_long_alt = '' then null else inferred_country_long_alt end as inferred_country_long_alt,
        case when inferred_postal_code = '' then null else inferred_postal_code end as inferred_postal_code,

        -- ip address
        case when ip_address = '' then null else ip_address end as ip_address,

        -- status
        is_unsubscribed,
        is_email_invalid,
        do_not_call,

        -- timestamps
        updated_at,
        created_at,

        {{ fivetran_utils.string_agg(field_to_agg="distinct nicknames.canonical_name", delimiter="', '") }} as possible_alt_first_names

    from restandardize
    left join {{ ref('customer360__nicknames') }} as nicknames 
        on lower(restandardize.first_name_clean) = nicknames.nickname 

    {{ dbt_utils.group_by(n=59 + match_id_list|length) }}
)

select *
from final