{{ config(enabled=var('customer360__using_stripe', true)) }}

{%- set match_id_list = [] -%}

with customer as (

    select stripe__customer_overview.*

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.stripe %}
                {%- if match_set.stripe.map_table %}
                , map.{{ match_set.stripe.match_key }}
                {%- else %}
                , {{ match_set.stripe.match_key }}
                {%- endif %}
            {%- else %}
                , null 
            {%- endif %} as {{ match_set.name }}
            {% do match_id_list.append(match_set.name) -%}
        {%- endfor %}
    {% endif %}

    from {{ ref('stripe__customer_overview') }}

    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {%- if match_set.stripe %}
                {%- if match_set.stripe.map_table %}

    left join {{ fivetran_utils.wrap_in_quotes(match_set.stripe.map_table) }} as map 
        on map.{{ match_set.stripe.map_table_join_on }} = stripe__customer_overview.{{ match_set.stripe.join_with_map_on }}

                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}
),

clean_state_and_country as (

    select 
        *,
        -- sometimes the customer_address_state is the code, sometimes the longform name
        -- most state codes are 2-characters, but a handful or international states are 3-char
        case 
            when length(customer_address_state) <= 3 then upper(customer_address_state)
            else null
        end as customer_state_code,
        case 
            when length(customer_address_state) > 3 then customer_address_state
            else null
        end as customer_state_long,

        case 
            when length(shipping_address_state) <= 3 then upper(shipping_address_state)
            else null
        end as shipping_state_code,
        case 
            when length(shipping_address_state) > 3 then shipping_address_state
            else null
        end as shipping_state_long

    from customer
),

standardize as (

    select
        customer_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        lower(email) as email,

        -- full  name
        ---- in Stripe this can reflect an individual, an organization, or potentially both.
        ---- in the case of both, use the following variables to split them up based on your internally enforced pattern.
        {{ var('stripe_customer_full_name_extract_sql', 'customer_name') }} as customer_name,
        {{ var('stripe_shipping_full_name_extract_sql', 'shipping_name') }} as shipping_name,

        -- organization name (in stripe there's one field that maps onto both)
        ---- in Stripe this can reflect an individual, an organization, or potentially both.
        ---- in the case of both, use the following variables to split them up based on your internally enforced pattern.
        {{ var('stripe_customer_organization_name_extract_sql', 'customer_name') }} as customer_organization_name,
        {{ var('stripe_shipping_organization_name_extract_sql', 'shipping_name') }} as shipping_organization_name,

        -- phone
        -- remove non-alphanumeric characters and standardize extension format 
        replace(lower(REGEXP_REPLACE(replace(phone, '#', 'ext'), r'[^a-zA-Z0-9]', '')), 'extension', 'ext') as phone,
        replace(lower(REGEXP_REPLACE(replace(shipping_phone, '#', 'ext'), r'[^a-zA-Z0-9]', '')), 'extension', 'ext') as shipping_phone,

        -- address
        customer_address_line_1,
        {{ customer360.street_suffixes('customer_address_line_1') }},
        customer_address_line_2,
        {{ customer360.street_suffixes('customer_address_line_2') }}, -- sometimes stripe switches these
        customer_address_city as customer_city,
        coalesce(customer_state_code, customer_state_codes.state_code) as customer_state_code,
        coalesce(customer_state_long, customer_state_codes.state_territory) as customer_state_long,
        customer_address_country as customer_country_code,
        customer_country_codes.country_name as customer_country_long,
        customer_country_codes.alternative_country_name as customer_country_long_alt,
        customer_address_postal_code as customer_postal_code,
        
        shipping_address_line_1,
        {{ customer360.street_suffixes('shipping_address_line_1') }},
        shipping_address_line_2,
        {{ customer360.street_suffixes('shipping_address_line_2') }}, -- sometimes stripe switches these
        shipping_address_city as shipping_city,
        coalesce(shipping_state_code, shipping_state_codes.state_code) as shipping_state_code,
        coalesce(shipping_state_long, shipping_state_codes.state_territory) as shipping_state_long,
        shipping_address_country as shipping_country_code,
        shipping_country_codes.country_name as shipping_country_long,
        shipping_country_codes.alternative_country_name as shipping_country_long_alt,
        shipping_address_postal_code as shipping_postal_code,

        -- no ip address

        -- status
        is_delinquent,
        is_deleted,

        -- timestamps
        cast(null as {{ dbt.type_timestamp() }}) as updated_at, -- stripe doesn't have this
        customer_created_at as created_at

    from clean_state_and_country

    left join {{ ref('customer360__country_codes') }} as customer_country_codes
        on clean_state_and_country.customer_address_country = customer_country_codes.country_code_alpha_2
    left join {{ ref('customer360__country_codes') }} as shipping_country_codes
        on clean_state_and_country.shipping_address_country = shipping_country_codes.country_code_alpha_2

    -- sometimes state is code or a longform name
    left join {{ ref('customer360__state_territory_codes') }} as customer_state_codes
        on (clean_state_and_country.customer_state_code = customer_state_codes.state_code
            or clean_state_and_country.customer_state_long = customer_state_codes.state_territory) 
        and clean_state_and_country.customer_address_country = customer_state_codes.alpha2_country_code
    left join {{ ref('customer360__state_territory_codes') }} as shipping_state_codes
        on (clean_state_and_country.shipping_state_code = shipping_state_codes.state_code
            or clean_state_and_country.shipping_state_long = shipping_state_codes.state_territory)
        and clean_state_and_country.shipping_address_country = shipping_state_codes.alpha2_country_code
),

tokenize as (

    select 
        customer_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        {{ dbt.split_part('email', "'@'", 1) }} as email_address,
        {{ dbt.split_part('email', "'@'", 2) }} as email_domain,
        
        -- full name
        customer_name,
        {{ customer360.name_titles('customer_name') }},
        
        shipping_name,
        {{ customer360.name_titles('shipping_name') }},

        -- organization name
        customer_organization_name,
        {{ customer360.org_suffixes('customer_organization_name') }},
        shipping_organization_name,
        {{ customer360.org_suffixes('shipping_organization_name') }}, 

        -- phone
        {{ dbt.split_part('phone', "'ext'", 1) }} as phone_number,
        {{ dbt.split_part('phone', "'ext'", 2) }} as phone_extension,
        {{ dbt.split_part('shipping_phone', "'ext'", 1) }} as shipping_phone_number,
        {{ dbt.split_part('shipping_phone', "'ext'", 2) }} as shipping_phone_extension,

        -- address
        customer_address_line_1,
        customer_address_line_1_long,
        customer_address_line_2,
        customer_address_line_2_long,
        customer_city,
        customer_state_code,
        customer_state_long,
        customer_country_code,
        customer_country_long,
        customer_country_long_alt,
        customer_postal_code,

        shipping_address_line_1,
        shipping_address_line_1_long,
        shipping_address_line_2,
        shipping_address_line_2_long,
        shipping_city,
        shipping_state_code,
        shipping_state_long,
        shipping_country_code,
        shipping_country_long,
        shipping_country_long_alt,
        shipping_postal_code,

        -- no ip address

        -- status
        is_delinquent,
        is_deleted,

        -- timestamps
        updated_at,
        created_at

    from standardize
),

restandardize as (

    select 
        customer_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        -- remove non alpha-numeric characters for matching
        REGEXP_REPLACE(email_address, r'[^a-zA-Z0-9]', '') as email_address_stripped,
        email_domain,    
        
        -- full name
        customer_name,
        customer_name_title,
        {{ customer360.name_suffixes('customer_name_no_title') }},

        shipping_name,
        shipping_name_title,
        {{ customer360.name_suffixes('shipping_name_no_title') }},

        -- company name
        customer_organization_name,
        customer_organization_name_no_suffix,
        customer_organization_name_suffix,
        shipping_organization_name,
        shipping_organization_name_no_suffix,
        shipping_organization_name_suffix,

        -- phone
        case when phone_number is null or phone_number = '' then null else  
            '+' || case when length(phone_number) = 10 then '1' else '' end || REGEXP_REPLACE(phone_number, '[^0-9]', '') 
        end as phone,
        REGEXP_REPLACE(phone_extension, r'[^a-zA-Z0-9]', '') as phone_extension,

        case when shipping_phone_number is null or shipping_phone_number = '' then null else
            '+' || case when length(shipping_phone_number) = 10 then '1' else '' end || REGEXP_REPLACE(shipping_phone_number, '[^0-9]', '') 
        end as shipping_phone,
        REGEXP_REPLACE(shipping_phone_extension, r'[^a-zA-Z0-9]', '') as shipping_phone_extension,

        -- address
        customer_address_line_1,
        customer_address_line_1_long,
        customer_address_line_2,
        customer_address_line_2_long,
        customer_city,
        customer_state_code,
        customer_state_long,
        customer_country_code,
        customer_country_long,
        customer_country_long_alt,
        REGEXP_REPLACE(cast(customer_postal_code as {{ dbt.type_string () }}), '[^0-9]', '') as customer_postal_code,
        shipping_address_line_1,
        shipping_address_line_1_long,
        shipping_address_line_2,
        shipping_address_line_2_long,
        shipping_city,
        shipping_state_code,
        shipping_state_long,
        shipping_country_code,
        shipping_country_long,
        shipping_country_long_alt,
        REGEXP_REPLACE(cast(shipping_postal_code as {{ dbt.type_string() }}), '[^0-9]', '') as shipping_postal_code,

        -- no ip address

        -- status
        is_delinquent,
        is_deleted,

        -- timestamps
        updated_at,
        created_at

    from tokenize 
),

split_first_name as (

    select 
        customer_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}

        -- email
        email,
        email_address_stripped,
        email_domain,
        
        -- full name
        customer_name,
        customer_name_no_title_no_suffix as customer_name_clean,
        customer_name_no_title_suffix as customer_name_suffix,
        customer_name_title,
        case 
            when customer_name_no_title_no_suffix like '%, %' then {{ dbt.split_part('customer_name_no_title_no_suffix', "', '", 2) }}
            else {{ dbt.split_part('customer_name_no_title_no_suffix', "' '", 1) }} 
        end as customer_first_name,
        case 
            when customer_name_no_title_no_suffix like '%, %' then {{ dbt.split_part('customer_name_no_title_no_suffix', "', '", 1) }}
            else {{ dbt.split_part('customer_name_no_title_no_suffix', "' '", 2) }} 
        end as customer_last_name,

        shipping_name,
        shipping_name_no_title_no_suffix as shipping_name_clean,
        shipping_name_no_title_suffix as shipping_name_suffix,
        shipping_name_title,
        case 
            when shipping_name_no_title_no_suffix like '%, %' then {{ dbt.split_part('shipping_name_no_title_no_suffix', "', '", 2) }}
            else {{ dbt.split_part('shipping_name_no_title_no_suffix', "' '", 1) }} 
        end as shipping_first_name,
        case 
            when shipping_name_no_title_no_suffix like '%, %' then {{ dbt.split_part('shipping_name_no_title_no_suffix', "', '", 1) }}
            else {{ dbt.split_part('shipping_name_no_title_no_suffix', "' '", 2) }} 
        end as shipping_last_name,

        -- company name
        customer_organization_name,
        customer_organization_name_no_suffix,
        customer_organization_name_suffix,
        shipping_organization_name,
        shipping_organization_name_no_suffix,
        shipping_organization_name_suffix,

        -- phone
        phone,
        phone_extension,
        shipping_phone,
        shipping_phone_extension,

        -- address
        customer_address_line_1,
        customer_address_line_1_long,
        customer_address_line_2,
        customer_address_line_2_long,
        customer_city,
        customer_state_code,
        customer_state_long,
        customer_country_code,
        customer_country_long,
        customer_country_long_alt,
        customer_postal_code,
        shipping_address_line_1,
        shipping_address_line_1_long,
        shipping_address_line_2,
        shipping_address_line_2_long,
        shipping_city,
        shipping_state_code,
        shipping_state_long,
        shipping_country_code,
        shipping_country_long,
        shipping_country_long_alt,
        shipping_postal_code,

        -- no ip address

        -- status
        is_delinquent,
        is_deleted,

        -- timestamps
        updated_at,
        created_at

    from restandardize
),

add_nicknames as (

    select 
        customer_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}
        
        -- email
        email,
        email_address_stripped,
        email_domain,
        
        -- full name
        customer_name,
        coalesce(customer_first_name, '') || case when customer_first_name is not null and customer_last_name is not null then ' ' else '' end || coalesce(customer_last_name, '') as customer_name_clean,
        customer_name_suffix,
        customer_name_title,

        shipping_name,
        coalesce(shipping_first_name, '') || case when shipping_first_name is not null and shipping_last_name is not null then ' ' else '' end || coalesce(shipping_last_name, '') as shipping_name_clean,
        shipping_name_suffix,
        shipping_name_title,
        
        -- company name
        customer_organization_name,
        customer_organization_name_no_suffix,
        customer_organization_name_suffix,
        shipping_organization_name,
        shipping_organization_name_no_suffix,
        shipping_organization_name_suffix,

        -- phone
        phone,
        phone_extension,
        shipping_phone,
        shipping_phone_extension,

        -- address
        customer_address_line_1,
        customer_address_line_1_long,
        customer_address_line_2,
        customer_address_line_2_long,
        customer_city,
        customer_state_code,
        customer_state_long,
        customer_country_code,
        customer_country_long,
        customer_country_long_alt,
        customer_postal_code,
        shipping_address_line_1,
        shipping_address_line_1_long,
        shipping_address_line_2,
        shipping_address_line_2_long,
        shipping_city,
        shipping_state_code,
        shipping_state_long,
        shipping_country_code,
        shipping_country_long,
        shipping_country_long_alt,
        shipping_postal_code,

        -- no ip address

        -- status
        is_delinquent,
        is_deleted,

        -- timestamps
        updated_at,
        created_at,

        {{ fivetran_utils.string_agg(field_to_agg="distinct customer_nicknames.canonical_name", delimiter="', '") }} as customer_possible_alt_first_names,
        {{ fivetran_utils.string_agg(field_to_agg="distinct shipping_nicknames.canonical_name", delimiter="', '") }} as shipping_possible_alt_first_names
    
    from split_first_name
    left join {{ ref('customer360__nicknames') }} as customer_nicknames 
        on lower(split_first_name.customer_first_name) = customer_nicknames.nickname 
    left join {{ ref('customer360__nicknames') }} as shipping_nicknames 
        on lower(split_first_name.shipping_first_name) = shipping_nicknames.nickname 

    {{ dbt_utils.group_by(n=48 + match_id_list|length) }}
),

final as (

    select 
        customer_id,
        {%- if match_id_list|length > 0 %}
        {{ match_id_list | join(",") }},
        {%- endif %}
        
        -- email
        case when email = '' then null else email end as email,
        case when email_address_stripped = '' then null else email_address_stripped end as email_address_stripped,
        case when email_domain = '' then null else email_domain end as email_domain,
        
        -- full name
        case when customer_name = '' then null else customer_name end as customer_name,
        case when customer_name_clean = '' then null else customer_name_clean end as customer_name_clean,
        case when customer_name_title = '' then null else customer_name_title end as customer_name_title,
        case when customer_name_suffix = '' then null else customer_name_suffix end as customer_name_suffix,

        case when shipping_name = '' then null else shipping_name end as shipping_name,
        case when shipping_name_clean = '' then null else shipping_name_clean end as shipping_name_clean,
        case when shipping_name_title = '' then null else shipping_name_title end as shipping_name_title,
        case when shipping_name_suffix = '' then null else shipping_name_suffix end as shipping_name_suffix,
        
        -- company name
        case when customer_organization_name = '' then null else customer_organization_name end as customer_organization_name,
        case when customer_organization_name_no_suffix = '' then null else customer_organization_name_no_suffix end as customer_organization_name_no_suffix,
        case when customer_organization_name_suffix = '' then null else customer_organization_name_suffix end as customer_organization_name_suffix,
        case when shipping_organization_name = '' then null else shipping_organization_name end as shipping_organization_name,
        case when shipping_organization_name_no_suffix = '' then null else shipping_organization_name_no_suffix end as shipping_organization_name_no_suffix,
        case when shipping_organization_name_suffix = '' then null else shipping_organization_name_suffix end as shipping_organization_name_suffix,

        -- phone
        case when phone = '' then null else phone end as phone,
        case when phone_extension = '' then null else phone_extension end as phone_extension,
        case when shipping_phone = '' then null else shipping_phone end as shipping_phone,
        case when shipping_phone_extension = '' then null else shipping_phone_extension end as shipping_phone_extension,

        -- address
        case when customer_address_line_1 = '' then null else customer_address_line_1 end as customer_address_line_1,
        case when customer_address_line_1_long = '' then null else customer_address_line_1_long end as customer_address_line_1_long,
        case when customer_address_line_2 = '' then null else customer_address_line_2 end as customer_address_line_2,
        case when customer_address_line_2_long = '' then null else customer_address_line_2_long end as customer_address_line_2_long,
        case when customer_city = '' then null else customer_city end as customer_city,
        case when customer_state_code = '' then null else customer_state_code end as customer_state_code,
        case when customer_state_long = '' then null else customer_state_long end as customer_state_long,
        case when customer_country_code = '' then null else customer_country_code end as customer_country_code,
        case when customer_country_long = '' then null else customer_country_long end as customer_country_long,
        case when customer_country_long_alt = '' then null else customer_country_long_alt end as customer_country_long_alt,
        case when customer_postal_code = '' then null else customer_postal_code end as customer_postal_code,
        
        case when shipping_address_line_1 = '' then null else shipping_address_line_1 end as shipping_address_line_1,
        case when shipping_address_line_1_long = '' then null else shipping_address_line_1_long end as shipping_address_line_1_long,
        case when shipping_address_line_2 = '' then null else shipping_address_line_2 end as shipping_address_line_2,
        case when shipping_address_line_2_long = '' then null else shipping_address_line_2_long end as shipping_address_line_2_long,
        case when shipping_city = '' then null else shipping_city end as shipping_city,
        case when shipping_state_code = '' then null else shipping_state_code end as shipping_state_code,
        case when shipping_state_long = '' then null else shipping_state_long end as shipping_state_long,
        case when shipping_country_code = '' then null else shipping_country_code end as shipping_country_code,
        case when shipping_country_long = '' then null else shipping_country_long end as shipping_country_long,
        case when shipping_country_long_alt = '' then null else shipping_country_long_alt end as shipping_country_long_alt,
        case when shipping_postal_code = '' then null else shipping_postal_code end as shipping_postal_code,

        -- no ip address

        -- status
        is_delinquent,
        is_deleted,

        -- timestamps
        updated_at,
        created_at,

        -- nicknames
        customer_possible_alt_first_names,
        shipping_possible_alt_first_names

    from add_nicknames
)

select *
from final