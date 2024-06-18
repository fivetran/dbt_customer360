{{ config(enabled=var('customer360__using_marketo', true)) }}

with marketo as (

    select 
        lead_id,
        lower(full_name_clean) as full_name_clean,
        email,
        phone,
        company_phone,
        mobile_phone,
        lower(address_line_1_long) as address_line_1_long,
        lower(city) as city,
        state_code,
        lower(state_long) as state_long,
        country_code,
        lower(country_long) as country_long,
        lower(country_long_alt) as country_long_alt,
        postal_code,
        lower(billing_address_line_1_long) as billing_address_line_1_long,
        lower(billing_city) as billing_city,
        billing_state_code,
        lower(billing_state_long) as billing_state_long,
        billing_country_code,
        lower(billing_country_long) as billing_country_long,
        lower(billing_country_long_alt) as billing_country_long_alt,
        billing_postal_code,
        updated_at,
        created_at
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , {{ match_set.name }}
            {%- endfor %}
        {% endif %}

    from {{ ref('int_customer360__marketo') }}
),

-- let's try to limit the data used for our mega-joins
marketo_matching as (

    select *
    from marketo 
    where 
    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {{ match_set.name }} is not null or
        {%- endfor %}
    {% endif %}
    ((full_name_clean is not null or email is not null)
    and (
        email is not null 
        or coalesce(phone, company_phone, mobile_phone) is not null
        or (
            coalesce(address_line_1_long, billing_address_line_1_long) is not null and 
                (
                    (
                        coalesce(city, billing_city) is not null and coalesce(state_long, billing_state_long, state_code, billing_state_code) is not null
                    )
                or (
                        coalesce(postal_code, billing_postal_code) is not null and coalesce(country_long, billing_country_long, country_code, billing_country_code) is not null
                    )
                )
            )
        )
    )
),

final as (

    select
        lead_id,
        full_name_clean,
        coalesce(email, 'null_marketo') as email,
        coalesce(phone, 'null_marketo') as phone,
        coalesce(company_phone, 'null_marketo') as company_phone,
        coalesce(mobile_phone, 'null_marketo') as mobile_phone,
        coalesce(address_line_1_long, 'null_marketo') as address_line_1_long,
        coalesce(city, 'null_marketo') as city,
        coalesce(state_code, 'null_marketo') as state_code,
        coalesce(state_long, 'null_marketo') as state_long,
        coalesce(country_code, 'null_marketo') as country_code,
        coalesce(country_long, 'null_marketo') as country_long,
        coalesce(country_long_alt, 'null_marketo') as country_long_alt,
        coalesce(postal_code, 'null_marketo') as postal_code,
        coalesce(billing_address_line_1_long, 'null_marketo') as billing_address_line_1_long,
        coalesce(billing_city, 'null_marketo') as billing_city,
        coalesce(billing_state_code, 'null_marketo') as billing_state_code,
        coalesce(billing_state_long, 'null_marketo') as billing_state_long,
        coalesce(billing_country_code, 'null_marketo') as billing_country_code,
        coalesce(billing_country_long, 'null_marketo') as billing_country_long,
        coalesce(billing_country_long_alt, 'null_marketo') as billing_country_long_alt,
        coalesce(billing_postal_code, 'null_marketo') as billing_postal_code,
        updated_at,
        created_at
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , coalesce(cast({{ match_set.name }} as {{ dbt.type_string() }}), 'null_marketo') as {{ match_set.name }}
            {%- endfor %}
        {% endif %}
        
    from marketo_matching
)

select *
from final