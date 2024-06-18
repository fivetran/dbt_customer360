{{ config(enabled=var('customer360__using_stripe', true)) }}

with stripe as (

    select 
        customer_id,
        lower(customer_name_clean) as customer_name_clean,
        lower(shipping_name_clean) as shipping_name_clean,
        email,
        phone,
        shipping_phone,
        lower(customer_address_line_1_long) as customer_address_line_1_long,
        lower(customer_address_line_2_long) as customer_address_line_2_long,
        lower(customer_city) as customer_city,
        customer_state_code,
        lower(customer_state_long) as customer_state_long,
        customer_country_code,
        lower(customer_country_long) as customer_country_long,
        lower(customer_country_long_alt) as customer_country_long_alt,
        customer_postal_code,
        lower(shipping_address_line_1_long) as shipping_address_line_1_long,
        lower(shipping_address_line_2_long) as shipping_address_line_2_long,
        lower(shipping_city) as shipping_city,
        shipping_state_code,
        lower(shipping_state_long) as shipping_state_long,
        shipping_country_code,
        lower(shipping_country_long) as shipping_country_long,
        lower(shipping_country_long_alt) as shipping_country_long_alt,
        shipping_postal_code,
        updated_at,
        created_at
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , {{ match_set.name }}
            {%- endfor %}
        {% endif %}

    from {{ ref('int_customer360__stripe') }}
),

-- let's try to limit the data used for our mega-joins
stripe_matching as (

    select *
    from stripe
    where 
    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {{ match_set.name }} is not null or
        {%- endfor %}
    {% endif %}
    ((coalesce(customer_name_clean, shipping_name_clean) is not null or email is not null)
    and (
        email is not null 
        or coalesce(phone, shipping_phone) is not null
        or (
            coalesce(customer_address_line_1_long, shipping_address_line_1_long, customer_address_line_2_long, shipping_address_line_2_long) is not null and 
                (
                    (
                        coalesce(customer_city, shipping_city) is not null and coalesce(customer_state_long, shipping_state_long, customer_state_code, shipping_state_code) is not null
                    )
                or (
                        coalesce(customer_postal_code, shipping_postal_code) is not null and coalesce(customer_country_long, shipping_country_long, customer_country_code, shipping_country_code) is not null
                    )
                )
            )
        )
    )
),

final as (

    select 
        customer_id,
        customer_name_clean,
        shipping_name_clean,
        coalesce(email, 'null_stripe') as email,
        coalesce(phone, 'null_stripe') as phone,
        coalesce(shipping_phone, 'null_stripe') as shipping_phone,
        coalesce(customer_address_line_1_long, 'null_stripe') as customer_address_line_1_long,
        coalesce(customer_address_line_2_long, 'null_stripe') as customer_address_line_2_long,
        coalesce(customer_city, 'null_stripe') as customer_city,
        coalesce(customer_state_code, 'null_stripe') as customer_state_code,
        coalesce(customer_state_long, 'null_stripe') as customer_state_long,
        coalesce(customer_country_code, 'null_stripe') as customer_country_code,
        coalesce(customer_country_long, 'null_stripe') as customer_country_long,
        coalesce(customer_country_long_alt, 'null_stripe') as customer_country_long_alt,
        coalesce(customer_postal_code, 'null_stripe') as customer_postal_code,
        coalesce(shipping_address_line_1_long, 'null_stripe') as shipping_address_line_1_long,
        coalesce(shipping_address_line_2_long, 'null_stripe') as shipping_address_line_2_long,
        coalesce(shipping_city, 'null_stripe') as shipping_city,
        coalesce(shipping_state_code, 'null_stripe') as shipping_state_code,
        coalesce(shipping_state_long, 'null_stripe') as shipping_state_long,
        coalesce(shipping_country_code, 'null_stripe') as shipping_country_code,
        coalesce(shipping_country_long, 'null_stripe') as shipping_country_long,
        coalesce(shipping_country_long_alt, 'null_stripe') as shipping_country_long_alt,
        coalesce(shipping_postal_code, 'null_stripe') as shipping_postal_code,
        updated_at,
        created_at
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , coalesce(cast({{ match_set.name }} as {{ dbt.type_string() }}), 'null_stripe') as {{ match_set.name }}
            {%- endfor %}
        {% endif %}

    from stripe_matching
)

select *
from final