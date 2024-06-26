with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

marketo as (

    select *
    from {{ ref('int_customer360__marketo') }}
),

stripe as (

    select *
    from {{ ref('int_customer360__stripe') }}
),

marketo_address as (

    select 
        lead_id,
        address_line_1_long as address_line_1,
        address_line_2,
        city,
        state_code,
        state_long,
        country_code,
        country_long,
        country_long_alt,
        postal_code,
        'primary' as type

    from marketo
    where coalesce(address_line_1_long, address_line_2, city, state_long, country_long, postal_code) is not null

    union all
    
    select 
        lead_id,
        billing_address_line_1_long as address_line_1,
        billing_address_line_2 as address_line_2,
        billing_city as city,
        billing_state_code as state_code,
        billing_state_long as state_long,
        billing_country_code as country_code,
        billing_country_long as country_long,
        billing_country_long_alt as country_long_alt,
        billing_postal_code as postal_code,
        'billing' as type

    from marketo
    where coalesce(billing_address_line_1_long, billing_address_line_2, billing_city, billing_state_long, billing_country_long, billing_postal_code) is not null

    union all 

    select 
        lead_id,
        cast(null as {{ dbt.type_string() }}) as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        inferred_city as city,
        inferred_state_code as state,
        inferred_state_long as state_long,
        inferred_country_code as country_code,
        inferred_country_long as country_long,
        inferred_country_long_alt as country_long_alt,
        inferred_postal_code as postal_code,
        'inferred' as type

    from marketo
    where coalesce(inferred_city, inferred_state_long, inferred_country_long, inferred_postal_code) is not null
),

stripe_address as (

    select 
        customer_id,
        customer_address_line_1_long as address_line_1,
        customer_address_line_2_long as address_line_2,
        customer_city as city,
        customer_state_code as state_code,
        customer_state_long as state_long,
        customer_country_code as country_code,
        customer_country_long as country_long,
        customer_country_long_alt as country_long_alt,
        customer_postal_code as postal_code,
        'primary' as type

    from stripe
    where coalesce(customer_address_line_1_long, customer_address_line_2_long, customer_city, customer_state_long, customer_country_long, customer_postal_code) is not null

    union all 

    select 
        customer_id,
        shipping_address_line_1_long as address_line_1,
        shipping_address_line_2_long as address_line_2,
        shipping_city as city,
        shipping_state_code as state_code,
        shipping_state_long as state_long,
        shipping_country_code as country_code,
        shipping_country_long as country_long,
        shipping_country_long_alt as country_long_alt,
        shipping_postal_code as postal_code,
        'shipping' as type

    from stripe
    where coalesce(shipping_address_line_1_long, shipping_address_line_2_long, shipping_city, shipping_state_long, shipping_country_long, shipping_postal_code) is not null
),

unioned as (

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        marketo_address.address_line_1,
        marketo_address.address_line_2,
        marketo_address.city,
        marketo_address.state_code,
        marketo_address.state_long,
        marketo_address.country_code,
        marketo_address.country_long,
        marketo_address.country_long_alt,
        marketo_address.postal_code,
        marketo_address.type,
        'marketo' as source,
        mapping.marketo_updated_at as updated_at,
        mapping.marketo_created_at as created_at,

    from mapping
    join marketo_address
        on mapping.marketo_lead_id = marketo_address.lead_id

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        stripe_address.address_line_1,
        stripe_address.address_line_2,
        stripe_address.city,
        stripe_address.state_code,
        stripe_address.state_long,
        stripe_address.country_code,
        stripe_address.country_long,
        stripe_address.country_long_alt,
        stripe_address.postal_code,
        stripe_address.type,
        'stripe' as source,
        stripe_updated_at as updated_at,
        mapping.stripe_created_at as created_at

    from mapping
    join stripe_address
        on mapping.stripe_customer_id = stripe_address.customer_id
),

rank_value_confidence as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        address_line_1,
        address_line_2,
        city,
        state_code,
        state_long,
        country_code,
        country_long,
        country_long_alt,
        postal_code,
        type,
        source,
        count(*) over (partition by customer360_id, address_line_1, address_line_2, city, state_long, country_long, postal_code) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, address_line_1, address_line_2, city, state_long, country_long, postal_code) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        address_line_1,
        address_line_2,
        city,
        state_long as state,
        country_long as country,
        country_long_alt as country_alt_name,
        postal_code,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc, (case when type = 'inferred' then 1 else 0 end) asc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc, (case when type = 'inferred' then 1 else 0 end) asc) as index

    from rank_value_confidence
)

select * 
from final
