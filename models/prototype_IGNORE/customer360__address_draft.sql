with mapping as (

    select * 
    from {{ ref('customer360__mapping_draft') }}
),

marketo as (

    select *
    from {{ ref('marketo__leads') }}
),

stripe as (

    select *
    from {{ ref('stripe__customer_overview') }}
),

marketo_address as (

    select 
        lead_id,
        coalesce(address, address_lead) as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        city,
        coalesce(state, state_code) as state,
        coalesce(country, country_code) as country,
        postal_code,
        'primary' as type

    from marketo
    where coalesce(address, address_lead) is not null

    union all
    
    select 
        lead_id,
        billing_street as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        billing_city as city,
        coalesce(billing_state, billing_state_code) as state,
        coalesce(billing_country, billing_country_code) as country,
        billing_postal_code as postal_code,
        'billing' as type

    from marketo
    where billing_street is not null

    union all 

    select 
        lead_id,
        null as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        inferred_city as city,
        inferred_state_region as state,
        inferred_country as country,
        inferred_postal_code as postal_code,
        'inferred' as type

    from marketo
    where inferred_city is not null
),

stripe_address as (

    select 
        customer_id,
        customer_address_line_1 as address_line_1,
        customer_address_line_2 as address_line_2,
        customer_address_city as city,
        customer_address_state as state,
        customer_address_country as country,
        customer_address_postal_code as postal_code,
        'primary' as type

    from stripe
    where customer_address_line_1 is not null

    union all 

    select 
        customer_id,
        shipping_address_line_1 as address_line_1,
        shipping_address_line_2 as address_line_2,
        shipping_address_city as city,
        shipping_address_state as state,
        shipping_address_country as country,
        shipping_address_postal_code as postal_code,
        'shipping' as type

    from stripe
    where shipping_address_line_1 is not null
),

unioned as (

    select 
        mapping.customer360_id,
        marketo_address.address_line_1,
        marketo_address.address_line_2,
        marketo_address.city,
        marketo_address.state,
        marketo_address.country,
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
        stripe_address.address_line_1,
        stripe_address.address_line_2,
        stripe_address.city,
        stripe_address.state,
        stripe_address.country,
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
        address_line_1,
        address_line_2,
        city,
        state,
        country,
        postal_code,
        type,
        source,
        count(*) over (partition by customer360_id, address_line_1, address_line_2, city, state, country, postal_code) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, address_line_1, address_line_2, city, state, country, postal_code) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        address_line_1,
        address_line_2,
        city,
        state,
        country,
        postal_code,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_value_confidence
)

select * 
from final

