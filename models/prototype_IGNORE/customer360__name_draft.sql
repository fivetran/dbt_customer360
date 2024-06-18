with mapping as (

    select * 
    from {{ ref('customer360__mapping_draft') }}
),

marketo as (

    select *
    from {{ ref('marketo__leads') }}
    where coalesce(first_name, last_name) is not null
),

stripe as (

    select *
    from {{ ref('stripe__customer_overview') }}
),

zendesk as (

    select *
    from {{ ref('stg_zendesk__user') }}
    where role = 'end-user'
    and name is not null
),

stripe_names as (

    select 
        customer_id,
        customer_name as full_name,
        'primary' as type
    from stripe
    where customer_name is not null

    union all 

    select 
        customer_id,
        shipping_name as full_name,
        'shipping' as type
    from stripe
    where shipping_name is not null
),

unioned as (

    select 
        mapping.customer360_id,
        marketo.first_name || ' ' || marketo.last_name as full_name,
        'primary' as type,
        'marketo' as source,
        mapping.marketo_updated_at as updated_at,
        marketo_created_at as created_at

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id

    union all

    select 
        mapping.customer360_id,
        stripe_names.full_name,
        stripe_names.type,
        'stripe' as source,
        stripe_updated_at as updated_at,
        stripe_created_at as created_at

    from mapping
    join stripe_names
        on mapping.stripe_customer_id = stripe_names.customer_id

    union all

    select 
        mapping.customer360_id,
        zendesk.name as full_name,
        'primary' as type,
        'zendesk' as source,
        mapping.zendesk_updated_at as updated_at,
        zendesk_created_at as created_at

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
),

rank_value_confidence as (

    select
        customer360_id,
        full_name,
        type,
        source,
        count(*) over (partition by customer360_id, full_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, full_name) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        full_name,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_value_confidence
)

select * 
from final