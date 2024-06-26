with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

marketo as (

    select *
    from {{ ref('int_customer360__marketo') }}
    where email is not null
),

stripe as (

    select *
    from {{ ref('int_customer360__stripe') }}
    where email is not null
),

zendesk as (

    select *
    from {{ ref('int_customer360__zendesk') }}
    where email is not null
),

unioned as (

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        marketo.email,
        'marketo' as source,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        stripe.email,
        'stripe' as source,
        stripe_updated_at as updated_at,
        stripe_created_at as created_at

    from mapping
    join stripe
        on mapping.stripe_customer_id = stripe.customer_id

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        zendesk.email,
        'zendesk' as source,
        zendesk_updated_at as updated_at,
        zendesk_created_at as created_at

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
),

rank_value_confidence as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        email,
        source,
        count(*) over (partition by customer360_id, email) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, email) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        email,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_value_confidence
)

select * 
from final