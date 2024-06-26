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

zendesk as (

    select *
    from {{ ref('int_customer360__zendesk') }}
),

marketo_org_names as (

    select
        lead_id,
        organization_name,
        'primary' as type

    from marketo
    where organization_name is not null

    union all 

    select
        lead_id,
        inferred_organization_name as organization_name,
        'inferred' as type

    from marketo
    where inferred_organization_name is not null
),

stripe_org_names as (

    select 
        customer_id,
        customer_organization_name as organization_name,
        'primary' as type
    from stripe
    where customer_organization_name is not null

    union all 

    select 
        customer_id,
        shipping_organization_name as organization_name,
        'shipping' as type
    from stripe
    where shipping_organization_name is not null
),

unioned as (

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        marketo_org_names.organization_name,
        marketo_org_names.type,
        'marketo' as source,
        mapping.marketo_updated_at as updated_at,
        marketo_created_at as created_at

    from mapping
    join marketo_org_names
        on mapping.marketo_lead_id = marketo_org_names.lead_id

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        stripe_org_names.organization_name,
        stripe_org_names.type,
        'stripe' as source,
        stripe_updated_at as updated_at,
        stripe_created_at as created_at

    from mapping
    join stripe_org_names
        on mapping.stripe_customer_id = stripe_org_names.customer_id

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        zendesk.organization_name,
        'primary' as type,
        'zendesk' as source,
        greatest(mapping.zendesk_updated_at, zendesk.organization_updated_at) as updated_at,
        greatest(mapping.zendesk_created_at, zendesk.organization_created_at) as created_at

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
),

rank_value_confidence as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        organization_name,
        type,
        source,
        count(*) over (partition by customer360_id, organization_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, organization_name) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        organization_name,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc, (case when type = 'inferred' then 1 else 0 end) asc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc, (case when type = 'inferred' then 1 else 0 end) asc) as index

    from rank_value_confidence
)

select * 
from final