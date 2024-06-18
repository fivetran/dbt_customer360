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

zendesk_users as (

    select *
    from {{ ref('stg_zendesk__user') }}
    where role = 'end-user'
),

zendesk_orgs as (

    select *
    from {{ ref('stg_zendesk__organization') }}
),

marketo_org_names as (

    select
        lead_id,
        company as organization_name,
        'primary' as type

    from marketo
    where company is not null

    union all 

    select
        lead_id,
        inferred_company as organization_name,
        'inferred' as type

    from marketo
    where inferred_company is not null
),

stripe_org_names as (

    select 
        customer_id,
        customer_name as organization_name,
        'primary' as type
    from stripe
    where customer_name is not null

    union all 

    select 
        customer_id,
        shipping_name as organization_name,
        'shipping' as type
    from stripe
    where shipping_name is not null
),

zendesk_org_names as (

    select 
        zendesk_users.*,
        zendesk_orgs.name as organization_name,
        zendesk_orgs.updated_at as org_updated_at,
        zendesk_orgs.created_at as org_created_at
    from zendesk_users
    left join zendesk_orgs
        on zendesk_users.organization_id = zendesk_orgs.organization_id
),

unioned as (

    select 
        mapping.customer360_id,
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
        zendesk_org_names.organization_name,
        'primary' as type,
        'zendesk' as source,
        greatest(mapping.zendesk_updated_at, zendesk_org_names.org_updated_at) as updated_at,
        greatest(mapping.zendesk_created_at, zendesk_org_names.org_created_at) as created_at

    from mapping
    join zendesk_org_names
        on mapping.zendesk_user_id = zendesk_org_names.user_id
),

rank_value_confidence as (

    select
        customer360_id,
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
        organization_name,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_value_confidence
)

select * 
from final