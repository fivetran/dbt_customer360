with mapping as (

    select * 
    from {{ ref('customer360__mapping_draft') }}
),

zendesk_users as (

    select *
    from {{ ref('stg_zendesk__user') }}
    where role = 'end-user'
    and email is not null
),

zendesk_orgs as (

    select *
    from {{ ref('stg_zendesk__organization') }}
),

zendesk as (

    select 
        zendesk_users.*,
        zendesk_orgs.updated_at as org_updated_at,
        zendesk_orgs.created_at as org_created_at
    from zendesk_users
    left join zendesk_orgs
        on zendesk_users.organization_id = zendesk_orgs.organization_id
),

unioned as (

    select 
        customer360_id,
        marketo_created_at as created_at,
        marketo_updated_at as updated_at,
        'customer' as type,
        'marketo' as source

    from mapping
    where marketo_lead_id is not null

    union all

    select 
        customer360_id,
        stripe_created_at as created_at,
        stripe_updated_at as updated_at,
        'customer' as type,
        'stripe' as source

    from mapping
    where stripe_customer_id is not null

    union all

    select 
        mapping.customer360_id,
        zendesk_created_at,
        mapping.zendesk_updated_at as updated_at,
        'customer' as type,
        'zendesk' as source

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id

    union all

    select 
        mapping.customer360_id,
        zendesk.org_created_at as created_at,
        zendesk.org_updated_at as updated_at,
        'organization' as type,
        'zendesk' as source

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
        and zendesk.org_updated_at is not null
)

select * 
from unioned