with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
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

    union all

    select 
        mapping.customer360_id,
        zendesk_organization_created_at as created_at,
        zendesk_organization_updated_at as updated_at,
        'organization' as type,
        'zendesk' as source

    from mapping
    where zendesk_organization_created_at is not null
)

select * 
from unioned