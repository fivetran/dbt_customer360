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

marketo_status as (

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'unsubscribed' as status,
        'marketo' as source

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id
    where coalesce(marketo.is_unsubscribed, false)

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'email invalid' as status,
        'marketo' as source

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id
    where coalesce(marketo.is_email_invalid, false)

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'do not call' as status,
        'marketo' as source

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id
    where coalesce(marketo.do_not_call, false)
),

stripe_status as (

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'delinquent' as status,
        'stripe' as source

    from mapping
    join stripe
        on mapping.stripe_customer_id = stripe.customer_id
    where coalesce(stripe.is_delinquent, false)

    union all
    
    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'deleted' as status,
        'stripe' as source

    from mapping
    join stripe
        on mapping.stripe_customer_id = stripe.customer_id
    where coalesce(stripe.is_deleted, false)
),

zendesk_status as (

    select 
        customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'deleted' as status,
        'zendesk' as source

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
    where coalesce(is_deleted, false) or not coalesce(is_active, true)

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        'suspended' as status,
        'zendesk' as source

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
    where coalesce(is_suspended, false)
),

unioned as (

    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        status,
        source

    from marketo_status

    union all

    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        status,
        source

    from stripe_status

    union all

    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        status,
        source

    from zendesk_status
)

select * 
from unioned