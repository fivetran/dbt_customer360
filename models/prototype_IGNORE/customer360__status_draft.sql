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

zendesk as (

    select *
    from {{ ref('stg_zendesk__user') }}
    where role = 'end-user'
),

marketo_status as (

    select 
        mapping.customer360_id,
        'unsubscribed' as status,
        'marketo' as source

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id
    where coalesce(marketo.is_unsubscribed, false)

    union all

    select 
        mapping.customer360_id,
        'email invalid' as status,
        'marketo' as source

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id
    where coalesce(marketo.is_email_invalid, false)

    union all

    select 
        mapping.customer360_id,
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
        'delinquent' as status,
        'stripe' as source

    from mapping
    join stripe
        on mapping.stripe_customer_id = stripe.customer_id
    where coalesce(stripe.is_delinquent, false)

    union all
    
    select 
        mapping.customer360_id,
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
        'deleted' as status,
        'zendesk' as source

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
    where not coalesce(is_active, true)

    union all

    select 
        mapping.customer360_id,
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
        status,
        source

    from marketo_status

    union all

    select 
        customer360_id,
        status,
        source

    from stripe_status

    union all

    select 
        customer360_id,
        status,
        source

    from zendesk_status
)

select * 
from unioned