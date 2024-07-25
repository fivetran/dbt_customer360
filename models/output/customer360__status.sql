with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

{% if var('customer360__using_marketo', true) %}
marketo as (

    select *
    from {{ ref('int_customer360__marketo') }}
),
{% endif %}

{% if var('customer360__using_stripe', true) %}
stripe as (

    select *
    from {{ ref('int_customer360__stripe') }}
),
{% endif %}

{% if var('customer360__using_zendesk', true) %}
zendesk as (

    select *
    from {{ ref('int_customer360__zendesk') }}
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
{% endif %}

{% if var('customer360__using_marketo', true) %}
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
{% endif %}

{% if var('customer360__using_stripe', true) %}
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
{% endif %}

unioned as (

{% if var('customer360__using_marketo', true) %}
    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        status,
        source

    from marketo_status

    union all
{% endif %}

{% if var('customer360__using_stripe', true) %}
    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        status,
        source

    from stripe_status

    {% if var('customer360__using_zendesk', true) %}
    union all
    {% endif %}
{% endif %}

{% if var('customer360__using_zendesk', true) %}
    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        status,
        source

    from zendesk_status
{% endif %}
)

select * 
from unioned