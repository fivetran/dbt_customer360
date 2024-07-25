with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

{% if var('customer360__using_marketo', true) %}
marketo as (

    select *
    from {{ ref('int_customer360__marketo') }}
    where email is not null
),
{% endif %}

{% if var('customer360__using_stripe', true) %}
stripe as (

    select *
    from {{ ref('int_customer360__stripe') }}
    where email is not null
),
{% endif %}

{% if var('customer360__using_zendesk', true) %}
zendesk as (

    select *
    from {{ ref('int_customer360__zendesk') }}
    where email is not null
),
{% endif %}

unioned as (
{% if var('customer360__using_marketo', true) %}
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
{% endif %}

{% if var('customer360__using_stripe', true) %}
    {% if var('customer360__using_marketo', true) %}
    union all
    {% endif %}

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
{% endif %}

{% if var('customer360__using_zendesk', true) %}
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
{% endif %}
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