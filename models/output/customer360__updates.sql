with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

unioned as (
{% if var('customer360__using_marketo', true) %}
    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        marketo_created_at as created_at,
        marketo_updated_at as updated_at,
        'customer' as type,
        'marketo' as source

    from mapping
    where marketo_lead_id is not null

    union all
{% endif %}

{% if var('customer360__using_stripe', true) %}
    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        stripe_created_at as created_at,
        stripe_updated_at as updated_at,
        'customer' as type,
        'stripe' as source

    from mapping
    where stripe_customer_id is not null

    {% if var('customer360__using_zendesk', true) %}
    union all
    {% endif %}
{% endif %}

{% if var('customer360__using_zendesk', true) %}
    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        zendesk_created_at,
        mapping.zendesk_updated_at as updated_at,
        'customer' as type,
        'zendesk' as source

    from mapping

    union all

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        zendesk_organization_created_at as created_at,
        zendesk_organization_updated_at as updated_at,
        'organization' as type,
        'zendesk' as source

    from mapping
    where zendesk_organization_created_at is not null
{% endif %}
)

select * 
from unioned