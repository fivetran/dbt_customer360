with source_matches as (

    select *
    from {{ ref('int_customer360__source_matches') }}
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

{% set match_id_list = [] -%}

combine_all_results as (

    select
        coalesce(marketo_lead_id, marketo.lead_id) as marketo_lead_id,
        coalesce(stripe_customer_id, stripe.customer_id) as stripe_customer_id,
        coalesce(zendesk_user_id, zendesk.user_id) as zendesk_user_id,
        zendesk.organization_id as zendesk_organization_id,

        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                coalesce(
                    source_matches.{{ match_set.name }}, 
                    marketo.{{ match_set.name }}, 
                    stripe.{{ match_set.name }}, 
                    zendesk.{{ match_set.name }}
                ) as {{ match_set.name }},
                {% do match_id_list.append(match_set.name) -%}
            {%- endfor %}
        {% endif %}

        max(coalesce(marketo_updated_at, marketo.updated_at)) as marketo_updated_at,
        max(coalesce(stripe_updated_at, stripe.updated_at)) as stripe_updated_at,
        max(coalesce(zendesk_updated_at, zendesk.updated_at)) as zendesk_updated_at,
        max(coalesce(zendesk_organization_updated_at, zendesk.organization_updated_at)) as zendesk_organization_updated_at,
        max(coalesce(marketo_created_at, marketo.created_at)) as marketo_created_at,
        max(coalesce(stripe_created_at, stripe.created_at)) as stripe_created_at,
        max(coalesce(zendesk_created_at, zendesk.created_at)) as zendesk_created_at,
        max(coalesce(zendesk_organization_created_at, zendesk.organization_created_at)) as zendesk_organization_created_at

    from source_matches
    full outer join marketo 
        on marketo.lead_id = marketo_lead_id
    full outer join stripe
        on stripe.customer_id = stripe_customer_id
    full outer join zendesk
        on zendesk.user_id = zendesk_user_id

    {{ dbt_utils.group_by(n=4 + match_id_list|length) }}
), 

final as (
    select 
        {{ dbt_utils.generate_surrogate_key(['marketo_lead_id', 'stripe_customer_id', 'zendesk_user_id']) }} as customer360_id,
        *
    from combine_all_results
)

select *
from final