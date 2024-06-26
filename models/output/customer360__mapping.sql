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
        marketo.organization_name_no_suffix as marketo_organization_name,
        marketo.inferred_organization_name_no_suffix as marketo_inferred_organization_name,
        stripe.customer_organization_name_no_suffix as stripe_customer_organization_name,
        stripe.shipping_organization_name_no_suffix as stripe_shipping_organization_name,
        zendesk.organization_name as zendesk_organization_name,

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

    {{ dbt_utils.group_by(n=9 + match_id_list|length) }}
), 

batch_organizations as (
    
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['marketo_organization_name', 'marketo_inferred_organization_name', 'stripe_customer_organization_name', 'stripe_shipping_organization_name', 'zendesk_organization_id']) }} as customer360_organization_id

    from combine_all_results

),

create_org_header_row as (

    select 
        customer360_organization_id,
        marketo_lead_id,
        stripe_customer_id,
        zendesk_user_id,
        false as is_organization_header,
        {{ match_id_list | join(', ') }},
        '{"marketo":"' || marketo_lead_id || '","stripe":"' || stripe_customer_id || '","zendesk":"' || zendesk_user_id || '"}' as source_ids,
        marketo_updated_at,
        stripe_updated_at,
        zendesk_updated_at,
        zendesk_organization_updated_at,
        marketo_created_at,
        stripe_created_at,
        zendesk_created_at,
        zendesk_organization_created_at

    from batch_organizations

    union all

    select 
        customer360_organization_id,
        cast(null as {{ dbt.type_int() }}) as marketo_lead_id,
        cast(null as {{ dbt.type_string() }}) as stripe_customer_id,
        cast(null as {{ dbt.type_int() }}) as zendesk_user_id,
        true as is_organization_header,

        {% set individual_match_id_list = [] -%}

        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
                {%- if match_set.customer_grain == 'organization' or get_highest_common_grain() == 'individual' %}
                    {{ match_set.name }},
                {% else %}
                    '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || cast(" ~ match_set.name ~ " as " ~ dbt.type_string() ~ ") || '\"'", delimiter="','") }} || ']' as {{ match_set.name }},
                    {% do individual_match_id_list.append(match_set.name) -%}
                {% endif %}
            {%- endfor %}
        {% endif %}

        '{"marketo":' || '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || cast(marketo_lead_id as " ~ dbt.type_string() ~ ") || '\"'", delimiter="','") }} || ']' || 
            ',"stripe":' || '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || stripe_customer_id || '\"'", delimiter="','") }} || ']' || 
            ',"zendesk":' || '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || cast(zendesk_user_id as " ~ dbt.type_string() ~ ") || '\"'", delimiter="','") }} || ']}' as source_ids,

        max(marketo_updated_at) as marketo_updated_at,
        max(stripe_updated_at) as stripe_updated_at,
        max(zendesk_updated_at) as zendesk_updated_at,
        max(zendesk_organization_updated_at) as zendesk_organization_updated_at,
        min(marketo_created_at) as marketo_created_at,
        min(stripe_created_at) as stripe_created_at,
        min(zendesk_created_at) as zendesk_created_at,
        min(zendesk_organization_created_at) as zendesk_organization_created_at

    from batch_organizations
    {{ dbt_utils.group_by(n=5 + match_id_list|length - individual_match_id_list|length) }}
),

final as (

    select 
        {{ dbt_utils.generate_surrogate_key(['marketo_lead_id', 'stripe_customer_id', 'zendesk_user_id', 'is_organization_header', 'customer360_organization_id'] + match_id_list) }} as customer360_id,
        *
    from create_org_header_row
)

select *
from final