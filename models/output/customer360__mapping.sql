with source_matches as (

    select *
    from {{ ref('int_customer360__source_matches') }}
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
{% endif %}

{% set match_id_list = [] -%}

combine_all_results as (

    select
        {% if var('customer360__using_marketo', true) -%} 
            coalesce(marketo_lead_id, marketo.lead_id) as marketo_lead_id
            , marketo.organization_name_no_suffix as marketo_organization_name
            , marketo.inferred_organization_name_no_suffix as marketo_inferred_organization_name
        {%- endif %}

        {% if var('customer360__using_stripe', true) -%}
            {% if var('customer360__using_marketo', true) -%},{%- endif -%} coalesce(stripe_customer_id, stripe.customer_id) as stripe_customer_id
            , stripe.customer_organization_name_no_suffix as stripe_customer_organization_name
            , stripe.shipping_organization_name_no_suffix as stripe_shipping_organization_name
        {%- endif %}

        {% if var('customer360__using_zendesk', true) -%} 
            , coalesce(zendesk_user_id, zendesk.user_id) as zendesk_user_id
            , zendesk.organization_id as zendesk_organization_id
            , zendesk.organization_name as zendesk_organization_name
        {%- endif %}
        
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , coalesce(
                    source_matches.{{ match_set.name }}
                    {%- if var('customer360__using_marketo', true) -%}, marketo.{{ match_set.name }} {%- endif -%}
                    {%- if var('customer360__using_stripe', true) -%}, stripe.{{ match_set.name }} {%- endif -%}
                    {%- if var('customer360__using_zendesk', true) -%}, zendesk.{{ match_set.name }} {%- endif -%}
                ) as {{ match_set.name }}
                {% do match_id_list.append(match_set.name) -%}
            {%- endfor %}
        {% endif %}

        {% if var('customer360__using_marketo', true) -%}
        , max(coalesce(marketo_updated_at, marketo.updated_at)) as marketo_updated_at
        , max(coalesce(marketo_created_at, marketo.created_at)) as marketo_created_at
        {%- endif %}

        {% if var('customer360__using_stripe', true) -%}
        , max(coalesce(stripe_updated_at, stripe.updated_at)) as stripe_updated_at
        , max(coalesce(stripe_created_at, stripe.created_at)) as stripe_created_at
        {%- endif %}

        {% if var('customer360__using_zendesk', true) -%}
        , max(coalesce(zendesk_updated_at, zendesk.updated_at)) as zendesk_updated_at
        , max(coalesce(zendesk_organization_updated_at, zendesk.organization_updated_at)) as zendesk_organization_updated_at
        , max(coalesce(zendesk_created_at, zendesk.created_at)) as zendesk_created_at
        , max(coalesce(zendesk_organization_created_at, zendesk.organization_created_at)) as zendesk_organization_created_at
        {% endif %}

    from source_matches

    {% if var('customer360__using_marketo', true) %}
    full outer join marketo 
        on marketo.lead_id = marketo_lead_id
    {% endif %}

    {% if var('customer360__using_stripe', true) %}
    full outer join stripe
        on stripe.customer_id = stripe_customer_id
    {% endif %}

    {% if var('customer360__using_zendesk', true) %}
    full outer join zendesk
        on zendesk.user_id = zendesk_user_id
    {% endif %}

    {{ dbt_utils.group_by(n=(3 if var('customer360__using_marketo', true) else 0) + (3 if var('customer360__using_stripe', true) else 0) + (3 if var('customer360__using_zendesk', true) else 0) + match_id_list|length) }}
), 

batch_organizations as (
    
    select
        *,
        {{ dbt_utils.generate_surrogate_key( (['marketo_organization_name', 'marketo_inferred_organization_name'] if var('customer360__using_marketo', true) else []) + (['stripe_customer_organization_name', 'stripe_shipping_organization_name'] if var('customer360__using_stripe', true) else []) + (['zendesk_organization_id'] if var('customer360__using_zendesk', true) else []) ) }} as customer360_organization_id

    from combine_all_results

),

create_org_header_row as (

    select 
        customer360_organization_id,
        {%- if var('customer360__using_marketo', true) -%} marketo_lead_id,{%- endif -%}
        {%- if var('customer360__using_stripe', true) -%} stripe_customer_id,{%- endif -%}
        {%- if var('customer360__using_zendesk', true) -%} zendesk_user_id,{%- endif -%}
        false as is_organization_header,
        {{ match_id_list | join(', ') }},
        '{' || 
            {%- if var('customer360__using_marketo', true) -%}'"marketo":"' || coalesce(cast(marketo_lead_id as {{ dbt.type_string() }}), '') || '",' || {%- endif -%}
            {%- if var('customer360__using_stripe', true) -%} '"stripe":"' || coalesce(stripe_customer_id, '') || '"' {% if var('customer360__using_zendesk', true) -%} || ',' || {%- endif -%}{%- endif -%}
            {% if var('customer360__using_zendesk', true) -%} '"zendesk":"' || coalesce(cast(zendesk_user_id as {{ dbt.type_string() }}), '') || '"' {%- endif -%} 
        || '}' as source_ids

        {%- if var('customer360__using_marketo', true) -%}
        , marketo_updated_at
        , marketo_created_at
        {% endif %}

        {%- if var('customer360__using_stripe', true) -%}
        , stripe_updated_at
        , stripe_created_at
        {% endif %}

        {%- if var('customer360__using_zendesk', true) -%}
        , zendesk_updated_at
        , zendesk_organization_updated_at
        , zendesk_created_at
        , zendesk_organization_created_at
        {% endif %}

    from batch_organizations

    union all

    select 
        customer360_organization_id,
        {%- if var('customer360__using_marketo', true) -%} cast(null as {{ dbt.type_int() }}) as marketo_lead_id,{%- endif -%}
        {%- if var('customer360__using_stripe', true) -%} cast(null as {{ dbt.type_string() }}) as stripe_customer_id,{%- endif -%}
        {%- if var('customer360__using_zendesk', true) -%} cast(null as {{ dbt.type_int() }}) as zendesk_user_id,{%- endif -%}
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

        '{' || 
        {%- if var('customer360__using_marketo', true) -%}
            '"marketo":' || '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || coalesce(cast(marketo_lead_id as " ~ dbt.type_string() ~ "), '') || '\"'", delimiter="','") }} || '],' || 
        {%- endif -%}
        {%- if var('customer360__using_stripe', true) -%}
            '"stripe":' || '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || coalesce(stripe_customer_id, '') || '\"'", delimiter="','") }} || ']' {%- if var('customer360__using_zendesk', true) -%} || ',' || {%- endif -%}
        {%- endif -%}
        {%- if var('customer360__using_zendesk', true) -%}
            '"zendesk":' || '[' || {{ fivetran_utils.string_agg(field_to_agg="distinct '\"' || coalesce(cast(zendesk_user_id as " ~ dbt.type_string() ~ "), '') || '\"'", delimiter="','") }} || ']'
        {%- endif -%} 
        || '}' as source_ids

        {%- if var('customer360__using_marketo', true) -%}
        , max(marketo_updated_at) as marketo_updated_at
        , min(marketo_created_at) as marketo_created_at
        {% endif %}

        {%- if var('customer360__using_stripe', true) -%}
        , max(stripe_updated_at) as stripe_updated_at
        , min(stripe_created_at) as stripe_created_at
        {% endif %}

        {%- if var('customer360__using_zendesk', true) -%}
        , min(zendesk_created_at) as zendesk_created_at
        , max(zendesk_updated_at) as zendesk_updated_at
        , max(zendesk_organization_updated_at) as zendesk_organization_updated_at
        , min(zendesk_organization_created_at) as zendesk_organization_created_at
        {% endif %}

    from batch_organizations
    {{ dbt_utils.group_by(n=2 + (1 if var('customer360__using_marketo', true) else 0) + (1 if var('customer360__using_stripe', true) else 0) + (1 if var('customer360__using_zendesk', true) else 0) + match_id_list|length - individual_match_id_list|length) }}
),

final as (

    select 
        {{ dbt_utils.generate_surrogate_key((['marketo_lead_id'] if var('customer360__using_marketo', true) else []) + (['stripe_customer_id'] if var('customer360__using_stripe', true) else []) + (['zendesk_user_id'] if var('customer360__using_zendesk', true) else []) + ['is_organization_header', 'customer360_organization_id'] + match_id_list) }} as customer360_id,
        *
    from create_org_header_row
)

select *
from final