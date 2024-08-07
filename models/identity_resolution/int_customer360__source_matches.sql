with 
{% if var('customer360__using_marketo', true) %}
marketo as (

    select *
    from {{ ref('int_customer360__marketo_for_matching') }}
{%- endif %}

{% if var('customer360__using_stripe', true) %}
    {%- if var('customer360__using_marketo', true) -%} ), {%- endif -%}
stripe as (

    select *
    from {{ ref('int_customer360__stripe_for_matching') }}
{%- endif %}

{% if var('customer360__using_zendesk', true) %}
), zendesk as (

    select *
    from {{ ref('int_customer360__zendesk_for_matching') }}
{%- endif %}

{%- set match_id_list = [] -%}

{% if var('customer360__using_marketo', true) and var('customer360__using_stripe', true) %}
), marketo_join_stripe as (

    select 
        marketo.lead_id as marketo_lead_id,
        stripe.customer_id as stripe_customer_id,
        marketo.email as marketo_email,
        stripe.email as stripe_email,
        marketo.full_name_clean as marketo_full_name,
        stripe.customer_name_clean as stripe_full_customer_name,
        stripe.shipping_name_clean as stripe_full_shipping_name,
        marketo.organization_name_no_suffix as marketo_organization_name,
        marketo.inferred_organization_name_no_suffix as marketo_inferred_organization_name,
        stripe.customer_organization_name_no_suffix as stripe_customer_organization_name,
        stripe.shipping_organization_name_no_suffix as stripe_shipping_organization_name,
        marketo.updated_at as marketo_updated_at,
        stripe.updated_at as stripe_updated_at,
        marketo.created_at as marketo_created_at,
        stripe.created_at as stripe_created_at

        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            , case when marketo.{{ match_set.name }} = 'null_marketo' then null else marketo.{{ match_set.name }} end as marketo_{{ match_set.name }}
            , case when stripe.{{ match_set.name }}  = 'null_stripe' then null else stripe.{{ match_set.name }} end as stripe_{{ match_set.name }}

            {% do match_id_list.append(match_set.name) -%}

            {%- endfor %}
        {%- endif %}

    from marketo join stripe
    on
        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            marketo.{{ match_set.name }} = stripe.{{ match_set.name }} or
            {%- endfor %}
        {%- endif %}

        -- rule 1: exact email
        marketo.email = stripe.email
        
        -- rule 2: exact phone (will pair with exact email or fuzzy name)
        or marketo.phone = stripe.phone 
        or marketo.company_phone = stripe.phone
        or marketo.mobile_phone = stripe.phone
        or marketo.phone = stripe.shipping_phone 
        or marketo.company_phone = stripe.shipping_phone
        or marketo.mobile_phone = stripe.shipping_phone

        -- rule 3: exact partial address (will pair with exact email or fuzzy name)
        or (
            ( -- address lines
                ---- marketo lead address <> stripe customer address
                marketo.address_line_1_long = stripe.customer_address_line_1_long or marketo.address_line_1_long = stripe.customer_address_line_2_long
                ---- marketo billing address <> stripe customer address
                or marketo.billing_address_line_1_long = stripe.customer_address_line_1_long or marketo.billing_address_line_1_long = stripe.customer_address_line_2_long
                ---- marketo lead address <> stripe shipping address
                or marketo.address_line_1_long = stripe.shipping_address_line_1_long or marketo.address_line_1_long = stripe.shipping_address_line_2_long
                ---- marketo billing address <> stripe shipping address
                or marketo.billing_address_line_1_long = stripe.shipping_address_line_1_long or marketo.billing_address_line_1_long = stripe.shipping_address_line_2_long
            )
            and (
                -- city + state
                ---- marketo lead address <> stripe customer address
                (marketo.city = stripe.customer_city
                and (marketo.state_long = stripe.customer_state_long or marketo.state_code = stripe.customer_state_code)) 
                ---- marketo billing address <> stripe customer address
                or (marketo.billing_city = stripe.customer_city
                and (marketo.billing_state_long = stripe.customer_state_long or marketo.billing_state_code = stripe.customer_state_code)) 
                ---- marketo lead address <> stripe shipping address
                or (marketo.city = stripe.shipping_city 
                and (marketo.state_long = stripe.shipping_state_long or marketo.state_code = stripe.shipping_state_code))
                ---- marketo billing address <> stripe shipping address
                or (marketo.billing_city = stripe.shipping_city
                and (marketo.billing_state_long = stripe.shipping_state_long or marketo.billing_state_code = stripe.shipping_state_code))

                -- zipcode + country
                ---- marketo lead address <> stripe customer address
                or (marketo.postal_code = stripe.customer_postal_code 
                ---- marketo comes with longform names (stripe comes with codes that we then use to join in longform names) so let's compare primary and alternate country names for marketo
                and (marketo.country_long = stripe.customer_country_long or marketo.country_long = stripe.customer_country_long_alt or marketo.country_code = stripe.customer_country_code))
                ---- marketo billing address <> stripe customer address
                or (marketo.billing_postal_code = stripe.customer_postal_code 
                and (marketo.billing_country_long = stripe.customer_country_long or marketo.billing_country_long = stripe.customer_country_long_alt or marketo.billing_country_code = stripe.customer_country_code))
                ---- marketo lead address <> stripe shipping address
                or (marketo.postal_code = stripe.shipping_postal_code 
                and (marketo.country_long = stripe.shipping_country_long or marketo.country_long = stripe.shipping_country_long_alt or marketo.country_code = stripe.shipping_country_code))
                ---- marketo billing address <> stripe shipping address
                or (marketo.billing_postal_code = stripe.shipping_postal_code 
                and (marketo.billing_country_long = stripe.shipping_country_long or marketo.billing_country_long = stripe.shipping_country_long_alt or marketo.billing_country_code = stripe.shipping_country_code))
            )
        )
), marketo_stripe_filtered as (

    select 
        *
        {% if not var('customer360__using_zendesk', true) %}
        {# if we are using zendesk as well, this coalesce happens in a later CTE where we join everything together #}
            {%- if var('customer360_internal_match_ids', []) != [] %}
                {%- for match_set in var('customer360_internal_match_ids', []) %}
                , coalesce(marketo_{{ match_set.name }}, stripe_{{ match_set.name }}) as {{ match_set.name }}
                {%- endfor %}
            {%- endif %}
        {%- endif %}

    from marketo_join_stripe

    where
        {% if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
                {%- if match_set.customer_grain == 'individual' or var('customer360_grain_stripe', 'individual') == 'organization' or var('customer360_grain_marketo', 'individual') == 'organization' %}
                    marketo_{{ match_set.name }} = stripe_{{ match_set.name }} or 
                {% endif -%}
            {%- endfor %}
        {% endif -%}
        
        {# we'll also use these in the marketo_zendesk_filtered and stripe_zendesk_filtered CTEs #}
        {% set marketo_names = ['marketo_organization_name', 'marketo_inferred_organization_name'] if var('customer360_grain_marketo', 'individual') == 'organization' else ['marketo_full_name'] %}
        {% set stripe_names = ['stripe_customer_organization_name', 'stripe_shipping_organization_name'] if var('customer360_grain_stripe', 'individual') == 'organization' else ['stripe_full_customer_name', 'stripe_full_shipping_name'] %}

        marketo_email = stripe_email
        {# we want to compare each marketo name to each stripe one #}
        {% for marketo_name in marketo_names %}
            {% for stripe_name in stripe_names %}
                {# coalesce to completely different strings to avoid false positives #}
                or {{ levenshtein_distance("coalesce(lower(" ~ marketo_name ~ "), 'aaa')", "coalesce(lower(" ~ stripe_name ~ "), 'bbb')") }} >= .95
            {% endfor %}
        {% endfor %}
{%- endif %}

{% if var('customer360__using_marketo', true) and var('customer360__using_zendesk', true) %}
), marketo_join_zendesk as (
    
    select 
        marketo.lead_id as marketo_lead_id,
        zendesk.user_id as zendesk_user_id,
        marketo.email as marketo_email,
        zendesk.email as zendesk_email,
        marketo.full_name_clean as marketo_full_name,
        zendesk.full_name_clean as zendesk_full_name,
        marketo.organization_name_no_suffix as marketo_organization_name,
        marketo.inferred_organization_name_no_suffix as marketo_inferred_organization_name,
        zendesk.organization_name_no_suffix as zendesk_organization_name,
        zendesk.organization_id as zendesk_organization_id,
        marketo.updated_at as marketo_updated_at,
        zendesk.updated_at as zendesk_updated_at,
        zendesk.organization_updated_at as zendesk_organization_updated_at,
        marketo.created_at as marketo_created_at,
        zendesk.created_at as zendesk_created_at,
        zendesk.organization_created_at as zendesk_organization_created_at

        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            , case when marketo.{{ match_set.name }} = 'null_marketo' then null else marketo.{{ match_set.name }} end as marketo_{{ match_set.name }}
            , case when zendesk.{{ match_set.name }} = 'null_zendesk' then null else zendesk.{{ match_set.name }} end as zendesk_{{ match_set.name }}
            {%- endfor %}
        {%- endif %}

    from marketo join zendesk
    on 
        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            marketo.{{ match_set.name }} = zendesk.{{ match_set.name }} or
            {%- endfor %}
        {%- endif %}
        -- rule 1: exact email 
        marketo.email = zendesk.email

        -- rule 2: exact phone (will pair with exact email or fuzzy name)
        or marketo.phone = zendesk.phone 
        or marketo.company_phone = zendesk.phone
        or marketo.mobile_phone = zendesk.phone

        -- no rule 3 since zendesk doesn't have an address

), marketo_zendesk_filtered as (

    select 
        *
        {% if not var('customer360__using_stripe', true) %}
        {# if we are using stripe as well, this coalesce happens in a later CTE where we join everything together #}
            {%- if var('customer360_internal_match_ids', []) != [] %}
                {%- for match_set in var('customer360_internal_match_ids', []) %}
                , coalesce(zendesk_{{ match_set.name }}, marketo_{{ match_set.name }}) as {{ match_set.name }}
                {%- endfor %}
            {%- endif %}
        {%- endif %}

    from marketo_join_zendesk

    where
        {% if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
                {%- if match_set.customer_grain == 'individual' or var('customer360_grain_stripe', 'individual') == 'organization' or var('customer360_grain_marketo', 'individual') == 'organization' %}
                marketo_{{ match_set.name }} = zendesk_{{ match_set.name }} or 
                {% endif -%}
            {%- endfor %}
        {% endif -%}

        {% set zendesk_names = ['zendesk_organization_name'] if var('customer360_grain_zendesk', 'individual') == 'organization' else ['zendesk_full_name'] %}
        {# we already set marketo_names in marketo_stripe_filtered CTE #}

        marketo_email = zendesk_email
        {# we want to compare each marketo name to the zendesk name #}
        {% for marketo_name in marketo_names %}
            {% for zendesk_name in zendesk_names %}
                {# coalesce to completely different strings to avoid false positives #}
                or {{ levenshtein_distance("coalesce(lower(" ~ marketo_name ~ "), 'aaa')", "coalesce(lower(" ~ zendesk_name ~ "), 'bbb')") }} >= .95
            {% endfor %}
        {% endfor %}
{%- endif %}

{% if var('customer360__using_stripe', true) and var('customer360__using_zendesk', true) %}
), stripe_join_zendesk as (

    select 
        stripe.customer_id as stripe_customer_id,
        zendesk.user_id as zendesk_user_id,
        stripe.email as stripe_email,
        zendesk.email as zendesk_email,
        stripe.customer_name_clean as stripe_full_customer_name,
        stripe.shipping_name_clean as stripe_full_shipping_name,
        zendesk.full_name_clean as zendesk_full_name,
        stripe.customer_organization_name_no_suffix as stripe_customer_organization_name,
        stripe.shipping_organization_name_no_suffix as stripe_shipping_organization_name,
        zendesk.organization_name_no_suffix as zendesk_organization_name,
        zendesk.organization_id as zendesk_organization_id,
        stripe.updated_at as stripe_updated_at,
        zendesk.updated_at as zendesk_updated_at,
        zendesk.organization_updated_at as zendesk_organization_updated_at,
        stripe.created_at as stripe_created_at,
        zendesk.created_at as zendesk_created_at,
        zendesk.organization_created_at as zendesk_organization_created_at
        {% if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            , case when stripe.{{ match_set.name }} = 'null_stripe' then null else stripe.{{ match_set.name }} end as stripe_{{ match_set.name }}
            , case when zendesk.{{ match_set.name }} = 'null_zendesk' then null else zendesk.{{ match_set.name }} end as zendesk_{{ match_set.name }}
            {%- endfor %}
        {%- endif %}

    from stripe
    join zendesk
    on 
        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            stripe.{{ match_set.name }} = zendesk.{{ match_set.name }} or
            {%- endfor %}
        {%- endif %}

        -- rule 1: exact email
        stripe.email = zendesk.email
        
        -- rule 2: exact phone (will pair with exact email or fuzzy name)
        or stripe.phone = zendesk.phone 
        or stripe.shipping_phone = zendesk.phone

        -- no rule 3 since zendesk doesn't have an address

), stripe_zendesk_filtered as (

    select 
        *
        {% if not var('customer360__using_marketo', true) %}
        {# if we are using marketo as well, this coalesce happens in a later CTE where we join everything together #}
            {%- if var('customer360_internal_match_ids', []) != [] %}
                {%- for match_set in var('customer360_internal_match_ids', []) %}
                , coalesce(zendesk_{{ match_set.name }}, stripe_{{ match_set.name }}) as {{ match_set.name }}
                {%- endfor %}
            {%- endif %}
        {%- endif %}

    from stripe_join_zendesk
    
    where
        {% if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
                {%- if match_set.customer_grain == 'individual' or var('customer360_grain_stripe', 'individual') == 'organization' or var('customer360_grain_marketo', 'individual') == 'organization' %}
                stripe_{{ match_set.name }} = zendesk_{{ match_set.name }} or
                {% endif -%}
            {%- endfor %}
        {% endif -%}
        
        stripe_email = zendesk_email
        {# we want to compare each zendesk name to each stripe one #}
        {% for zendesk_name in zendesk_names %}
            {% for stripe_name in stripe_names %}
                {# coalesce to completely different strings to avoid false positives #}
                or {{ levenshtein_distance("coalesce(lower(" ~ zendesk_name ~ "), 'aaa')", "coalesce(lower(" ~ stripe_name ~ "), 'bbb')") }} >= .95
            {% endfor %}
        {% endfor %}
{%- endif -%}

{% if var('customer360__using_marketo', true) and var('customer360__using_stripe', true) and var('customer360__using_zendesk', true) %}
), combine_joins as (

    select 
        coalesce(marketo_stripe_filtered.marketo_lead_id, marketo_zendesk_filtered.marketo_lead_id) as marketo_lead_id,
        coalesce(marketo_stripe_filtered.stripe_customer_id, stripe_zendesk_filtered_join_stripe.stripe_customer_id, stripe_zendesk_filtered_join_zendesk.stripe_customer_id) as stripe_customer_id,
        coalesce(marketo_zendesk_filtered.zendesk_user_id, stripe_zendesk_filtered_join_zendesk.zendesk_user_id, stripe_zendesk_filtered_join_stripe.zendesk_user_id) as zendesk_user_id,

        {%- if var('customer360_internal_match_ids', []) != [] %}
            {%- for match_set in var('customer360_internal_match_ids', []) %}
            coalesce(marketo_zendesk_filtered.zendesk_{{ match_set.name }}, marketo_zendesk_filtered.marketo_{{ match_set.name }}, 
                marketo_stripe_filtered.marketo_{{ match_set.name }}, marketo_stripe_filtered.stripe_{{ match_set.name }}, 
                stripe_zendesk_filtered_join_stripe.stripe_{{ match_set.name }}, stripe_zendesk_filtered_join_stripe.zendesk_{{ match_set.name }}, 
                stripe_zendesk_filtered_join_zendesk.stripe_{{ match_set.name }}, stripe_zendesk_filtered_join_zendesk.zendesk_{{ match_set.name }}
                ) as {{ match_set.name }},
            {%- endfor %}
        {%- endif %}

        max(coalesce(marketo_zendesk_filtered.marketo_updated_at, marketo_stripe_filtered.marketo_updated_at)) as marketo_updated_at,
        max(coalesce(marketo_stripe_filtered.stripe_updated_at, stripe_zendesk_filtered_join_stripe.stripe_updated_at, stripe_zendesk_filtered_join_zendesk.stripe_updated_at)) as stripe_updated_at,
        max(coalesce(marketo_zendesk_filtered.zendesk_updated_at, stripe_zendesk_filtered_join_zendesk.zendesk_updated_at, stripe_zendesk_filtered_join_stripe.zendesk_updated_at)) as zendesk_updated_at,
        max(coalesce(marketo_zendesk_filtered.zendesk_organization_updated_at, stripe_zendesk_filtered_join_zendesk.zendesk_organization_updated_at, stripe_zendesk_filtered_join_stripe.zendesk_organization_updated_at)) as zendesk_organization_updated_at,
        max(coalesce(marketo_zendesk_filtered.marketo_created_at, marketo_stripe_filtered.marketo_created_at)) as marketo_created_at,
        max(coalesce(marketo_stripe_filtered.stripe_created_at, stripe_zendesk_filtered_join_stripe.stripe_created_at, stripe_zendesk_filtered_join_zendesk.stripe_created_at)) as stripe_created_at,
        max(coalesce(marketo_zendesk_filtered.zendesk_created_at, stripe_zendesk_filtered_join_zendesk.zendesk_created_at, stripe_zendesk_filtered_join_stripe.zendesk_created_at)) as zendesk_created_at,
        max(coalesce(marketo_zendesk_filtered.zendesk_organization_created_at, stripe_zendesk_filtered_join_zendesk.zendesk_organization_created_at, stripe_zendesk_filtered_join_stripe.zendesk_organization_created_at)) as zendesk_organization_created_at

    from marketo_zendesk_filtered -- prioritize this because it'll be the lowest grain likely
    full outer join marketo_stripe_filtered 
        on marketo_stripe_filtered.marketo_lead_id = marketo_zendesk_filtered.marketo_lead_id
    full outer join stripe_zendesk_filtered as stripe_zendesk_filtered_join_stripe
        on marketo_stripe_filtered.stripe_customer_id = stripe_zendesk_filtered_join_stripe.stripe_customer_id
    left join stripe_zendesk_filtered as stripe_zendesk_filtered_join_zendesk
        on marketo_zendesk_filtered.zendesk_user_id = stripe_zendesk_filtered_join_zendesk.zendesk_user_id

    {{ dbt_utils.group_by(n=3 + match_id_list|length) }}
)

select * from combine_joins

{% elif var('customer360__using_zendesk', true) and var('customer360__using_stripe', true) and not var('customer360__using_marketo', true) %}
{# Just Zendesk + Stripe #}
) select * from stripe_zendesk_filtered

{% elif var('customer360__using_marketo', true) and var('customer360__using_zendesk', true) and not var('customer360__using_stripe', true) %}
{# Just Zendesk + Marketo #}
) select * from marketo_zendesk_filtered

{% elif var('customer360__using_marketo', true) and var('customer360__using_stripe', true) and not var('customer360__using_zendesk', true) %}
{# Just Marketo + Stripe #}
) select * from marketo_stripe_filtered

{% else %}
{# 1 or none -- use the individual platform dbt packages #}
) select null limit 0
{{ exceptions.raise_compiler_error("Invalid configs. To use the Customer360 Data Model, you must use at least 2 out of the 3 supported sources (Zendesk, Marketo, Stripe).") }}

{% endif -%}