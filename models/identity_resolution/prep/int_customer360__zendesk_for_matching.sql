{{ config(enabled=var('customer360__using_zendesk', true)) }}

with zendesk as (

    select 
        user_id,
        lower(full_name_clean) as full_name_clean,
        lower(organization_name_no_suffix) as organization_name_no_suffix,
        organization_id,
        email,
        phone,
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , {{ match_set.name }}
            {%- endfor %}
        {% endif %}
        
    from {{ ref('int_customer360__zendesk') }}
),

-- let's try to limit the data used for our mega-joins
zendesk_matching as (

    select *
    from zendesk
    where 
    {%- if var('customer360_internal_match_ids') %}
        {%- for match_set in var('customer360_internal_match_ids') %}
            {{ match_set.name }} is not null or
        {%- endfor %}
    {% endif %}
    (( {{ 'organization_name_no_suffix' if var('customer360_grain_zendesk', 'individual') == 'organization' else 'full_name_clean' }}  is not null or email is not null) -- todo: figure out how to include customer grain
    and (email is not null or phone is not null))
),

final as (

    select 
        user_id,
        full_name_clean,
        organization_name_no_suffix,
        organization_id,
        coalesce(email, 'null_zendesk') as email,
        coalesce(phone, 'null_zendesk') as phone,
        updated_at,
        created_at,
        organization_updated_at,
        organization_created_at
        {%- if var('customer360_internal_match_ids') %}
            {%- for match_set in var('customer360_internal_match_ids') %}
                , coalesce(cast({{ match_set.name }} as {{ dbt.type_string() }}), 'null_zendesk') as {{ match_set.name }}
            {%- endfor %}
        {% endif %}

    from zendesk_matching
)

select *
from final