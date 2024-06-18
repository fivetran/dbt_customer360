with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

marketo as (

    select *
    from {{ ref('int_customer360__marketo') }}
    where full_name_clean is not null
),

stripe as (

    select *
    from {{ ref('int_customer360__stripe') }}
    where coalesce(customer_name_clean, shipping_name_clean) is not null
),

zendesk as (

    select *
    from {{ ref('int_customer360__zendesk') }}
    where full_name_clean is not null
),

stripe_names as (

    select 
        customer_id,
        customer_name_clean as full_name,
        {# customer_name_title as title,
        customer_name_suffix as suffix, #}
        'primary' as type
    from stripe
    where customer_name is not null

    union all 

    select 
        customer_id,
        shipping_name_clean as full_name,
        {# shipping_name_title as title,
        shipping_name_suffix as suffix, #}
        'shipping' as type
    from stripe
    where shipping_name is not null
),

unioned as (

    select 
        mapping.customer360_id,
        marketo.full_name_clean as full_name,
        {# name_title as title,
        name_suffix as suffix, #}
        'primary' as type,
        'marketo' as source,
        mapping.marketo_updated_at as updated_at,
        marketo_created_at as created_at

    from mapping
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id

    union all

    select 
        mapping.customer360_id,
        stripe_names.full_name,
        {# title,
        suffix, #}
        stripe_names.type,
        'stripe' as source,
        stripe_updated_at as updated_at,
        stripe_created_at as created_at

    from mapping
    join stripe_names
        on mapping.stripe_customer_id = stripe_names.customer_id

    union all

    select 
        mapping.customer360_id,
        zendesk.full_name_clean as full_name,
        {# name_title as title,
        name_suffix as suffix, #}
        'primary' as type,
        'zendesk' as source,
        mapping.zendesk_updated_at as updated_at,
        zendesk_created_at as created_at

    from mapping
    join zendesk
        on mapping.zendesk_user_id = zendesk.user_id
),

rank_value_confidence as (

    select
        customer360_id,
        full_name,
        {# title,
        suffix, #}
        type,
        source,
        count(*) over (partition by customer360_id, full_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, full_name) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        full_name,
        {# title,
        suffix, #}
        type,
        source,
        dense_rank() over (partition by customer360_id order by (case when lower(full_name) in ('permanently deleted', 'placeholder contact', 'not available') then 0 else 1 end) desc,
                            value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc
        ) as confidence_rank,
        row_number() over (partition by customer360_id order by (case when lower(full_name) in ('permanently deleted', 'placeholder contact', 'not available') then 0 else 1 end) desc, 
                            value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc
        ) as index

    from rank_value_confidence
)

select * 
from final