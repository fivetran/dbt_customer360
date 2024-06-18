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

marketo_phones as (

    select 
        lead_id,
        phone,
        'primary' as type
    from marketo
    where phone is not null

    union all 

    select 
        lead_id,
        main_phone as phone,
        'company' as type
    from marketo
    where main_phone is not null

    union all
    
    select 
        lead_id,
        mobile_phone as phone,
        'mobile' as type
    from marketo
    where mobile_phone is not null
),

stripe_phones as (

    select 
        customer_id,
        phone,
        'primary' as type
    from stripe
    where phone is not null

    union all 

    select 
        customer_id,
        shipping_phone as phone,
        'shipping' as type
    from stripe
    where shipping_phone is not null
),

zendesk_phones as (

    select
        user_id,
        phone,
        'primary' as type
    from zendesk
    where phone is not null
),

unioned as (

    select 
        mapping.customer360_id,
        marketo_phones.phone,
        marketo_phones.type,
        'marketo' as source,
        mapping.marketo_updated_at as updated_at,
        marketo_created_at as created_at

    from mapping
    join marketo_phones
        on mapping.marketo_lead_id = marketo_phones.lead_id

    union all

    select 
        mapping.customer360_id,
        stripe_phones.phone,
        stripe_phones.type,
        'stripe' as source,
        stripe_updated_at as updated_at,
        stripe_created_at as created_at

    from mapping
    join stripe_phones
        on mapping.stripe_customer_id = stripe_phones.customer_id

    union all

    select 
        mapping.customer360_id,
        zendesk_phones.phone,
        zendesk_phones.type,
        'zendesk' as source,
        mapping.zendesk_updated_at as updated_at,
        zendesk_created_at as created_at

    from mapping
    join zendesk_phones
        on mapping.zendesk_user_id = zendesk_phones.user_id
),

rank_value_confidence as (

    select
        customer360_id,
        phone,
        type,
        source,
        count(*) over (partition by customer360_id, phone) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, phone) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        phone,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_value_confidence
)

select * 
from final