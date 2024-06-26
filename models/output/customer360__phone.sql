with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
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

marketo_phones as (

    select 
        lead_id,
        phone,
        phone_extension as extension,
        'primary' as type
    from marketo
    where phone is not null

    union all 

    select 
        lead_id,
        company_phone as phone,
        company_phone_extension as extension,
        'company' as type
    from marketo
    where company_phone is not null

    union all
    
    select 
        lead_id,
        mobile_phone as phone,
        mobile_phone_extension as extension,
        'mobile' as type
    from marketo
    where mobile_phone is not null
),

stripe_phones as (

    select 
        customer_id,
        phone,
        phone_extension as extension,
        'primary' as type
    from stripe
    where phone is not null

    union all 

    select 
        customer_id,
        shipping_phone as phone,
        shipping_phone_extension as extension,
        'shipping' as type
    from stripe
    where shipping_phone is not null
),

zendesk_phones as (

    select
        user_id,
        phone,
        phone_extension as extension,
        'primary' as type
    from zendesk
    where phone is not null
),

unioned as (

    select 
        mapping.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        marketo_phones.phone,
        marketo_phones.extension,
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
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        stripe_phones.phone,
        stripe_phones.extension,
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
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        zendesk_phones.phone,
        zendesk_phones.extension,
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
        customer360_organization_id,
        is_organization_header,
        phone,
        extension,
        type,
        source,
        count(*) over (partition by customer360_id, phone, extension) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by customer360_id, phone, extension) as value_last_updated_at

    from unioned
),

final as (

    select
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        phone,
        extension,
        type,
        source,
        dense_rank() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by customer360_id order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_value_confidence
)

select * 
from final