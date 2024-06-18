with marketo as (

    select *
    from {{ ref('marketo__leads') }}
    where email is not null
),

stripe as (

    select *
    from {{ ref('stripe__customer_overview') }}
    where email is not null
),

zendesk as (

    select *
    from {{ ref('stg_zendesk__user') }}

    where role = 'end-user'
    and email is not null
),

joined as (

    select 
        coalesce(marketo.email, stripe.email, zendesk.email) as email,
        marketo.lead_id as marketo_lead_id,
        stripe.customer_id as stripe_customer_id,
        zendesk.user_id as zendesk_user_id,
        max(marketo.updated_timestamp) as marketo_updated_at,
        -- stripe customer object does not have an updated_at field
        max(zendesk.updated_at) as zendesk_updated_at,
        max(marketo.created_timestamp) as marketo_created_at,
        max(stripe.customer_created_at) as stripe_created_at,
        max(zendesk.created_at) as zendesk_created_at

    from marketo 

    full outer join stripe 
        on lower(stripe.email) = lower(marketo.email)

    full outer join zendesk
        on lower(zendesk.email) = lower(coalesce(marketo.email, stripe.email))

    group by 1,2,3,4
),

final as (

    select 
        {{ dbt_utils.generate_surrogate_key(['marketo_lead_id', 'stripe_customer_id', 'zendesk_user_id']) }} as customer360_id,
        email,
        marketo_lead_id,
        stripe_customer_id,
        zendesk_user_id,
        marketo_updated_at,
        cast(null as {{ dbt.type_timestamp() }}) as stripe_updated_at,
        zendesk_updated_at,
        marketo_created_at,
        stripe_created_at,
        zendesk_created_at
        
    from joined
)

select *
from final