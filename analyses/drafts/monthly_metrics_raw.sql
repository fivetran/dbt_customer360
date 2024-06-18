with spine as (

    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2023-03-01' as date)",
        end_date="cast('2024-04-01' as date)"
        )
    }}
),

join_sources as (

    select 
        {{ dbt_utils.generate_surrogate_key(['marketo_lead_id', 'stripe_customer_id', 'zendesk_user_id']) }} as unique_customer_id,
        marketo.lead_id as marketo_lead_id,
        stripe.customer_id as stripe_customer_id,
        zendesk.user_id as zendesk_user_id,

        lower(coalesce(marketo.email, stripe.email, zendesk.email)) as email,

        marketo.first_name || ' ' || marketo.last_name as marketo_full_name,
        stripe.customer_name as stripe_customer_full_name,
        stripe.shipping_name as stripe_shipping_full_name,
        zendesk.name as zendesk_full_name,

        marketo.company as marketo_organization_name,
        marketo.inferred_company as marketo_inferred_organization_name,
        stripe.customer_name as stripe_customer_organization_name,
        stripe.shipping_name as stripe_shipping_organization_name,
        zendesk_org.name as zendesk_organization_name,

        marketo.anonymous_ip as ip_address,

        max(marketo.updated_timestamp) as marketo_updated_at,
        -- stripe customer object does not have an updated_at field
        max(zendesk.updated_at) as zendesk_updated_at,
        max(zendesk_org.updated_at) as zendesk_org_updated_at,

        max(marketo.created_timestamp) as marketo_created_at,
        max(stripe.customer_created_at) as stripe_created_at,
        max(zendesk.created_at) as zendesk_created_at,
        max(zendesk_org.created_at) as zendesk_org_created_at

    from (select * from {{ ref('marketo__leads') }} where email is not null) as marketo

    full outer join (select * from {{ ref('stripe__customer_overview') }} where email is not null) as stripe 
        on lower(stripe.email) = lower(marketo.email)

    full outer join (select * from {{ ref('stg_zendesk__user') }} where email is not null and role = 'end-user') as zendesk
        on lower(zendesk.email) = lower(coalesce(marketo.email, stripe.email))

    left join {{ ref('stg_zendesk__organization') }} as zendesk_org 
        on zendesk.organization_id = zendesk_org.organization_id 

    {{ dbt_utils.group_by(n=14) }}
),

union_names as (

    select 
        email,
        marketo_full_name as full_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at
    from join_sources
    where marketo_full_name is not null

    union all 

    select 
        email,
        stripe_customer_full_name as full_name,
        null as stripe_updated_at,
        stripe_created_at
    from join_sources
    where stripe_customer_full_name is not null

    union all 

    select 
        email,
        stripe_shipping_full_name as full_name,
        null as stripe_updated_at,
        stripe_created_at
    from join_sources
    where stripe_shipping_full_name is not null

    union all 

    select 
        email,
        zendesk_full_name as full_name,
        zendesk_updated_at,
        zendesk_created_at
    from join_sources
    where zendesk_full_name is not null
),

rank_names as (

    select
        email,
        full_name,
        count(*) over (partition by email, full_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by email, full_name) as value_last_updated_at

    from union_names
),

choose_names as (
    select * from (
        select
            email,
            full_name,
            dense_rank() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
            row_number() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

        from rank_names
    ) where index = 1
),

union_org_names as (

    select 
        email,
        marketo_organization_name as organization_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at
    from join_sources
    where marketo_organization_name is not null

    union all 

    select 
        email,
        marketo_inferred_organization_name as organization_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at
    from join_sources
    where marketo_organization_name is not null

    union all

    select 
        email,
        stripe_customer_organization_name as organization_name,
        null as stripe_updated_at,
        stripe_created_at
    from join_sources
    where stripe_customer_organization_name is not null

    union all 

    select 
        email,
        stripe_shipping_organization_name as organization_name,
        null as stripe_updated_at,
        stripe_created_at
    from join_sources
    where stripe_shipping_organization_name is not null

    union all 

    select 
        email,
        zendesk_organization_name as organization_name,
        greatest(zendesk_org_updated_at, zendesk_updated_at) as updated_at,
        greatest(zendesk_created_at, zendesk_created_at) as created_at
    from join_sources
    where zendesk_full_name is not null
),

rank_org_names as (

    select
        email,
        organization_name,
        count(*) over (partition by email, organization_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by email, organization_name) as value_last_updated_at

    from union_names
),

choose_org_names as (
    select * from (
        select
            email,
            organization_name,
            dense_rank() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
            row_number() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

        from rank_org_names
    ) where index = 1
),

select 
    spine.date_month,
    join_sources.unique_customer_id,
    join_sources.email,
    choose_names.full_name,
    choose_org_names.organization_name,
    join_sources.ip_address,

    -- stripe 
    sum(balance_transaction_net) as monthly_revenue,

    -- marketo
    sum(count_deliveries) as count_email_deliveries,
    sum(count_opens) as count_email_opens,
    sum(count_clicks) as count_email_clicks,
    count(distinct campaign_id) as count_campaigns_sent,

    -- zendesk
    count(distinct ticket_id) as tickets_opened,
    avg(first_reply_time_calendar_minutes) as avg_first_reply_time_calendar_minutes,
    avg(final_resolution_calendar_minutes) as avg_final_resolution_calendar_minutes,
    avg(requester_wait_time_in_calendar_minutes) as requester_wait_time_in_calendar_minutes
    
from join_sources
left join choose_names
    on choose_names.email = join_sources.email
left join choose_org_names
    on choose_org_names.email = join_sources.email

cross join spine

left join {{ ref('stripe__balance_transactions') }}
    on join_sources.stripe_customer_id = stripe__balance_transactions.customer_id
    and spine.date_month = date_trunc(cast(stripe__balance_transactions.balance_transaction_created_at as date), month)

left join {{ ref('marketo__email_sends') }}
    on join_sources.marketo_lead_id = marketo__email_sends.lead_id 
    and spine.date_month = date_trunc(cast(marketo__email_sends.activity_timestamp as date), month)

left join {{ ref('zendesk__ticket_metrics') }}
    on join_sources.zendesk_user_id = zendesk__ticket_metrics.submitter_id 
    and spine.date_month = date_trunc(cast(zendesk__ticket_metrics.created_at as date), month)

where unique_customer_id in ('5712f93342119125086fad679f570b15', '40e857a025b4b95e65921c07412f7e74') --'d07dceb993152e965791960a997d2c0d')
group by 1,2,3,4,5,6
order by 2 desc,1 desc