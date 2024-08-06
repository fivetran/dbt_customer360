{# 
I am a Fivetran user who run’s a SaaS company. I would like to send email campaigns to customers that fall into the following 3 segments: 

1. Customers who have more than 100k in yearly revenue and create more than 10 support tickets a year
2. Customers who have between 1k and 100k in yearly revenue and create more than 10 support tickets a year
3. Customers who have less than 1k in yearly revenue and create more than 10 support tickets a year
#}

with zendesk as (

    select 
        stg_zendesk__user.*,
        stg_zendesk__organization.name as organization_name

    from {{ ref('stg_zendesk__user') }}
    left join {{ ref('stg_zendesk__organization') }}
        on stg_zendesk__user.organization_id = stg_zendesk__organization.organization_id 
    where role = 'end-user'
),

stripe as (

    select 
        *,
        -- this is specific to how we store names in Stripe
        --- in the customer360 schema, this is housed in the stripe_customer_organization_name_extract_sql and stripe_shipping_organization_name_extract_sql variables
        coalesce({{ dbt.split_part('customer_name', "' ('", 1) }}, customer_name) as customer_organization_name,
        coalesce({{ dbt.split_part('shipping_name', "' ('", 1) }}, shipping_name) as shipping_organization_name

    from {{ ref('stripe__customer_overview') }}
),

map_sources as (

    select
        marketo.lead_id as marketo_lead_id,
        stripe.customer_id as stripe_customer_id,
        zendesk.user_id as zendesk_user_id,
        zendesk.organization_id as zendesk_organization_id,

        marketo.company as marketo_organization_name,
        marketo.inferred_company as marketo_inferred_organization_name,
        stripe.customer_organization_name as stripe_customer_organization_name,
        stripe.shipping_organization_name as stripe_shipping_organization_name,
        zendesk.organization_name as zendesk_organization_name,
        coalesce(marketo.email, stripe.email, zendesk.email) as email,

        max(marketo.updated_timestamp) as marketo_updated_at,
        -- stripe customer object does not have an updated_at field
        max(zendesk.updated_at) as zendesk_updated_at,
        max(zendesk.updated_at) as zendesk_org_updated_at,

        max(marketo.created_timestamp) as marketo_created_at,
        max(stripe.customer_created_at) as stripe_created_at,
        max(zendesk.created_at) as zendesk_created_at,
        max(zendesk.created_at) as zendesk_org_created_at

    -- for now just joining on email 
    --- alternative is to use custom internal ID like sf_account_id
    from {{ ref('marketo__leads') }} as marketo 

    full outer join stripe 
        on coalesce(lower(stripe.email), 'null_stripe') = coalesce(lower(marketo.email), 'null_marketo')

    full outer join zendesk
        on coalesce(lower(zendesk.email), 'null_zendesk') = lower(coalesce(marketo.email, stripe.email, 'null_marketo_stripe'))

    {{ dbt_utils.group_by(n=10) }}
),

union_org_names as (

    select 
        email,
        marketo_organization_name as organization_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at,
        false as is_inferred
    from map_sources
    where marketo_organization_name is not null

    union all 

    select 
        email,
        marketo_inferred_organization_name as organization_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at,
        true as is_inferred
    from map_sources
    where marketo_inferred_organization_name is not null

    union all

    select 
        email,
        stripe_customer_organization_name as organization_name,
        null as stripe_updated_at,
        stripe_created_at,
        false as is_inferred
    from map_sources
    where stripe_customer_organization_name is not null

    union all 

    select 
        email,
        stripe_shipping_organization_name as organization_name,
        null as stripe_updated_at,
        stripe_created_at,
        false as is_inferred
    from map_sources
    where stripe_shipping_organization_name is not null

    union all 

    select 
        email,
        zendesk_organization_name as organization_name,
        greatest(zendesk_org_updated_at, zendesk_updated_at) as updated_at,
        greatest(zendesk_created_at, zendesk_created_at) as created_at,
        false as is_inferred
    from map_sources
    where zendesk_organization_name is not null
),

rank_org_names as (

    select
        email,
        organization_name,
        is_inferred,
        count(*) over (partition by email, organization_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by email, organization_name) as value_last_updated_at

    from union_org_names
),

choose_org_names as (
    select * from (
        select
            email,
            organization_name,
            dense_rank() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc, is_inferred asc) as confidence_rank,
            row_number() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc, is_inferred asc) as index

        from rank_org_names
    ) where index = 1
),

dimensions as (

    select 
        map_sources.email,
        marketo_lead_id,
        stripe_customer_id,
        zendesk_user_id,
        zendesk_organization_id,
        choose_org_names.organization_name

    from map_sources
    left join choose_org_names 
        on map_sources.email = choose_org_names.email
    {{ dbt_utils.group_by(n=6)}}
),

zendesk_metrics as (
    select 
        orgs.organization_name,
        count(distinct ticket_id) as count_tickets

    from {{ ref('zendesk__ticket_enriched') }}
    join (select distinct organization_name, zendesk_user_id from dimensions) as orgs
        on zendesk__ticket_enriched.requester_id = orgs.zendesk_user_id 
    where {{ dbt.datediff("zendesk__ticket_enriched.created_at", dbt.current_timestamp(), "year") }} <= 1
    group by 1
    having count_tickets >= 10
),

stripe_metrics as (
    select 
        orgs.organization_name, 
        sum(coalesce(balance_transaction_net, 0)) as revenue

    from {{ ref('stripe__balance_transactions') }}
    join (select distinct organization_name, stripe_customer_id from dimensions) as orgs
        on stripe__balance_transactions.customer_id = orgs.stripe_customer_id
    where {{ dbt.datediff("balance_transaction_created_at", dbt.current_timestamp(), "year") }} <= 1
    group by 1
),

stripe_buckets as (
    select 
        organization_name,
        case 
            when revenue >= 100000 then "100k+"
            when revenue >= 1000 and revenue < 100000 then "1k-100k"
            when revenue < 1000 and revenue >= 0 then "0-1k"
        else null end as revenue_category

    from stripe_metrics
)

select 
    dimensions.email,
    stripe_buckets.revenue_category,
    zendesk_metrics.count_tickets
from dimensions 
-- limit to customers only
join stripe_buckets 
    on dimensions.organization_name = stripe_buckets.organization_name
-- limit to customers who have opened 10+ tickets
join zendesk_metrics 
    on dimensions.organization_name = zendesk_metrics.organization_name
group by 1,2,3