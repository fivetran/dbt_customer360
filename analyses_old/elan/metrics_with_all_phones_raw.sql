{# 
I’m a Fivetran user who has a dashboard that shows me metrics on my customers (average order volume, total ARR, number of page visits, etc). These metrics are from a combination of sources. 

In this dashboard, I would like to be able to filter on any previous phone number the customer has had and find the specific customer.

In this dashboard, I would like to see the list of all the dimensions associated with the customer (email, phone, address, etc). This should include situations where a single customer 
has multiple of the same dimensions (ex: multiple phone numbers)
#}

with zendesk as (

    select 
        stg_zendesk__user.*,
        stg_zendesk__organization.name as organization_name

    from {{ ref('stg_zendesk__user') }}
    left join {{ ref('stg_zendesk__organization') }}
        on stg_zendesk__user.organization_id = stg_zendesk__organization.organization_id 
    where email is not null and role = 'end-user'
),

stripe as (

    select 
        *,
        {# this is specific to how we @ Fivetran store names in Stripe - Company_Name (Individual_Name)
        in the customer360 schema, this is housed in the following variables:
        - stripe_customer_organization_name_extract_sql
        - stripe_shipping_organization_name_extract_sql 
        - stripe_customer_full_name_extract_sql
        - stripe_shipping_full_name_extract_sql
        #}
        coalesce({{ dbt.split_part('customer_name', "' ('", 1) }}, customer_name) as customer_organization_name,
        coalesce({{ dbt.split_part('shipping_name', "' ('", 1) }}, shipping_name) as shipping_organization_name,
        coalesce(replace( {{ dbt.split_part('customer_name', "' ('", 2) }}, ')', ''), customer_name) as customer_full_name,
        coalesce(replace( {{ dbt.split_part('shipping_name', "' ('", 2) }}, ')', ''), shipping_name) as shipping_full_name

    from {{ ref('stripe__customer_overview') }}
    where email is not null
),

marketo as (

    select *
    from {{ ref('marketo__leads') }} where email is not null -- since we're using email to match records here
),

map_sources as (

    select
        marketo.lead_id as marketo_lead_id,
        stripe.customer_id as stripe_customer_id,
        zendesk.user_id as zendesk_user_id,
        zendesk.organization_id as zendesk_organization_id,
        lower(coalesce(marketo.email, stripe.email, zendesk.email)) as email,

        marketo.first_name || ' ' || marketo.last_name as marketo_full_name,
        stripe.customer_full_name as stripe_customer_full_name,
        stripe.shipping_full_name as stripe_shipping_full_name,
        zendesk.name as zendesk_full_name,

        marketo.company as marketo_organization_name,
        marketo.inferred_company as marketo_inferred_organization_name,
        stripe.customer_name as stripe_customer_organization_name,
        stripe.shipping_name as stripe_shipping_organization_name,
        zendesk.organization_name as zendesk_organization_name,
        marketo.phone as marketo_phone,
        marketo.main_phone as marketo_company_phone,
        marketo.mobile_phone as marketo_mobile_phone,
        stripe.phone as stripe_customer_phone,
        stripe.shipping_phone as stripe_shipping_phone,
        zendesk.phone as zendesk_phone,
        marketo.anonymous_ip as ip_address,
        -- leave out address here, join in a later CTE

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
    from marketo 

    full outer join stripe 
        on lower(stripe.email) = lower(marketo.email)

    full outer join zendesk
        on lower(zendesk.email) = lower(coalesce(marketo.email, stripe.email))
    {{ dbt_utils.group_by(n=21) }}
),

-- to grab most confident address for each individual
marketo_address as (

    select 
        lead_id,
        coalesce(address, address_lead) as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        city,
        coalesce(state, state_code) as state,
        coalesce(country, country_code) as country,
        postal_code

    from marketo
    where coalesce(address, address_lead) is not null

    union all
    
    select 
        lead_id,
        billing_street as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        billing_city as city,
        coalesce(billing_state, billing_state_code) as state,
        coalesce(billing_country, billing_country_code) as country,
        billing_postal_code as postal_code

    from marketo
    where billing_street is not null

    union all 

    select 
        lead_id,
        null as address_line_1,
        cast(null as {{ dbt.type_string() }}) as address_line_2,
        inferred_city as city,
        inferred_state_region as state,
        inferred_country as country,
        inferred_postal_code as postal_code

    from marketo
    where inferred_city is not null
),

stripe_address as (

    select 
        customer_id,
        customer_address_line_1 as address_line_1,
        customer_address_line_2 as address_line_2,
        customer_address_city as city,
        customer_address_state as state,
        customer_address_country as country,
        customer_address_postal_code as postal_code

    from stripe
    where coalesce(customer_address_line_1, customer_address_line_2) is not null

    union all 

    select 
        customer_id,
        shipping_address_line_1 as address_line_1,
        shipping_address_line_2 as address_line_2,
        shipping_address_city as city,
        shipping_address_state as state,
        shipping_address_country as country,
        shipping_address_postal_code as postal_code

    from stripe
    where coalesce(shipping_address_line_1, shipping_address_line_2) is not null
),

union_address as (

    select 
        map_sources.email,
        marketo_address.address_line_1,
        marketo_address.address_line_2,
        marketo_address.city,
        marketo_address.state,
        marketo_address.country,
        marketo_address.postal_code,
        map_sources.marketo_updated_at as updated_at,
        map_sources.marketo_created_at as created_at,

    from marketo_address join map_sources
        on marketo_address.lead_id = map_sources.marketo_lead_id

    union all

    select 
        map_sources.email,
        stripe_address.address_line_1,
        stripe_address.address_line_2,
        stripe_address.city,
        stripe_address.state,
        stripe_address.country,
        stripe_address.postal_code,
        null as updated_at,
        map_sources.stripe_created_at as created_at

    from stripe_address join map_sources
        on stripe_address.customer_id = map_sources.stripe_customer_id
),

rank_address as (

    select
        email,
        address_line_1,
        address_line_2,
        city,
        state,
        country,
        postal_code,
        count(*) over (partition by email, address_line_1, address_line_2, city, state, country, postal_code) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by email, address_line_1, address_line_2, city, state, country, postal_code) as value_last_updated_at

    from union_address
),

choose_address as (

    select
        email,
        address_line_1,
        address_line_2,
        city,
        state,
        country,
        postal_code,
        dense_rank() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as confidence_rank,
        row_number() over (partition by email order by value_count desc, coalesce(value_last_updated_at, '1970-01-01') desc) as index

    from rank_address
),

-- to grab most confident organization name for each individual
union_org_names as (

    select 
        email,
        marketo_organization_name as organization_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at
    from map_sources
    where marketo_organization_name is not null

    union all 

    select 
        email,
        marketo_inferred_organization_name as organization_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at
    from map_sources
    where marketo_inferred_organization_name is not null

    union all

    select 
        email,
        stripe_customer_organization_name as organization_name,
        null as stripe_updated_at,
        stripe_created_at
    from map_sources
    where stripe_customer_organization_name is not null

    union all 

    select 
        email,
        stripe_shipping_organization_name as organization_name,
        null as stripe_updated_at,
        stripe_created_at
    from map_sources
    where stripe_shipping_organization_name is not null

    union all 

    select 
        email,
        zendesk_organization_name as organization_name,
        greatest(zendesk_org_updated_at, zendesk_updated_at) as updated_at,
        greatest(zendesk_created_at, zendesk_created_at) as created_at
    from map_sources
    where zendesk_organization_name is not null
),

rank_org_names as (

    select
        email,
        organization_name,
        count(*) over (partition by email, organization_name) as value_count,
        max(coalesce(updated_at, created_at)) over (partition by email, organization_name) as value_last_updated_at

    from union_org_names
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

-- to grab most confident full name for each individual
union_names as (

    select 
        email,
        marketo_full_name as full_name,
        marketo_updated_at as updated_at,
        marketo_created_at as created_at
    from map_sources
    where marketo_full_name is not null

    union all 

    select 
        email,
        stripe_customer_full_name as full_name,
        null as stripe_updated_at,
        stripe_created_at
    from map_sources
    where stripe_customer_full_name is not null

    union all 

    select 
        email,
        stripe_shipping_full_name as full_name,
        null as stripe_updated_at,
        stripe_created_at
    from map_sources
    where stripe_shipping_full_name is not null

    union all 

    select 
        email,
        zendesk_full_name as full_name,
        zendesk_updated_at,
        zendesk_created_at
    from map_sources
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

-- to grab ALL phones associated with a company's individuals
marketo_phones as (

    select 
        email,
        marketo_phone as phone
    from map_sources
    where marketo_phone is not null

    union all 

    select 
        email,
        marketo_company_phone as phone
    from map_sources
    where marketo_company_phone is not null

    union all
    
    select 
        email,
        marketo_mobile_phone as phone
    from map_sources
    where marketo_mobile_phone is not null
),

stripe_phones as (

    select 
        email,
        stripe_customer_phone as phone
    from map_sources
    where stripe_customer_phone is not null

    union all 

    select 
        email,
        stripe_shipping_phone as phone
    from map_sources
    where stripe_shipping_phone is not null
),

zendesk_phones as (

    select
        email,
        zendesk_phone as phone
    from map_sources
    where zendesk_phone is not null
),

-- just grab all phones, no ranking
union_phones as (

    select 
        email,
        marketo_phones.phone
    from marketo_phones

    union all

    select 
        email,
        stripe_phones.phone
    from stripe_phones

    union all

    select 
        email,
        zendesk_phones.phone
    from zendesk_phones
),

dimensions as (

    select 
        marketo_lead_id,
        stripe_customer_id,
        zendesk_user_id,
        zendesk_organization_id,
        choose_org_names.organization_name,
        map_sources.email,
        choose_names.full_name,
        choose_address.address_line_1,
        choose_address.address_line_2,
        choose_address.city,
        choose_address.state,
        choose_address.country,
        choose_address.postal_code,
        map_sources.ip_address,
        union_phones.phone 

    from map_sources
    left join choose_names
        on map_sources.email = choose_names.email
    left join choose_org_names
        on map_sources.email = choose_org_names.email
    left join choose_address
        on map_sources.email = choose_address.email
    left join union_phones
        on map_sources.email = union_phones.email 

),

stripe_metrics as (
    select 
        orgs.organization_name,
        sum(subtotal) as total_transaction_amount,
        {{ dbt_utils.safe_divide('sum(subtotal) * 1.0', 'count(distinct balance_transaction_id)') }} as avg_transaction_amount,
        sum(total) as total_transaction_net,
        {{ dbt_utils.safe_divide('sum(total) * 1.0', 'count(distinct balance_transaction_id)') }} as avg_transaction_net,
        sum(total_quantity) as total_quantity,
        {{ dbt_utils.safe_divide('sum(total_quantity) * 1.0', 'count(distinct balance_transaction_id)') }} as avg_transaction_quantity

    from (select distinct stripe_customer_id, organization_name from dimensions) as orgs
    join {{ ref('stripe__invoice_details') }}
        on stripe__invoice_details.customer_id = orgs.stripe_customer_id

    where {{ dbt.datediff("period_start", dbt.current_timestamp(), "year") }} <= 1
    group by 1
),

marketo_metrics as (
    select 
        orgs.organization_name,
        sum(count_deliveries) as count_email_deliveries,
        sum(count_opens) as count_email_opens,
        sum(count_clicks) as count_email_clicks,
        sum(count_unsubscribes) as count_email_unsubscribes,
        count(distinct campaign_id) as count_campaigns_sent
        
    from (select distinct marketo_lead_id, organization_name from dimensions) as orgs
    join {{ ref('marketo__email_sends') }} 
        on orgs.marketo_lead_id = marketo__email_sends.lead_id
    where {{ dbt.datediff("activity_timestamp", dbt.current_timestamp(), "year") }} <= 1
    group by 1
),

zendesk_metrics as (
    select 
        orgs.organization_name,
        count(distinct ticket_id) as tickets_opened,
        {{ dbt_utils.safe_divide('sum(first_reply_time_calendar_minutes) * 1.0', 'count(distinct ticket_id)') }} as avg_first_reply_time_calendar_minutes,
        {{ dbt_utils.safe_divide('sum(final_resolution_calendar_minutes) * 1.0', 'count(distinct ticket_id)') }} as avg_final_resolution_calendar_minutes,
        {{ dbt_utils.safe_divide('sum(requester_wait_time_in_calendar_minutes) * 1.0', 'count(distinct ticket_id)') }} as avg_requester_wait_time_in_calendar_minutes
        
    from (select distinct zendesk_user_id, organization_name from dimensions) as orgs
    join {{ ref('zendesk__ticket_metrics') }} 
        on orgs.zendesk_user_id = zendesk__ticket_metrics.requester_id
    where {{ dbt.datediff("created_at", dbt.current_timestamp(), "year") }} <= 1
    group by 1
)

select 
    dimensions.organization_name,
{# 
    dimensions.customer360_id,
    dimensions.marketo_lead_id,
    dimensions.stripe_customer_id,
    dimensions.zendesk_user_id, #}
    dimensions.email,
    dimensions.full_name,
    dimensions.address_line_1,
    dimensions.address_line_2,
    dimensions.city,
    dimensions.state,
    dimensions.country,
    dimensions.postal_code,
    dimensions.ip_address,
    dimensions.phone,

    -- all organization-level metrics
    stripe_metrics.total_transaction_amount,
    stripe_metrics.avg_transaction_amount,
    stripe_metrics.total_transaction_net,
    stripe_metrics.avg_transaction_net,
    stripe_metrics.total_quantity,
    stripe_metrics.avg_transaction_quantity,
    marketo_metrics.count_email_deliveries,
    marketo_metrics.count_email_opens,
    marketo_metrics.count_email_clicks,
    marketo_metrics.count_email_unsubscribes,
    marketo_metrics.count_campaigns_sent,
    zendesk_metrics.tickets_opened,
    zendesk_metrics.avg_first_reply_time_calendar_minutes,
    zendesk_metrics.avg_final_resolution_calendar_minutes,
    zendesk_metrics.avg_requester_wait_time_in_calendar_minutes

from dimensions
left join stripe_metrics
    on dimensions.organization_name = stripe_metrics.organization_name
left join marketo_metrics
    on dimensions.organization_name = marketo_metrics.organization_name
left join zendesk_metrics
    on dimensions.organization_name = zendesk_metrics.organization_name

{{ dbt_utils.group_by(n=26) }}