{# 
I’m a Fivetran user who has a dashboard that shows me metrics on my customers (average order volume, total ARR, number of page visits, etc). These metrics are from a combination of sources. 

In this dashboard, I would like to be able to filter on any previous phone number the customer has had and find the specific customer.

In this dashboard, I would like to see the list of all the dimensions associated with the customer (email, phone, address, etc). This should include situations where a single customer 
has multiple of the same dimensions (ex: multiple phone numbers) 

Questions for elan: 
- should i only surface ALL phones, or other dimensions? currently thinking ALL phones, and then the most-confident/summary values (which are all individual-level) of other dimensions. 
#}

with dimensions as (
    select
        summary.customer360_id,
        mapping.marketo_lead_id,
        mapping.stripe_customer_id,
        mapping.zendesk_user_id,
        summary.email,
        summary.full_name,
        summary.organization_name,
        summary.address_line_1,
        summary.address_line_2,
        summary.city,
        summary.state,
        summary.country,
        summary.country_alt_name,
        summary.postal_code,
        summary.ip_address,
        phones.phone,
        phones.extension

    from {{ ref('customer360__mapping') }} as mapping
    join {{ ref('customer360__summary') }} as summary 
        on mapping.customer360_id = summary.customer360_id
    left join {{ ref('customer360__phone') }} as phones 
        on mapping.customer360_id = phones.customer360_id 
    where coalesce(organization_name, '') != ''
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
    dimensions.country_alt_name,
    dimensions.postal_code,
    dimensions.ip_address,
    dimensions.phone,
    dimensions.extension,

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

{{ dbt_utils.group_by(n=28) }}