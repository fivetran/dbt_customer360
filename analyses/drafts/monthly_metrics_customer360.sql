with spine as (

    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2023-03-01' as date)",
        end_date="cast('2024-04-01' as date)"
        )
    }}
)

select 
    spine.date_month,

    customer360__summary.customer360_id,
    customer360__summary.email,

    customer360__summary.full_name,
    customer360__summary.organization_name,
    customer360__summary.ip_address,

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

from spine

cross join {{ ref('customer360__mapping') }}
join {{ ref('customer360__summary') }} 
        on customer360__mapping.customer360_id = customer360__summary.customer360_id

left join {{ ref('stripe__balance_transactions') }}
    on customer360__mapping.stripe_customer_id = stripe__balance_transactions.customer_id
    and spine.date_month = date_trunc(cast(stripe__balance_transactions.balance_transaction_created_at as date), month)

left join {{ ref('marketo__email_sends') }}
    on customer360__mapping.marketo_lead_id = marketo__email_sends.lead_id 
    and spine.date_month = date_trunc(cast(marketo__email_sends.activity_timestamp as date), month)

left join {{ ref('zendesk__ticket_metrics') }}
    on customer360__mapping.zendesk_user_id = zendesk__ticket_metrics.submitter_id 
    and spine.date_month = date_trunc(cast(zendesk__ticket_metrics.created_at as date), month)

where customer360_id in ('5712f93342119125086fad679f570b15', '40e857a025b4b95e65921c07412f7e74') --'d07dceb993152e965791960a997d2c0d')
group by 1,2,3,4,5,6
order by 2 desc, 1 desc