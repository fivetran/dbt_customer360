with dimensions as (
    select
        summary.customer360_id,
        mapping.customer360_organization_id,
        mapping.is_organization_header,
        {# {%- if var('customer360__using_marketo', true) -%} mapping.marketo_lead_id,{%- endif -%}
        {%- if var('customer360__using_stripe', true) -%} mapping.stripe_customer_id,{%- endif -%}
        {%- if var('customer360__using_zendesk', true) -%} mapping.zendesk_user_id,{%- endif -%} #}
        mapping.source_ids,
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
        {%- if var('customer360__using_marketo', true) -%} summary.ip_address,{%- endif -%}
        summary.phone

    from {{ ref('customer360__mapping') }} as mapping
    join {{ ref('customer360__summary') }} as summary 
        on mapping.customer360_id = summary.customer360_id
),

{% if var('customer360__using_stripe', true) %}
stripe_metrics as (
    select 
        mapping.customer360_id,
        min(first_sale_date) as stripe_first_sale_date,
        max(most_recent_sale_date) as stripe_most_recent_sale_date,
        sum(coalesce(total_sales, 0)) as stripe_total_sales,
        sum(coalesce(total_refunds, 0)) as stripe_total_refunds,
        sum(coalesce(total_gross_transaction_amount, 0)) as stripe_total_gross_transaction_amount,
        sum(coalesce(total_fees, 0)) as stripe_total_fees,
        sum(coalesce(total_net_transaction_amount, 0)) as stripe_total_net_transaction_amount,
        sum(coalesce(total_sales_count, 0)) as stripe_total_sales_count,
        sum(coalesce(total_refund_count, 0)) as stripe_total_refund_count,
        sum(coalesce(sales_this_month, 0)) as stripe_sales_this_month,
        sum(coalesce(refunds_this_month, 0)) as stripe_refunds_this_month,
        sum(coalesce(gross_transaction_amount_this_month, 0)) as stripe_gross_transaction_amount_this_month,
        sum(coalesce(fees_this_month, 0)) as stripe_fees_this_month,
        sum(coalesce(net_transaction_amount_this_month, 0)) as stripe_net_transaction_amount_this_month,
        sum(coalesce(sales_count_this_month, 0)) as stripe_sales_count_this_month,
        sum(coalesce(refund_count_this_month, 0)) as stripe_refund_count_this_month,
        sum(coalesce(total_failed_charge_count, 0)) as stripe_total_failed_charge_count,
        sum(coalesce(total_failed_charge_amount, 0)) as stripe_total_failed_charge_amount,
        sum(coalesce(failed_charge_count_this_month, 0)) as stripe_failed_charge_count_this_month,
        sum(coalesce(failed_charge_amount_this_month, 0)) as stripe_failed_charge_amount_this_month

    from {{ ref('stripe__customer_overview') }} as stripe
    left join {{ ref('customer360__mapping') }} as mapping
        on stripe.customer_id = mapping.stripe_customer_id
    group by 1
),
{% endif %}

{% if var('customer360__using_marketo', true) %}
marketo_metrics as (
    select 
        mapping.customer360_id,
        sum(coalesce(count_sends, 0)) as marketo_count_sends,
        sum(coalesce(count_opens, 0)) as marketo_count_opens,
        sum(coalesce(count_bounces, 0)) as marketo_count_bounces,
        sum(coalesce(count_clicks, 0)) as marketo_count_clicks,
        sum(coalesce(count_deliveries, 0)) as marketo_count_deliveries,
        sum(coalesce(count_unsubscribes, 0)) as marketo_count_unsubscribes,
        sum(coalesce(count_unique_opens, 0)) as marketo_count_unique_opens,
        sum(coalesce(count_unique_clicks, 0)) as marketo_count_unique_clicks
        
    from {{ ref('marketo__leads') }} as marketo
    left join {{ ref('customer360__mapping') }} as mapping
        on marketo.lead_id = mapping.marketo_lead_id
    group by 1
),
{% endif %}

{% if var('customer360__using_zendesk', true) %}
zendesk_metrics as (
    select 
        mapping.customer360_id,
        sum(coalesce(account_age_days, 0)) as zendesk_account_age_days,
        sum(coalesce(organization_account_age_days, 0)) as zendesk_organization_account_age_days,
        sum(coalesce(count_created_tickets, 0)) as zendesk_count_created_tickets,
        sum(coalesce(count_resolved_tickets, 0)) as zendesk_count_resolved_tickets,
        sum(coalesce(count_unresolved_tickets, 0)) as zendesk_count_unresolved_tickets,
        sum(coalesce(count_reopened_tickets, 0)) as zendesk_count_reopened_tickets,
        sum(coalesce(count_followup_tickets, 0)) as zendesk_count_followup_tickets,
        sum(coalesce(count_first_contact_resolved_tickets, 0)) as zendesk_count_first_contact_resolved_tickets,
        {{ dbt_utils.safe_divide('sum(avg_ticket_priority * count_created_tickets)', 'sum(count_created_tickets)') }} as zendesk_avg_ticket_priority,
        {{ dbt_utils.safe_divide('sum(avg_first_reply_time_calendar_minutes * count_created_tickets)', 'sum(count_created_tickets)') }} as zendesk_avg_first_reply_time_calendar_minutes,
        {{ dbt_utils.safe_divide('sum(avg_first_resolution_calendar_minutes * count_resolved_tickets)', 'sum(count_resolved_tickets)') }} as zendesk_avg_first_resolution_calendar_minutes,
        {{ dbt_utils.safe_divide('sum(avg_final_resolution_calendar_minutes * count_resolved_tickets)', 'sum(count_resolved_tickets)') }} as zendesk_avg_final_resolution_calendar_minutes,
        {{ dbt_utils.safe_divide('sum(avg_ticket_satisfaction_score * count_resolved_tickets)', 'sum(count_resolved_tickets)') }} as zendesk_avg_ticket_satisfaction_score,

        {# zendesk variable #}
        {% if var('using_schedules', true) %}
        , {{ dbt_utils.safe_divide('sum(avg_first_reply_time_business_minutes * count_created_tickets)', 'sum(count_created_tickets)') }} as zendesk_avg_first_reply_time_business_minutes
        , {{ dbt_utils.safe_divide('sum(avg_first_resolution_business_minutes * count_resolved_tickets)', 'sum(count_resolved_tickets)') }} as zendesk_avg_first_resolution_business_minutes
        , {{ dbt_utils.safe_divide('sum(avg_full_resolution_business_minutes * count_resolved_tickets)', 'sum(count_resolved_tickets)') }} as zendesk_avg_full_resolution_business_minutes
        {% endif %}

    from {{ ref('zendesk__customer_metrics') }} as zendesk
    left join {{ ref('customer360__mapping') }} as mapping
        on zendesk.user_id = mapping.zendesk_user_id
    group by 1
),
{% endif %}
combine_customers as (
    select 
        dimensions.*,

    {# Use MAXs instead of sums, etc in case of fanout #}
    {% if var('customer360__using_marketo', true) %}
        {# sum(coalesce(count_sends, 0)) as  #}
        marketo_count_sends,
        {# sum(coalesce(count_opens, 0)) as  #}
        marketo_count_opens,
        {# sum(coalesce(count_bounces, 0)) as  #}
        marketo_count_bounces,
        {# sum(coalesce(count_clicks, 0)) as  #}
        marketo_count_clicks,
        {# sum(coalesce(count_deliveries, 0)) as  #}
        marketo_count_deliveries,
        {# sum(coalesce(count_unsubscribes, 0)) as  #}
        marketo_count_unsubscribes,
        {# sum(coalesce(count_unique_opens, 0)) as  #}
        marketo_count_unique_opens,
        {# sum(coalesce(count_unique_clicks, 0)) as  #}
        marketo_count_unique_clicks,
    {% endif %}

    {% if var('customer360__using_stripe', true) %}
        {# min(stripe_metrics.first_sale_date) as  #}
        stripe_first_sale_date,
        {# max(stripe_metrics.most_recent_sale_date) as  #}
        stripe_most_recent_sale_date,
        {# max(coalesce(stripe_metrics.total_sales, 0)) as  #}
        stripe_total_sales,
        {# max(coalesce(stripe_metrics.total_refunds, 0)) as  #}
        stripe_total_refunds,
        {# max(coalesce(stripe_metrics.total_gross_transaction_amount, 0)) as  #}
        stripe_total_gross_transaction_amount,
        {# max(coalesce(stripe_metrics.total_fees, 0)) as  #}
        stripe_total_fees,
        {# max(coalesce(stripe_metrics.total_net_transaction_amount, 0)) as  #}
        stripe_total_net_transaction_amount,
        {# max(coalesce(stripe_metrics.total_sales_count, 0)) as  #}
        stripe_total_sales_count,
        {# max(coalesce(stripe_metrics.total_refund_count, 0)) as  #}
        stripe_total_refund_count,
        {# max(coalesce(stripe_metrics.sales_this_month, 0)) as  #}
        stripe_sales_this_month,
        {# max(coalesce(stripe_metrics.refunds_this_month, 0)) as  #}
        stripe_refunds_this_month,
        {# max(coalesce(stripe_metrics.gross_transaction_amount_this_month, 0)) as  #}
        stripe_gross_transaction_amount_this_month,
        {# max(coalesce(stripe_metrics.fees_this_month, 0)) as  #}
        stripe_fees_this_month,
        {# max(coalesce(stripe_metrics.net_transaction_amount_this_month, 0)) as  #}
        stripe_net_transaction_amount_this_month,
        {# max(coalesce(stripe_metrics.sales_count_this_month, 0)) as  #}
        stripe_sales_count_this_month,
        {# max(coalesce(stripe_metrics.refund_count_this_month, 0)) as  #}
        stripe_refund_count_this_month,
        {# max(coalesce(stripe_metrics.total_failed_charge_count, 0)) as  #}
        stripe_total_failed_charge_count,
        {# max(coalesce(stripe_metrics.total_failed_charge_amount, 0)) as  #}
        stripe_total_failed_charge_amount,
        {# max(coalesce(stripe_metrics.failed_charge_count_this_month, 0)) as  #}
        stripe_failed_charge_count_this_month,
        {# max(coalesce(stripe_metrics.failed_charge_amount_this_month, 0)) as  #}
        stripe_failed_charge_amount_this_month,
    {% endif %}

    {% if var('customer360__using_zendesk', true) %}
        {# max(account_age_days) as  #}
        zendesk_account_age_days,
        {# max(organization_account_age_days) as  #}
        zendesk_organization_account_age_days,
        {# max(coalesce(count_created_tickets, 0)) as  #}
        zendesk_count_created_tickets,
        {# max(coalesce(count_resolved_tickets, 0)) as  #}
        zendesk_count_resolved_tickets,
        {# max(coalesce(count_unresolved_tickets, 0)) as  #}
        zendesk_count_unresolved_tickets,
        {# max(coalesce(count_reopened_tickets, 0)) as  #}
        zendesk_count_reopened_tickets,
        {# max(coalesce(count_followup_tickets, 0)) as  #}
        zendesk_count_followup_tickets,
        {# max(coalesce(count_first_contact_resolved_tickets, 0)) as  #}
        zendesk_count_first_contact_resolved_tickets,

        {# max(avg_ticket_priority) as  #}
        zendesk_avg_ticket_priority,
        {# max(avg_first_reply_time_calendar_minutes) as  #}
        zendesk_avg_first_reply_time_calendar_minutes,
        {# max(avg_first_resolution_calendar_minutes) as  #}
        zendesk_avg_first_resolution_calendar_minutes,
        {# max(avg_final_resolution_calendar_minutes) as  #}
        zendesk_avg_final_resolution_calendar_minutes,
        {# max(avg_ticket_satisfaction_score) as  #}
        zendesk_avg_ticket_satisfaction_score

        {# zendesk variable #}
        {% if var('using_schedules', true) %}
        {# , max(avg_first_reply_time_business_minutes) as  #}
        , zendesk_avg_first_reply_time_business_minutes
        {# , max(avg_first_resolution_business_minutes) as  #}
        , zendesk_avg_first_resolution_business_minutes
        {# , max(avg_full_resolution_business_minutes) as  #}
        , zendesk_avg_full_resolution_business_minutes
        {% endif %}
    {% endif %}

    from dimensions

    {% if var('customer360__using_marketo', true) %}
    left join marketo_metrics
        on dimensions.customer360_id = marketo_metrics.customer360_id
    {% endif %}

    {% if var('customer360__using_stripe', true) %}
    left join stripe_metrics
        on dimensions.customer360_id = stripe_metrics.customer360_id
    {% endif %}

    {% if var('customer360__using_zendesk', true) %}
    left join zendesk_metrics
        on dimensions.customer360_id = zendesk_metrics.customer360_id
    {% endif %}

    {# {{ dbt_utils.group_by(n=15 + (1 if var('customer360__using_marketo', true) else 0)) }} #}
),

rollup_to_orgs as (

    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        {# {%- if var('customer360__using_marketo', true) -%} marketo_lead_id,{%- endif -%}
        {%- if var('customer360__using_stripe', true) -%} stripe_customer_id,{%- endif -%}
        {%- if var('customer360__using_zendesk', true) -%} zendesk_user_id,{%- endif -%} #}
        source_ids,
        email,
        full_name,
        organization_name,
        address_line_1,
        address_line_2,
        city,
        state,
        country,
        country_alt_name,
        postal_code,
        {%- if var('customer360__using_marketo', true) -%} ip_address,{%- endif -%}
        phone,

    {% if var('customer360__using_marketo', true) %}
        sum(coalesce(marketo_count_sends, 0)) as marketo_count_sends,
        sum(coalesce(marketo_count_opens, 0)) as marketo_count_opens,
        sum(coalesce(marketo_count_bounces, 0)) as marketo_count_bounces,
        sum(coalesce(marketo_count_clicks, 0)) as marketo_count_clicks,
        sum(coalesce(marketo_count_deliveries, 0)) as marketo_count_deliveries,
        sum(coalesce(marketo_count_unsubscribes, 0)) as marketo_count_unsubscribes,
        sum(coalesce(marketo_count_unique_opens, 0)) as marketo_count_unique_opens,
        sum(coalesce(marketo_count_unique_clicks, 0)) as marketo_count_unique_clicks,
    {% endif %}

    {% if var('customer360__using_stripe', true) %}
        min(stripe_first_sale_date) as stripe_first_sale_date,
        max(stripe_most_recent_sale_date) as stripe_most_recent_sale_date,
        sum(coalesce(stripe_total_sales, 0)) as stripe_total_sales,
        sum(coalesce(stripe_total_refunds, 0)) as stripe_total_refunds,
        sum(coalesce(stripe_total_gross_transaction_amount, 0)) as stripe_total_gross_transaction_amount,
        sum(coalesce(stripe_total_fees, 0)) as stripe_total_fees,
        sum(coalesce(stripe_total_net_transaction_amount, 0)) as stripe_total_net_transaction_amount,
        sum(coalesce(stripe_total_sales_count, 0)) as stripe_total_sales_count,
        sum(coalesce(stripe_total_refund_count, 0)) as stripe_total_refund_count,
        sum(coalesce(stripe_sales_this_month, 0)) as stripe_sales_this_month,
        sum(coalesce(stripe_refunds_this_month, 0)) as stripe_refunds_this_month,
        sum(coalesce(stripe_gross_transaction_amount_this_month, 0)) as stripe_gross_transaction_amount_this_month,
        sum(coalesce(stripe_fees_this_month, 0)) as stripe_fees_this_month,
        sum(coalesce(stripe_net_transaction_amount_this_month, 0)) as stripe_net_transaction_amount_this_month,
        sum(coalesce(stripe_sales_count_this_month, 0)) as stripe_sales_count_this_month,
        sum(coalesce(stripe_refund_count_this_month, 0)) as stripe_refund_count_this_month,
        sum(coalesce(stripe_total_failed_charge_count, 0)) as stripe_total_failed_charge_count,
        sum(coalesce(stripe_total_failed_charge_amount, 0)) as stripe_total_failed_charge_amount,
        sum(coalesce(stripe_failed_charge_count_this_month, 0)) as stripe_failed_charge_count_this_month,
        sum(coalesce(stripe_failed_charge_amount_this_month, 0)) as stripe_failed_charge_amount_this_month,
    {% endif %}

    {% if var('customer360__using_zendesk', true) %}
        max(zendesk_account_age_days) as zendesk_account_age_days,
        max(zendesk_organization_account_age_days) as zendesk_organization_account_age_days,
        sum(coalesce(zendesk_count_created_tickets, 0)) as zendesk_count_created_tickets,
        sum(coalesce(zendesk_count_resolved_tickets, 0)) as zendesk_count_resolved_tickets,
        sum(coalesce(zendesk_count_unresolved_tickets, 0)) as zendesk_count_unresolved_tickets,
        sum(coalesce(zendesk_count_reopened_tickets, 0)) as zendesk_count_reopened_tickets,
        sum(coalesce(zendesk_count_followup_tickets, 0)) as zendesk_count_followup_tickets,
        sum(coalesce(zendesk_count_first_contact_resolved_tickets, 0)) as zendesk_count_first_contact_resolved_tickets,

        {# Average of individual-customer averages #}
        avg(zendesk_avg_ticket_priority) as zendesk_avg_ticket_priority,
        avg(zendesk_avg_first_reply_time_calendar_minutes) as zendesk_avg_first_reply_time_calendar_minutes,
        avg(zendesk_avg_first_resolution_calendar_minutes) as zendesk_avg_first_resolution_calendar_minutes,
        avg(zendesk_avg_final_resolution_calendar_minutes) as zendesk_avg_final_resolution_calendar_minutes,
        avg(zendesk_avg_ticket_satisfaction_score) as zendesk_avg_ticket_satisfaction_score

        {# zendesk variable #}
        {% if var('using_schedules', true) %}
        , avg(zendesk_avg_first_reply_time_business_minutes) as zendesk_avg_first_reply_time_business_minutes
        , avg(zendesk_avg_first_resolution_business_minutes) as zendesk_avg_first_resolution_business_minutes
        , avg(zendesk_avg_full_resolution_business_minutes) as zendesk_avg_full_resolution_business_minutes
        {% endif %}
    {% endif %}

    from combine_customers
    where is_organization_header

    {{ dbt_utils.group_by(n=15 + (1 if var('customer360__using_marketo', true) else 0)) }}
),

final as (

    select *
    from combine_customers
    where not is_organization_header

    union all

    select *
    from rollup_to_orgs
    
)

select *
from final