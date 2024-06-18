

with dimensions as (
    select
        mapping.customer360_id,
        summary.organization_name,
        mapping.stripe_customer_id,
        mapping.zendesk_user_id,
        mapping.zendesk_organization_id,
        emails.email

    from `private-internal`.`zz_dbt_jamie_customer360`.`customer360__mapping` as mapping
    join `private-internal`.`zz_dbt_jamie_customer360`.`customer360__summary` as summary 
        on mapping.customer360_id = summary.customer360_id
    join `private-internal`.`zz_dbt_jamie_customer360`.`customer360__email` as emails 
        on mapping.customer360_id = emails.customer360_id 
    where coalesce(emails.email, '') != '' and coalesce(organization_name, '') != ''
    group by 1,2,3,4,5,6
),

zendesk_metrics as (
    select 
        orgs.organization_name,
        count(distinct ticket_id) as count_tickets

    from `private-internal`.`zz_dbt_jamie_zendesk`.`zendesk__ticket_enriched`
    join (select distinct zendesk_user_id, zendesk_organization_id, organization_name from dimensions) as orgs
        on zendesk__ticket_enriched.requester_id = orgs.zendesk_user_id
        or zendesk__ticket_enriched.organization_id = orgs.zendesk_organization_id
    where 

    datetime_diff(
        cast(current_timestamp() as datetime),
        cast(zendesk__ticket_enriched.created_at as datetime),
        year
    )

   <= 1
    group by 1
    having count_tickets >= 10
),

stripe_metrics as (
    select 
        orgs.organization_name, 
        sum(coalesce(balance_transaction_net, 0)) as revenue

    from `private-internal`.`zz_dbt_jamie_stripe`.`stripe__balance_transactions`
    join (select distinct stripe_customer_id, organization_name from dimensions) as orgs
        on stripe__balance_transactions.customer_id = orgs.stripe_customer_id
    where 

    datetime_diff(
        cast(current_timestamp() as datetime),
        cast(balance_transaction_created_at as datetime),
        year
    )

   <= 1
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