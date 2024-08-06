select 
    customer360__summary.customer360_id,
    customer360__summary.email,
    customer360__summary.full_name,
    customer360__summary.organization_name,
    date_trunc(cast(stripe__balance_transactions.balance_transaction_created_at as date), month) as transaction_date,
    sum(balance_transaction_net) as monthly_revenue
    
from zz_dbt_jamie_customer360.customer360__summary 
join zz_dbt_jamie_customer360.customer360__mapping 
    using(customer360_id) 
join zz_dbt_jamie_stripe.stripe__balance_transactions
    on customer360__mapping.stripe_customer_id = stripe__balance_transactions.customer_id
where customer360_id = '5712f93342119125086fad679f570b15'
group by 1,2,3,4,5
order by 5 desc