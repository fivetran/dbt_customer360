{{ config(enabled=var('customer360__using_marketo', true)) }}

with mapping as (

    select * 
    from {{ ref('customer360__mapping') }}
),

marketo as (

    select *
    from {{ ref('int_customer360__marketo') }}
    where ip_address is not null
),

final as (

    select 
        customer360_id,
        customer360_organization_id,
        is_organization_header,
        ip_address,
        case 
            when ip_address like '%:%' then 'ipv6'
            else 'ipv4' end as type,
        'marketo' as source,
    from mapping 
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id
)

select *
from final