with mapping as (

    select * 
    from {{ ref('customer360__mapping_draft') }}
),

marketo as (

    select *
    from {{ ref('marketo__leads') }}
),

final as (

    select 
        customer360_id,
        anonymous_ip as ip_address,
        case 
            when anonymous_ip like '%:%' then 'ipv6'
            else 'ipv4' end as type,
        'marketo' as source,
    from mapping 
    join marketo
        on mapping.marketo_lead_id = marketo.lead_id

    where marketo.anonymous_ip is not null
)

select *
from final