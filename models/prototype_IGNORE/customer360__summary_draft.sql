with mapping as (

    select *
    from {{ ref('customer360__mapping_draft') }}
),

emails as (

    select *
    from {{ ref('customer360__email_draft') }}
),

phones as (

    select *
    from {{ ref('customer360__phone_draft') }}
),

names as (

    select *
    from {{ ref('customer360__name_draft') }}
),

organizations as (

    select *
    from {{ ref('customer360__organization_draft') }}
),

addresses as (

    select *
    from {{ ref('customer360__address_draft') }}
),

ip_addresses as (

    select *
    from {{ ref('customer360__ip_address_draft') }}
),

rank_email as (

    select 
        customer360_id,
        email

    from emails
    where index = 1
),

rank_phone as (

    select 
        customer360_id,
        phone

    from phones
    where index = 1 
),

rank_name as (

    select 
        customer360_id,
        full_name

    from names
    where index = 1 
),

rank_organization as (

    select 
        customer360_id,
        organization_name

    from organizations
    where index = 1 
),

rank_address as (

    select 
        customer360_id,
        address_line_1,
        address_line_2,
        city,
        state,
        country,
        postal_code

    from addresses
    where index = 1 
),

rank_ip_address as (

    select 
        customer360_id,
        ip_address

    from ip_addresses
),

joined as (

    select 
        mapping.customer360_id,
        rank_email.email,
        rank_phone.phone,
        rank_name.full_name,
        rank_organization.organization_name,
        rank_address.address_line_1,
        rank_address.address_line_2,
        rank_address.city,
        rank_address.state,
        rank_address.country,
        rank_address.postal_code,
        rank_ip_address.ip_address

    from mapping
    left join rank_email
        on mapping.customer360_id = rank_email.customer360_id
    left join rank_phone
        on mapping.customer360_id = rank_phone.customer360_id
    left join rank_name
        on mapping.customer360_id = rank_name.customer360_id
    left join rank_organization
        on mapping.customer360_id = rank_organization.customer360_id
    left join rank_address
        on mapping.customer360_id = rank_address.customer360_id
    left join rank_ip_address
        on mapping.customer360_id = rank_ip_address.customer360_id
)

select *
from joined