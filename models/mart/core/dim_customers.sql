with customers as (
    select distinct(status) from {{ source('ecommerce', 'orders') }}
)

select * from customers

-- with failed_orders as (
--     select * from {{ source('ecommerce', 'orders') }} where status = 'Cancelled'
-- )

-- select * from failed_orders