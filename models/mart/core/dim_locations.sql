with orders as (
    select * from {{ ref('fct_orders_with_success') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

order_products as (

)

select * from order_products