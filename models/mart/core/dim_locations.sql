with orders as (
    select * from {{ ref('fct_orders_with_success') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

ordered_products as (
    select 
    orders.city,
    orders.country,
    orders.order_date,
    orders.payment_date,
    orders.amount,
    orders.quantity_ordered,
    products.product_name,
    products.product_vendor,
    products.product_line,
    customer_name

    from orders
    left join products using (product_code)
    group by 1,2,3,4,5,6,7,8,9,10
    
)

select * from ordered_products