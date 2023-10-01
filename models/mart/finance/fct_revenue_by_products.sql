with orders as (
    select 
    product_code,
    quantity_ordered,
    amount
    from {{ ref('fct_orders_with_success') }}

),

products as (
    select 
    product_code,
    product_line,
    product_name,
    product_vendor
    from {{ ref('dim_products') }}
),

product_revenue as (
    select
    product_code,
    coalesce(sum(quantity_ordered),0) as qty_ordered,
    coalesce(round(sum(amount), 2),0) as total_amount,
    product_line,
    product_name,
    product_vendor
    from 
    orders right join 
    products using (product_code) 
    group by 1,4,5,6
)

select * from product_revenue