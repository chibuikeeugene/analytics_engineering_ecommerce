with customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('fct_orders_with_success') }}
)

least_customers as (
    select 
    customers.cust_name,
    customers.credit_limit,
    customers.most_recent_order_dt

    from customers
    where cust_no = 141.0

)



select * from least_customers