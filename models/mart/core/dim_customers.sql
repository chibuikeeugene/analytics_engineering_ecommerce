with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}

),

customer_orders as (
    select
        customer_number,
        customer_name,
        contact_first_name,
        contact_last_name,
        credit_limit,
        order_number

    from customers
    left join orders using (customer_number)
    group by 1,2,3,4,5,6

),

successful_orders as (
    select * from {{ ref('fct_orders_with_success') }}
),

final as (
    select
        customer_orders.customer_number as cust_no,
        customer_orders.customer_name as customer_name,
        customer_orders.contact_first_name as cust_f_name,
        customer_orders.contact_last_name as cust_l_name,
        customer_orders.credit_limit,
        min(successful_orders.order_date) as first_order_dt,
        max(successful_orders.order_date) as most_recent_order_dt,
        coalesce(count(successful_orders.order_number), 0) as no_of_orders,
        coalesce(round(sum(successful_orders.amount),1), 0) as clv


    from customer_orders 
    left join successful_orders using (order_number)
    group by 1,2,3,4,5
    order by clv desc
)

select * from final