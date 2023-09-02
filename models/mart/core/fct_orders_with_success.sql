with orders as (
    select * from {{ ref("stg_orders") }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

successful_order_payments as (
    select
    orders.order_number,
    orders.customer_number, 
    count(orders.order_number) as number_of_orders,
    payments.payment_date,
    coalesce(sum(payments.amount),0) as amount
    -- customers.city,
    -- customers.country,
    -- orders.status as status,
    -- orders.order_date as order_date,
    -- orders.shipped_date as shipped_date,

    from orders
    left join payments using (customer_number)
    -- where status = 'Shipped' || 'Resolved'
    group by order_number,customer_number,payment_date
    order by number_of_orders desc 
)

-- final as (
--     select
--     successful_orders.order_number,
--     -- successful_orders.customer_number,
--     -- successful_orders.status,
--     -- successful_orders.order_date,
--     -- successful_orders.shipped_date,
--     payments.payment_date,
--     payments.amount

--     from successful_orders
--     left join customers using (order_number)
--     left join payments using (order_number)

--     group by customer_name
-- )

select * from successful_order_payments