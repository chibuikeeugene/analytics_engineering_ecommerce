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
        orders.order_date,
        orders.status,
        orders.comments,
        orders.customer_number, 
        count(orders.order_number) as number_of_orders,
        payments.payment_date,
        coalesce(sum(payments.amount),0) as amount

    from orders
    left join payments using (customer_number)

    where orders.status = 'Cancelled' or orders.status =  'Disputed'

    group by order_number,customer_number,order_date,payment_date, comments, status

    order by payment_date desc 
),

final as (
    select
        successful_order_payments.comments,
        customers.customer_name,
        customers.city,
        customers.country,
        successful_order_payments.status,
        successful_order_payments.order_number,
        successful_order_payments.order_date,
        successful_order_payments.payment_date,
        successful_order_payments.amount

    from customers inner join successful_order_payments using (customer_number)

    group by 1,2,3,4,5,6,7,8,9

)

select * from final