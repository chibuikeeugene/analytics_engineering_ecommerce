with orders as (
    select * from {{ ref("stg_orders") }}
),

order_details as (
    select * from {{ ref("stg_orderdetails") }}
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
        order_details.product_code,
        order_details.quantity_ordered,
        order_details.price_each,
        orders.order_date,
        orders.customer_number, 
        count(orders.order_number) as number_of_orders,
        payments.payment_date,
        coalesce(sum(payments.amount),0) as amount,
        orders.status

    from payments 
    right join orders using (customer_number)
    left join order_details using (order_number)

    where orders.status = 'Shipped' or orders.status = 'Resolved'

    group by order_number,customer_number,order_date,payment_date, status, product_code, quantity_ordered, price_each

    order by payment_date desc 
),

final as (
    select
        customers.customer_name,
        customers.city,
        customers.country,
        successful_order_payments.product_code,
        successful_order_payments.quantity_ordered,
        successful_order_payments.price_each,
        successful_order_payments.order_number,
        successful_order_payments.order_date,
        successful_order_payments.payment_date,
        successful_order_payments.amount,
        successful_order_payments.status

    from customers right join successful_order_payments using (customer_number)

    group by 1,2,3,4,5,6,7,8,9,10,11

)

select * from final