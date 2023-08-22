select
    order_number,
    order_date,
    required_date,
    shipped_date,
    status,
    comments,
    customer_number

from {{ source('ecommerce', 'orders') }}
limit 10
