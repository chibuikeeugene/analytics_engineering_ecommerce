select
    order_number,
    product_code,
    quantity_ordered,
    price_each,
    order_line_number
    
from {{ source('ecommerce', 'orderdetails') }}
limit 10