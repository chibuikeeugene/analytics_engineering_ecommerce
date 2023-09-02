select customer_number, check_number, payment_date, amount

from {{ source("ecommerce", "payments") }}
-- limit 100
