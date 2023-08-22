select 
    customer_number,
    customer_name,
    contact_last_name,
    contact_first_name,
    phone,
    address_line1,
    address_line2,
    city,
    state,
    postal_code,
    country,
    sales_rep_employee_number,
    credit_limit

from {{ source('ecommerce', 'customers') }}
limit 10
