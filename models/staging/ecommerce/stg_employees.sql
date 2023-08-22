select
    employee_number,
    last_name,
    first_name,
    extension,
    email,
    office_code,
    reports_to,

from {{ source('ecommerce', 'employees') }}
limit 10