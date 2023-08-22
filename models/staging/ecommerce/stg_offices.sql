select
    office_code,
    city,
    phone,
    address_line1,
    address_line2,
    state,
    country,
    postal_code,
    territory

from {{ source('ecommerce', 'offices') }}
limit 10