select product_code, product_line, product_name,product_scale, product_vendor, product_description, quantity_in_stock, buy_price, msrp

from {{ source('ecommerce', 'products') }}
limit 10