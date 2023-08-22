select product_line, text_description, html_description, image

from {{ source('ecommerce', 'product_lines') }}
limit 10