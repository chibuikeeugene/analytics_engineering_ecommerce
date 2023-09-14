with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        products.product_line,
        products.product_code,
        products.product_name,
        products.product_vendor,
        products.quantity_in_stock,
        products.buy_price as buy_price,
        products.msrp as msrp,
        round((msrp - buy_price ), 2) as profit_margin
        
    from products
    -- group by 1,2,3,4,5,6
)


select * from final