with location as (
    select * from {{ ref('dim_locations') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
    customer_name, 
    city,
    country,
    order_date,
    amount,
    quantity_ordered,
    product_name,
    product_vendor,
    product_line
    from  location inner join
    customers using (customer_name)
    group by 1,2,3,4,5,6,7,8,9
)



select * from final