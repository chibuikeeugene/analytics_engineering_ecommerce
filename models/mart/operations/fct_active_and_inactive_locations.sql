with location as (
    select * from {{ ref('dim_locations') }}
),

location_perfomance as (
    select 
    city, 
    country,  
    coalesce(round(sum(amount),2),0) as total_amount, 
    coalesce(sum(quantity_ordered),0) as total_qty_ordered
    from location
    group by city, country
    order by total_amount desc
)


select * from location_perfomance