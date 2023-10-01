with location_revenue as (
    select 
    city,
    country,
    coalesce(round(sum(amount),2),0) as total
    from {{ ref('dim_locations') }}
    group by city, country
    order by total desc
)

select * from location_revenue

