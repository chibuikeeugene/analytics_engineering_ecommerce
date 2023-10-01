with customers_revenue as (
    select 
    cust_no,
    customer_name,
    monetary,
    avg_monetary
    from {{ ref('fct_cust_rfm_analysis') }}
    order by monetary desc
)

select * from customers_revenue