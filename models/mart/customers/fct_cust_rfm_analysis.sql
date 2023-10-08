-- customer — these are average customers
-- one time customers — these are customers that made one purchase a long time ago
-- active loyal customers — they are our customers that make good purchases frequently
-- and have a good recency score.
-- slipping best customer — they are the best customers that have not made any
-- purchase recently.
-- Potential customers — they are customers who made a big purchase recently.
-- best customers — they are the perfect customers with a high frequency of purchases,
-- they have purchased recently and purchased a lot.
-- churned best customer — they are the best customers who have not made any purchases
-- for a long time and churned.
-- new customers — they have made a very recent purchase and have a low-frequency
-- score.
-- lost customers — they are normal and loyal customers who have not made any
-- purchases in a very long time.
-- declining customers — they are customers who have not made a purchase recently.
with
    customers as (select * from {{ ref("dim_customers") }}),

    orders as (select * from {{ ref("fct_orders_with_success") }}),

    customer_orders as (
        select cust_no, customer_name, order_number, amount, order_date, payment_date
        from customers
        right join orders using (customer_name)
        group by customer_name, order_number, amount, order_date, payment_date, cust_no

    ),

    -- RFM Analysis code block
    rfm as (
        select
            cust_no,
            customer_name,
            max(order_date) as last_order_date,
            (select max(order_date) from customer_orders) as max_order_date,
            date_diff(
                (select max(order_date) from customer_orders), max(order_date), day
            ) as recency_days,
            coalesce(count(order_number), 0) as frequency,
            coalesce(round(sum(amount), 1), 0) as monetary,
            coalesce(round(avg(amount), 1), 0) as avg_monetary
        from customer_orders
        group by customer_name, cust_no

    ),

    rfm_groupings as (
        select
            *,
            ntile(4) over (order by recency_days) as rfm_recency,
            ntile(4) over (order by frequency) rfm_frequency,
            ntile(4) over (order by monetary) rfm_monetary

        from rfm
    ),

    rfm_calc as (
        select
            *,
            rfm_recency + rfm_frequency + rfm_monetary as rfm_cell,
            concat(
                cast(rfm_frequency as string),
                cast(rfm_recency as string),
                cast(rfm_monetary as string)
            ) as rfm_score
        from rfm_groupings
    ),

    final as (
        select
            cust_no,
            customer_name,
            last_order_date,
            recency_days,
            frequency,
            monetary,
            avg_monetary,
            rfm_score,
           (case 
        when cast(rfm_score as numeric) in (444,443,434,433) then 'churned best customer' --they have transacted a lot and frequent but it has been a long time since last transaction
        when cast(rfm_score as numeric) in (421,422,423,424,434,432,433,431) then 'lost customer'
        when cast(rfm_score as numeric) in (342,332,341,331) then 'declining customer'
        when cast(rfm_score as numeric) in (344,343,334,333) then 'slipping best customer'--they are best customer that have not purchased in a while
        when cast(rfm_score as numeric) in (142,141,143,131,132,133,242,241,243,231,232,233,414) then 'active loyal customer' -- they have purchased recently, frequently, but have low monetary value
        when cast(rfm_score as numeric) in (112,111,113,114,211,213,214,212) then 'new customer' 
        when cast(rfm_score as numeric) in (144) then 'best customer'-- they have purchase recently and frequently, with high monetary value
        when cast(rfm_score as numeric) in (411,412,413,313,312,314,311) then 'one time customer'
        when cast(rfm_score as numeric) in (222,221,223,224) then 'Potential customer'
        else 'customer'
    end) as rfm_segment
    from rfm_calc
    )

select *
from final