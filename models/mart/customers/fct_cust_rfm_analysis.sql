-- customer — these are average customers
-- one time customers — these are customers that made one purchase a long time ago
-- active loyal customers — they are our customers that make good purchases frequently and have a good recency score.
-- slipping best customer — they are the best customers that have not made any purchase recently.
-- Potential customers — they are customers who made a big purchase recently.
-- best customers — they are the perfect customers with a high frequency of purchases, they have purchased recently and purchased a lot.
-- churned best customer — they are the best customers who have not made any purchases for a long time and churned.
-- new customers — they have made a very recent purchase and have a low-frequency score.
-- lost customers — they are normal and loyal customers who have not made any purchases in a very long time.
-- declining customers — they are customers who have not made a purchase recently.



with customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('fct_orders_with_success') }}
),

customer_orders as (
    select
    cust_no,
    customer_name,
    order_number,
    amount,
    order_date,
    payment_date
    from customers right join orders
    using (customer_name)
    group by customer_name, order_number, amount, order_date, payment_date, cust_no

),

-- RFM Analysis code block
rfm as (
    select
    cust_no, 
    customer_name,
    max(order_date) as Last_order_date,
    (select max(order_date) from customer_orders) as max_order_date,
    date_diff((select max(order_date) from customer_orders), max(order_date), day) as recency_days,
    coalesce(COUNT(order_number),0) as frequency,
    coalesce(round(SUM(amount),1),0) as monetary,
    coalesce(round(AVG(amount),1),0) as avg_monetary
    from customer_orders
    group by customer_name, cust_no

),

rfm_calc AS
(
    SELECT 
        *,
        NTILE(4) OVER (ORDER BY recency_days) as rfm_recency,
        NTILE(4) OVER (ORDER BY frequency) rfm_frequency,
        NTILE(4) OVER (ORDER BY monetary) rfm_monetary

    FROM rfm
),

final as (
    SELECT 
        cust_no,
        customer_name,
        Last_order_date,
        recency_days,
        frequency,
        monetary,
        avg_monetary,
       (CASE WHEN rfm_recency = 4 OR rfm_frequency = 4 OR rfm_monetary  = 4 THEN 'Champions'
            WHEN rfm_recency <= 3 OR rfm_frequency <= 3 OR rfm_monetary  <= 3 THEN 'Moderate Customers'
            WHEN rfm_recency <= 1 OR rfm_frequency <= 1 OR rfm_monetary  <= 1 THEN 'Declining customers'
        END) as rfm_segment
FROM rfm_calc)

select * from final

