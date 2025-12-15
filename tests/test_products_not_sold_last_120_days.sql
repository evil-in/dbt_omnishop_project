-- tests/test_products_not_sold_last_120_days.sql
-- Singular test to identify products that haven't been sold in the last 120 days
-- Returns product_ids that fail the freshness check

with last_sale_per_product as (
    select
        product_id,
        max(order_date) as last_sale_date
    from {{ ref('fct_sales') }}
    group by product_id
),

stale_products as (
    select
        product_id,
        last_sale_date,
        current_date as check_date,
        datediff('day', last_sale_date, current_date) as days_since_last_sale
    from last_sale_per_product
    where last_sale_date < dateadd('day', -120, current_date)
)

select
    product_id,
    last_sale_date,
    check_date,
    days_since_last_sale
from stale_products
order by days_since_last_sale desc