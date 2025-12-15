-- tests/test_line_total_accuracy.sql
-- Custom test to verify line_total calculation accuracy
-- Returns rows where total_cost does not equal (quantity * unit_price - discount)

select
    order_id,
    order_item_id,
    quantity,
    unit_price,
    discount,
    total_cost,
    (quantity * unit_price - discount) as expected_line_total
from {{ ref('fct_sales') }}
where total_cost != (quantity * unit_price - discount)
    and (
        total_cost is null
        and quantity is not null
        and unit_price is not null
        and discount is not null
    )