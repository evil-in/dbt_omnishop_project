-- models/marts/fct_sales.sql
-- Fact model for sales transactions at the order item grain
-- Incremental strategy: merge on order_item_id

{{
    config(
        materialized='incremental',
        unique_key='order_item_id'
    )
}}

with orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_date_day,
        order_status,
        sales_channel,
        channel_type,
        payment_method,
        promo_code,
        has_promo_applied,
        currency_code,
        store_id,
        is_in_store_order,
        is_cancelled_or_returned,
        updated_at as order_updated_at
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where updated_at > (select max(order_updated_at) from {{ this }})
    {% endif %}
),

order_items as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        line_total,
        gross_amount,
        discount_percentage,
        has_discount,
        updated_at as item_updated_at
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where updated_at > (select max(item_updated_at) from {{ this }})
    {% endif %}
),

products as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        list_price,
        cost_price,
        gross_margin as product_gross_margin,
        margin_percentage as product_margin_percentage
    from {{ ref('stg_products') }}
),

joined as (
    select
        -- Keys
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,
        
        -- Order attributes
        o.order_date,
        o.order_date_day,
        o.order_status,
        o.sales_channel,
        o.channel_type,
        o.payment_method,
        o.promo_code,
        o.has_promo_applied,
        o.currency_code,
        o.store_id,
        o.is_in_store_order,
        o.is_cancelled_or_returned,
        
        -- Product attributes
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        
        -- Item metrics
        oi.quantity,
        oi.unit_price,
        oi.gross_amount,
        oi.discount_amount,
        oi.discount_percentage,
        oi.has_discount,
        oi.line_total as revenue,
        
        -- Margin calculations
        oi.quantity * p.cost_price as total_cost,
        oi.line_total - (oi.quantity * p.cost_price) as margin,
        
        -- Margin percentage on actual sale
        case
            when oi.line_total > 0 
            then round(((oi.line_total - (oi.quantity * p.cost_price)) / oi.line_total) * 100, 2)
            else 0.00
        end as margin_percentage,
        
        -- Timestamps for incremental logic
        o.order_updated_at,
        oi.item_updated_at,
        greatest(o.order_updated_at, oi.item_updated_at) as last_updated_at
        
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join products p
        on oi.product_id = p.product_id
),

final as (
    select
        -- Primary key
        order_item_id,
        
        -- Foreign keys
        order_id,
        product_id,
        customer_id,
        
        -- Order dimensions
        order_date,
        order_date_day,
        order_status,
        sales_channel,
        channel_type,
        payment_method,
        promo_code,
        has_promo_applied,
        currency_code,
        store_id,
        is_in_store_order,
        is_cancelled_or_returned,
        
        -- Product dimensions
        product_name,
        category,
        subcategory,
        brand,
        
        -- Measures (as requested)
        revenue,
        discount_amount as discount,
        margin,
        
        -- Additional useful measures
        quantity,
        unit_price,
        gross_amount,
        total_cost,
        discount_percentage,
        margin_percentage,
        has_discount,
        
        -- Metadata
        order_updated_at,
        item_updated_at,
        last_updated_at
        
    from joined
)

select * from final