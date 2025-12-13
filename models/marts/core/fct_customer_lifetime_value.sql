-- models/marts/fct_customer_lifetime_value.sql
-- Aggregated fact model for customer lifetime value
-- Grain: customer_id
-- Incremental strategy: merge on customer_id

{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

with sales as (
    select
        customer_id,
        order_id,
        order_item_id,
        order_date,
        order_date_day,
        sales_channel,
        channel_type,
        currency_code,
        is_cancelled_or_returned,
        has_promo_applied,
        has_discount,
        revenue,
        discount,
        margin,
        total_cost,
        gross_amount,
        quantity,
        category,
        last_updated_at
    from {{ ref('fct_sales') }}
    {% if is_incremental() %}
    where last_updated_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
),

-- Get most common currency per customer
customer_currency as (
    select
        customer_id,
        currency_code,
        row_number() over (
            partition by customer_id 
            order by count(*) desc, currency_code
        ) as currency_rank
    from sales
    group by customer_id, currency_code
),

primary_currency as (
    select
        customer_id,
        currency_code as primary_currency
    from customer_currency
    where currency_rank = 1
),

customer_aggregates as (
    select
        -- Grain column
        s.customer_id,
        
        -- LTV calculation: total revenue minus total discounts
        sum(s.revenue) as total_revenue,
        sum(s.discount) as total_discounts,
        sum(s.revenue) - sum(s.discount) as lifetime_value_gross,
        
        -- Net LTV (excluding cancelled/returned)
        sum(case when not s.is_cancelled_or_returned then s.revenue else 0 end) 
            - sum(case when not s.is_cancelled_or_returned then s.discount else 0 end) as lifetime_value_net,
        
        -- Revenue metrics
        sum(s.gross_amount) as total_gross_amount,
        sum(s.margin) as total_margin,
        sum(s.total_cost) as total_cost,
        sum(s.quantity) as total_quantity,
        
        -- Net metrics (excluding cancelled/returned)
        sum(case when not s.is_cancelled_or_returned then s.revenue else 0 end) as net_revenue,
        sum(case when not s.is_cancelled_or_returned then s.discount else 0 end) as net_discounts,
        sum(case when not s.is_cancelled_or_returned then s.margin else 0 end) as net_margin,
        sum(case when not s.is_cancelled_or_returned then s.quantity else 0 end) as net_quantity,
        
        -- Order metrics
        count(distinct s.order_id) as total_order_count,
        count(distinct case when not s.is_cancelled_or_returned then s.order_id end) as net_order_count,
        count(distinct case when s.is_cancelled_or_returned then s.order_id end) as cancelled_order_count,
        count(s.order_item_id) as total_item_count,
        
        -- Time-based metrics
        min(s.order_date) as first_order_date,
        max(s.order_date) as last_order_date,
        min(s.order_date_day) as first_order_day,
        max(s.order_date_day) as last_order_day,
        
        -- Channel preferences
        count(distinct case when s.channel_type = 'online' then s.order_id end) as online_order_count,
        count(distinct case when s.channel_type = 'offline' then s.order_id end) as offline_order_count,
        sum(case when s.channel_type = 'online' then s.revenue else 0 end) as online_revenue,
        sum(case when s.channel_type = 'offline' then s.revenue else 0 end) as offline_revenue,
        
        -- Promo & discount behavior
        count(distinct case when s.has_promo_applied then s.order_id end) as promo_order_count,
        count(case when s.has_discount then s.order_item_id end) as discounted_item_count,
        
        -- Category diversity
        count(distinct s.category) as unique_categories_purchased,
        
        -- Currency (most common per customer from separate CTE)
        max(pc.primary_currency) as primary_currency,
        
        -- Incremental tracking
        max(s.last_updated_at) as last_updated_at
        
    from sales s
    left join primary_currency pc
        on s.customer_id = pc.customer_id
    group by 1
),

final as (
    select
        -- Primary key
        customer_id,
        
        -- Core LTV metrics
        lifetime_value_gross,
        lifetime_value_net,
        total_revenue,
        total_discounts,
        net_revenue,
        net_discounts,
        
        -- Profitability
        total_margin,
        net_margin,
        total_cost,
        
        -- Volume metrics
        total_quantity,
        net_quantity,
        total_gross_amount,
        
        -- Order behavior
        total_order_count,
        net_order_count,
        cancelled_order_count,
        total_item_count,
        
        -- Time metrics
        first_order_date,
        last_order_date,
        first_order_day,
        last_order_day,
        last_order_day - first_order_day as customer_tenure_days,
        
        -- Recency (days since last order)
        current_date - last_order_day as days_since_last_order,
        
        -- Channel behavior
        online_order_count,
        offline_order_count,
        online_revenue,
        offline_revenue,
        case
            when online_order_count > offline_order_count then 'online'
            when offline_order_count > online_order_count then 'offline'
            when online_order_count = offline_order_count and online_order_count > 0 then 'omnichannel'
            else 'unknown'
        end as preferred_channel,
        
        -- Promo behavior
        promo_order_count,
        discounted_item_count,
        
        -- Diversity
        unique_categories_purchased,
        primary_currency,
        
        -- Calculated averages
        case
            when net_order_count > 0 
            then round(lifetime_value_net / net_order_count, 2)
            else 0.00
        end as avg_order_value,
        
        case
            when net_order_count > 0 
            then round(net_quantity::decimal / net_order_count, 2)
            else 0.00
        end as avg_items_per_order,
        
        case
            when net_revenue > 0 
            then round((net_margin / net_revenue) * 100, 2)
            else 0.00
        end as margin_percentage,
        
        case
            when total_gross_amount > 0 
            then round((total_discounts / total_gross_amount) * 100, 2)
            else 0.00
        end as discount_percentage,
        
        case
            when total_order_count > 0 
            then round(cancelled_order_count::decimal / total_order_count * 100, 2)
            else 0.00
        end as cancellation_rate,
        
        case
            when total_order_count > 0 
            then round(promo_order_count::decimal / total_order_count * 100, 2)
            else 0.00
        end as promo_usage_rate,
        
        -- Purchase frequency (orders per month of tenure)
        case
            when customer_tenure_days > 30 
            then round(net_order_count::decimal / (customer_tenure_days / 30.0), 2)
            else net_order_count::decimal
        end as orders_per_month,
        
        -- Customer segmentation helpers
        case
            when net_order_count = 1 then 'one_time'
            when net_order_count between 2 and 3 then 'repeat'
            when net_order_count between 4 and 10 then 'loyal'
            when net_order_count > 10 then 'vip'
            else 'inactive'
        end as customer_order_segment,
        
        -- Metadata
        last_updated_at
        
    from customer_aggregates
)

select * from final