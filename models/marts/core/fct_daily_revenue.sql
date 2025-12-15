-- models/marts/fct_daily_revenue.sql
-- Aggregated fact model for daily revenue by channel
-- Grain: day + sales_channel
-- Incremental strategy: merge on composite key

{{
    config(
        materialized='incremental',
        unique_key='daily_channel_key'
    )
}}

with sales as (
    select
        order_date_day,
        sales_channel,
        channel_type,
        currency_code,
        is_cancelled_or_returned,
        revenue,
        discount,
        margin,
        total_cost,
        gross_amount,
        quantity,
        order_id,
        order_item_id,
        last_updated_at
    from {{ ref('fct_sales') }}
    {% if is_incremental() %}
    where last_updated_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
),

daily_aggregates as (
    select
        -- Grain columns
        order_date_day,
        sales_channel,
        
        -- Derived key for incremental merge
        {{ dbt_utils.generate_surrogate_key(['order_date_day', 'sales_channel']) }} as daily_channel_key,
        
        -- Channel attributes (take first non-null)
        max(channel_type) as channel_type,
        max(currency_code) as currency_code,
        
        -- Revenue metrics (all transactions)
        sum(revenue) as total_revenue,
        sum(gross_amount) as total_gross_amount,
        sum(discount) as total_discount,
        sum(margin) as total_margin,
        sum(total_cost) as total_cost,
        sum(quantity) as total_quantity,
        
        -- Revenue metrics (excluding cancelled/returned)
        sum(case when not is_cancelled_or_returned then revenue else 0 end) as net_revenue,
        sum(case when not is_cancelled_or_returned then margin else 0 end) as net_margin,
        sum(case when not is_cancelled_or_returned then quantity else 0 end) as net_quantity,
        
        -- Cancelled/returned metrics
        sum(case when is_cancelled_or_returned then revenue else 0 end) as cancelled_revenue,
        sum(case when is_cancelled_or_returned then quantity else 0 end) as cancelled_quantity,
        
        -- Order counts
        count(distinct order_id) as total_order_count,
        count(distinct case when not is_cancelled_or_returned then order_id end) as net_order_count,
        count(distinct case when is_cancelled_or_returned then order_id end) as cancelled_order_count,
        
        -- Item counts
        count(order_item_id) as total_item_count,
        count(case when not is_cancelled_or_returned then order_item_id end) as net_item_count,
        
        -- Incremental tracking
        max(last_updated_at) as last_updated_at
        
    from sales
    group by 1, 2
),

final as (
    select
        -- Surrogate key
        daily_channel_key,
        
        -- Grain columns
        order_date_day,
        sales_channel,
        channel_type,
        currency_code,
        
        -- Date attributes for easier filtering/grouping
        extract(year from order_date_day) as revenue_year,
        extract(month from order_date_day) as revenue_month,
        extract(week from order_date_day) as revenue_week,
        extract(dow from order_date_day) as day_of_week,
        
        -- Gross revenue metrics
        total_revenue,
        total_gross_amount,
        total_discount,
        total_margin,
        total_cost,
        total_quantity,
        
        -- Net revenue metrics (excluding cancellations/returns)
        net_revenue,
        net_margin,
        net_quantity,
        
        -- Cancellation metrics
        cancelled_revenue,
        cancelled_quantity,
        
        -- Order & item counts
        total_order_count,
        net_order_count,
        cancelled_order_count,
        total_item_count,
        net_item_count,
        
        -- Calculated ratios
        case
            when total_order_count > 0 
            then round(net_revenue / total_order_count, 2)
            else 0.00
        end as avg_order_value,
        
        case
            when net_revenue > 0 
            then round((net_margin / net_revenue) * 100, 2)
            else 0.00
        end as margin_percentage,
        
        case
            when total_gross_amount > 0 
            then round((total_discount / total_gross_amount) * 100, 2)
            else 0.00
        end as discount_percentage,
        
        case
            when total_order_count > 0 
            then round(cancelled_order_count::decimal / total_order_count * 100, 2)
            else 0.00
        end as cancellation_rate,
        
        -- Metadata
        last_updated_at
        
    from daily_aggregates
)

select * from final