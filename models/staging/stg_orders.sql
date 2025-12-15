-- models/staging/stg_orders.sql
-- Staging model for raw orders data
-- Applies: renaming, casting, null handling, lowercasing, DRY principles

with source as (
    -- Step 1: Reference raw source data
    select * from {{ source('raw_sources', 'orders_raw') }}
),

renamed as (
    -- Step 2: Rename columns to business-friendly snake_case names
    select
        order_id as order_id,
        order_number as order_number,
        customer_id as customer_id,
        order_date as order_date,
        order_status as order_status,
        channel as sales_channel,
        payment_method as payment_method,
        promo_code as promo_code,
        currency as currency_code,
        order_total as order_total_amount,
        store_id as store_id,
        created_at as created_at,
        updated_at as updated_at
    from source
),

casted as (
    -- Step 3: Cast datatypes explicitly
    select
        cast(order_id as varchar(50)) as order_id,
        cast(order_number as varchar(50)) as order_number,
        cast(customer_id as varchar(50)) as customer_id,
        cast(order_date as timestamp) as order_date,
        cast(order_status as varchar(50)) as order_status,
        cast(sales_channel as varchar(50)) as sales_channel,
        cast(payment_method as varchar(50)) as payment_method,
        cast(promo_code as varchar(50)) as promo_code,
        cast(currency_code as varchar(10)) as currency_code,
        cast(order_total_amount as decimal(14, 2)) as order_total_amount,
        cast(store_id as varchar(50)) as store_id,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from renamed
),

cleaned as (
    -- Step 4: Handle nulls and lowercase text fields
    select
        lower(coalesce(order_id, 'unknown')) as order_id,
        lower(coalesce(order_number, 'unknown')) as order_number,
        lower(coalesce(customer_id, 'unknown')) as customer_id,
        order_date,
        lower(coalesce(order_status, 'unknown')) as order_status,
        lower(coalesce(sales_channel, 'unknown')) as sales_channel,
        lower(coalesce(payment_method, 'unknown')) as payment_method,
        lower(promo_code) as promo_code,  -- Allow null for no promo
        lower(coalesce(currency_code, 'inr')) as currency_code,
        coalesce(order_total_amount, 0.00) as order_total_amount,
        lower(store_id) as store_id,  -- Allow null for online orders
        created_at,
        updated_at
    from casted
),

final as (
    -- Step 5: Add derived columns and final transformations
    select
        order_id,
        order_number,
        customer_id,
        order_date,
        cast(order_date as date) as order_date_day,
        order_status,
        sales_channel,
        payment_method,
        promo_code,
        case
            when promo_code is not null then true
            else false
        end as has_promo_applied,
        currency_code,
        order_total_amount,
        store_id,
        case
            when store_id is not null then true
            else false
        end as is_in_store_order,
        case
            when sales_channel = 'web' then 'online'
            when sales_channel = 'mobile' then 'online'
            when sales_channel = 'store' then 'offline'
            else 'unknown'
        end as channel_type,
        case
            when order_status in ('cancelled', 'returned', 'refunded') then true
            else false
        end as is_cancelled_or_returned,
        created_at,
        updated_at
    from cleaned
)

select * from final