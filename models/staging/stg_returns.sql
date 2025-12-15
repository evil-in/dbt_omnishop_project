-- models/staging/stg_returns.sql
-- Staging model for raw returns data
-- Applies: renaming, casting, null handling, lowercasing, DRY principles

with source as (
    -- Step 1: Reference raw source data
    select * from {{ source('raw_sources', 'returns_raw') }}
),

renamed as (
    -- Step 2: Rename columns to business-friendly snake_case names
    select
        return_id as return_id,
        order_id as order_id,
        order_item_id as order_item_id,
        product_id as product_id,
        return_reason as return_reason,
        return_status as return_status,
        refund_amount as refund_amount,
        created_at as return_requested_at,
        processed_at as return_processed_at
    from source
),

casted as (
    -- Step 3: Cast datatypes explicitly
    select
        cast(return_id as varchar(50)) as return_id,
        cast(order_id as varchar(50)) as order_id,
        cast(order_item_id as varchar(50)) as order_item_id,
        cast(product_id as varchar(50)) as product_id,
        cast(return_reason as varchar(255)) as return_reason,
        cast(return_status as varchar(50)) as return_status,
        cast(refund_amount as decimal(14, 2)) as refund_amount,
        cast(return_requested_at as timestamp) as return_requested_at,
        cast(return_processed_at as timestamp) as return_processed_at
    from renamed
),

cleaned as (
    -- Step 4: Handle nulls and lowercase text fields
    select
        lower(coalesce(return_id, 'unknown')) as return_id,
        lower(coalesce(order_id, 'unknown')) as order_id,
        lower(coalesce(order_item_id, 'unknown')) as order_item_id,
        lower(coalesce(product_id, 'unknown')) as product_id,
        lower(coalesce(return_reason, 'not specified')) as return_reason,
        lower(coalesce(return_status, 'pending')) as return_status,
        coalesce(refund_amount, 0.00) as refund_amount,
        return_requested_at,
        return_processed_at
    from casted
),

final as (
    -- Step 5: Add derived columns and final transformations
    select
        return_id,
        order_id,
        order_item_id,
        product_id,
        return_reason,
        
        -- Categorize return reasons
        case
            when return_reason like '%defective%' then 'product_quality'
            when return_reason like '%damaged%' then 'product_quality'
            when return_reason like '%wrong%' then 'fulfillment_error'
            when return_reason like '%changed mind%' then 'customer_preference'
            when return_reason like '%not as described%' then 'description_mismatch'
            else 'other'
        end as return_reason_category,
        
        return_status,
        
        -- Flag for completed returns
        case
            when return_status in ('refunded', 'replaced', 'completed') then true
            else false
        end as is_return_completed,
        
        -- Flag for pending returns
        case
            when return_status in ('pending', 'approved', 'in_transit') then true
            else false
        end as is_return_pending,
        
        refund_amount,
        
        -- Flag for returns with refund
        case
            when refund_amount > 0 then true
            else false
        end as has_refund,
        
        return_requested_at,
        cast(return_requested_at as date) as return_requested_date,
        return_processed_at,
        
        -- Flag for processed returns
        case
            when return_processed_at is not null then true
            else false
        end as is_processed,
        
        -- Calculate processing time in hours
        case
            when return_processed_at is not null 
            then round(datediff('hour', return_requested_at, return_processed_at), 2)
            else null
        end as processing_time_hours,
        
        -- Calculate processing time in days
        case
            when return_processed_at is not null 
            then datediff('day', return_requested_at, return_processed_at)
            else null
        end as processing_time_days
        
    from cleaned
)

select * from final