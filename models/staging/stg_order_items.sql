-- models/staging/stg_order_items.sql
-- Staging model for raw order items data
-- Applies: renaming, casting, null handling, lowercasing, DRY principles

with source as (
    -- Step 1: Reference raw source data
    select * from {{ source('raw_sources', 'order_items_raw') }}
),

renamed as (
    -- Step 2: Rename columns to business-friendly snake_case names
    select
        order_item_id as order_item_id,
        order_id as order_id,
        product_id as product_id,
        quantity as quantity,
        unit_price as unit_price,
        discount_amount as discount_amount,
        line_total as line_total,
        created_at as created_at,
        updated_at as updated_at
    from source
),

casted as (
    -- Step 3: Cast datatypes explicitly
    select
        cast(order_item_id as varchar(50)) as order_item_id,
        cast(order_id as varchar(50)) as order_id,
        cast(product_id as varchar(50)) as product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as decimal(14, 2)) as unit_price,
        cast(discount_amount as decimal(14, 2)) as discount_amount,
        cast(line_total as decimal(14, 2)) as line_total,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from renamed
),

cleaned as (
    -- Step 4: Handle nulls and lowercase text fields
    select
        lower(coalesce(order_item_id, 'unknown')) as order_item_id,
        lower(coalesce(order_id, 'unknown')) as order_id,
        lower(coalesce(product_id, 'unknown')) as product_id,
        coalesce(quantity, 0) as quantity,
        coalesce(unit_price, 0.00) as unit_price,
        coalesce(discount_amount, 0.00) as discount_amount,
        coalesce(line_total, 0.00) as line_total,
        created_at,
        updated_at
    from casted
),

final as (
    -- Step 5: Add derived columns and final transformations
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        line_total,
        
        -- Calculated gross amount before discount
        quantity * unit_price as gross_amount,
        
        -- Expected line total for validation
        (quantity * unit_price) - discount_amount as calculated_line_total,
        
        -- Flag for line total mismatch (useful for data quality checks)
        case
            when abs(line_total - ((quantity * unit_price) - discount_amount)) > 0.01 then true
            else false
        end as has_line_total_mismatch,
        
        -- Discount percentage
        case
            when (quantity * unit_price) > 0 
            then round((discount_amount / (quantity * unit_price)) * 100, 2)
            else 0.00
        end as discount_percentage,
        
        -- Flag for discounted items
        case
            when discount_amount > 0 then true
            else false
        end as has_discount,
        
        created_at,
        updated_at
    from cleaned
)

select * from final