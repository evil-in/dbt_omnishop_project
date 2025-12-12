-- models/staging/stg_inventory.sql
-- Staging model for raw inventory data
-- Applies: renaming, casting, null handling, lowercasing, DRY principles

with source as (
    -- Step 1: Reference raw source data
    select * from {{ source('raw_sources', 'inventory_raw') }}
),

renamed as (
    -- Step 2: Rename columns to business-friendly snake_case names
    select
        inventory_id as inventory_id,
        product_id as product_id,
        warehouse_id as warehouse_id,
        on_hand_qty as on_hand_quantity,
        reserved_qty as reserved_quantity,
        safety_stock_qty as safety_stock_quantity,
        last_restocked_at as last_restocked_at,
        updated_at as updated_at
    from source
),

casted as (
    -- Step 3: Cast datatypes explicitly
    select
        cast(inventory_id as varchar(50)) as inventory_id,
        cast(product_id as varchar(50)) as product_id,
        cast(warehouse_id as varchar(50)) as warehouse_id,
        cast(on_hand_quantity as integer) as on_hand_quantity,
        cast(reserved_quantity as integer) as reserved_quantity,
        cast(safety_stock_quantity as integer) as safety_stock_quantity,
        cast(last_restocked_at as timestamp) as last_restocked_at,
        cast(updated_at as timestamp) as updated_at
    from renamed
),

cleaned as (
    -- Step 4: Handle nulls and lowercase text fields
    select
        lower(coalesce(inventory_id, 'unknown')) as inventory_id,
        lower(coalesce(product_id, 'unknown')) as product_id,
        lower(coalesce(warehouse_id, 'unknown')) as warehouse_id,
        coalesce(on_hand_quantity, 0) as on_hand_quantity,
        coalesce(reserved_quantity, 0) as reserved_quantity,
        coalesce(safety_stock_quantity, 0) as safety_stock_quantity,
        last_restocked_at,
        updated_at
    from casted
),

final as (
    -- Step 5: Add derived columns and final transformations
    select
        inventory_id,
        product_id,
        warehouse_id,
        on_hand_quantity,
        reserved_quantity,
        safety_stock_quantity,
        on_hand_quantity - reserved_quantity as available_quantity,
        case
            when on_hand_quantity <= safety_stock_quantity then true
            else false
        end as is_below_safety_stock,
        last_restocked_at,
        updated_at
    from cleaned
)

select * from final