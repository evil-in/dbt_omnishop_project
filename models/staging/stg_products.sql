-- models/staging/stg_products.sql
-- Staging model for raw products data
-- Applies: renaming, casting, null handling, lowercasing, DRY principles

with source as (
    -- Step 1: Reference raw source data
    select * from {{ source('raw_sources', 'products_raw') }}
),

renamed as (
    -- Step 2: Rename columns to business-friendly snake_case names
    select
        product_id as product_id,
        product_name as product_name,
        category as category,
        subcategory as subcategory,
        brand as brand,
        list_price as list_price,
        cost_price as cost_price,
        is_active as is_active,
        created_at as created_at,
        updated_at as updated_at
    from source
),

casted as (
    -- Step 3: Cast datatypes explicitly
    select
        cast(product_id as varchar(50)) as product_id,
        cast(product_name as varchar(255)) as product_name,
        cast(category as varchar(100)) as category,
        cast(subcategory as varchar(100)) as subcategory,
        cast(brand as varchar(100)) as brand,
        cast(list_price as decimal(12, 2)) as list_price,
        cast(cost_price as decimal(12, 2)) as cost_price,
        cast(is_active as boolean) as is_active,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from renamed
),

cleaned as (
    -- Step 4: Handle nulls and lowercase text fields
    select
        lower(coalesce(product_id, 'unknown')) as product_id,
        lower(coalesce(product_name, 'unknown')) as product_name,
        lower(coalesce(category, 'uncategorized')) as category,
        lower(coalesce(subcategory, 'uncategorized')) as subcategory,
        lower(coalesce(brand, 'unknown')) as brand,
        coalesce(list_price, 0.00) as list_price,
        coalesce(cost_price, 0.00) as cost_price,
        coalesce(is_active, false) as is_active,
        created_at,
        updated_at
    from casted
),

final as (
    -- Step 5: Add derived columns and final transformations
    select
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        list_price,
        cost_price,
        list_price - cost_price as gross_margin,
        case
            when cost_price > 0 then round(((list_price - cost_price) / cost_price) * 100, 2)
            else 0.00
        end as margin_percentage,
        is_active,
        created_at,
        updated_at
    from cleaned
)

select * from final