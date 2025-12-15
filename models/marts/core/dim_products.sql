-- models/marts/dim_products.sql
-- Dimension model for products
-- Includes: Surrogate key, Type-1 SCD logic, denormalized categorical attributes

{{
    config(
        materialized='table',
        unique_key='product_sk',
        tags=['dimension', 'products']
    )
}}

{#- 
    Type-1 SCD Logic:
    - Simply overwrites existing records with latest values
    - No history tracking - current state only
    - Achieved via materialized='table' which rebuilds entirely each run
-#}

with stg_products as (
    -- Source from staging layer
    select * from {{ ref('stg_products') }}
),

-- Denormalized category attributes lookup
category_attributes as (
    select
        category_code,
        category_name,
        category_description,
        department,
        is_seasonal,
        typical_margin_tier
    from (
        values
            ('electronics', 'Electronics', 'Electronic devices and accessories', 'Technology', false, 'medium'),
            ('clothing', 'Clothing', 'Apparel and fashion items', 'Fashion', true, 'high'),
            ('home', 'Home', 'Home goods and furniture', 'Home & Living', false, 'medium'),
            ('food', 'Food', 'Food and beverages', 'Grocery', false, 'low'),
            ('sports', 'Sports', 'Sporting goods and equipment', 'Recreation', true, 'medium'),
            ('beauty', 'Beauty', 'Beauty and personal care products', 'Health & Beauty', false, 'high'),
            ('toys', 'Toys', 'Toys and games', 'Entertainment', true, 'medium'),
            ('books', 'Books', 'Books and media', 'Entertainment', false, 'low'),
            ('uncategorized', 'Uncategorized', 'Products pending categorization', 'Other', false, 'unknown')
    ) as t(category_code, category_name, category_description, department, is_seasonal, typical_margin_tier)
),

-- Denormalized brand attributes lookup
brand_attributes as (
    select
        brand_code,
        brand_display_name,
        brand_tier,
        is_house_brand,
        brand_origin_country
    from (
        values
            ('apple', 'Apple', 'premium', false, 'USA'),
            ('samsung', 'Samsung', 'premium', false, 'South Korea'),
            ('nike', 'Nike', 'premium', false, 'USA'),
            ('adidas', 'Adidas', 'premium', false, 'Germany'),
            ('sony', 'Sony', 'premium', false, 'Japan'),
            ('lg', 'LG', 'mid-tier', false, 'South Korea'),
            ('generic', 'Generic', 'budget', true, 'Various'),
            ('store_brand', 'Store Brand', 'budget', true, 'USA'),
            ('unknown', 'Unknown', 'unclassified', false, 'Unknown')
    ) as t(brand_code, brand_display_name, brand_tier, is_house_brand, brand_origin_country)
),

-- Price tier classification
price_tiers as (
    select
        tier_code,
        tier_name,
        min_price,
        max_price
    from (
        values
            ('budget', 'Budget', 0.00, 25.00),
            ('value', 'Value', 25.01, 100.00),
            ('mid_range', 'Mid-Range', 100.01, 500.00),
            ('premium', 'Premium', 500.01, 2000.00),
            ('luxury', 'Luxury', 2000.01, 999999.99)
    ) as t(tier_code, tier_name, min_price, max_price)
),

-- Margin tier classification
margin_tiers as (
    select
        tier_code,
        tier_name,
        min_margin_pct,
        max_margin_pct
    from (
        values
            ('negative', 'Loss Leader', -999.99, 0.00),
            ('low', 'Low Margin', 0.01, 15.00),
            ('standard', 'Standard Margin', 15.01, 30.00),
            ('healthy', 'Healthy Margin', 30.01, 50.00),
            ('high', 'High Margin', 50.01, 999.99)
    ) as t(tier_code, tier_name, min_margin_pct, max_margin_pct)
),

enriched_products as (
    select
        -- Generate surrogate key using Jinja macro
        {{ generate_surrogate_key(['p.product_id']) }} as product_sk,
        
        -- Natural key (business key)
        p.product_id as product_nk,
        
        -- Product attributes
        p.product_name,
        initcap(p.product_name) as product_name_display,
        
        -- Denormalized category attributes
        p.category as category_code,
        coalesce(c.category_name, 'Uncategorized') as category_name,
        coalesce(c.category_description, 'Products pending categorization') as category_description,
        coalesce(c.department, 'Other') as department,
        coalesce(c.is_seasonal, false) as is_seasonal_category,
        coalesce(c.typical_margin_tier, 'unknown') as category_typical_margin_tier,
        
        -- Subcategory (kept as-is, could be denormalized if lookup exists)
        p.subcategory as subcategory_code,
        initcap(replace(p.subcategory, '_', ' ')) as subcategory_name,
        
        -- Denormalized brand attributes
        p.brand as brand_code,
        coalesce(b.brand_display_name, initcap(p.brand)) as brand_name,
        coalesce(b.brand_tier, 'unclassified') as brand_tier,
        coalesce(b.is_house_brand, false) as is_house_brand,
        coalesce(b.brand_origin_country, 'Unknown') as brand_origin_country,
        
        -- Pricing attributes
        p.list_price,
        p.cost_price,
        p.gross_margin,
        p.margin_percentage,
        
        -- Denormalized price tier
        coalesce(pt.tier_code, 'value') as price_tier_code,
        coalesce(pt.tier_name, 'Value') as price_tier_name,
        
        -- Denormalized margin tier
        coalesce(mt.tier_code, 'standard') as margin_tier_code,
        coalesce(mt.tier_name, 'Standard Margin') as margin_tier_name,
        
        -- Profitability flags
        case when p.gross_margin > 0 then true else false end as is_profitable,
        case when p.margin_percentage >= 30 then true else false end as is_high_margin,
        
        -- Status attributes
        p.is_active,
        case 
            when p.is_active = true then 'Active'
            else 'Inactive'
        end as product_status,
        
        -- Product age calculations
        p.created_at,
        p.updated_at,
        datediff('day', p.created_at, current_timestamp) as product_age_days,
        datediff('day', p.updated_at, current_timestamp) as days_since_update,
        
        -- Product age tier
        case
            when datediff('day', p.created_at, current_timestamp) <= 30 then 'New'
            when datediff('day', p.created_at, current_timestamp) <= 180 then 'Recent'
            when datediff('day', p.created_at, current_timestamp) <= 365 then 'Established'
            else 'Mature'
        end as product_lifecycle_stage
        
    from stg_products p
    
    -- Left join to denormalize category attributes
    left join category_attributes c
        on p.category = c.category_code
    
    -- Left join to denormalize brand attributes
    left join brand_attributes b
        on p.brand = b.brand_code
    
    -- Left join to denormalize price tier
    left join price_tiers pt
        on p.list_price >= pt.min_price
        and p.list_price <= pt.max_price
    
    -- Left join to denormalize margin tier
    left join margin_tiers mt
        on p.margin_percentage >= mt.min_margin_pct
        and p.margin_percentage <= mt.max_margin_pct
),

final as (
    select
        -- Surrogate key (primary key)
        product_sk,
        
        -- Natural/business key
        product_nk,
        
        -- Product identifiers
        product_name,
        product_name_display,
        
        -- Denormalized category dimension (flattened)
        category_code,
        category_name,
        category_description,
        department,
        is_seasonal_category,
        category_typical_margin_tier,
        
        -- Subcategory
        subcategory_code,
        subcategory_name,
        
        -- Denormalized brand dimension (flattened)
        brand_code,
        brand_name,
        brand_tier,
        is_house_brand,
        brand_origin_country,
        
        -- Pricing
        list_price,
        cost_price,
        gross_margin,
        margin_percentage,
        
        -- Denormalized price tier (flattened)
        price_tier_code,
        price_tier_name,
        
        -- Denormalized margin tier (flattened)
        margin_tier_code,
        margin_tier_name,
        
        -- Profitability
        is_profitable,
        is_high_margin,
        
        -- Status
        is_active,
        product_status,
        
        -- Lifecycle
        product_age_days,
        days_since_update,
        product_lifecycle_stage,
        
        -- Audit timestamps
        created_at as product_created_at,
        updated_at as product_updated_at,
        
        -- SCD Type-1 metadata
        current_timestamp as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_batch_id,
        true as is_current  -- Always true for Type-1 (no history)
        
    from enriched_products
)

select * from final