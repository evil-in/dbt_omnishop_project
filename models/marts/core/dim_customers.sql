-- models/marts/dim_customers.sql
-- Dimension model for customers
-- Includes: Surrogate key, Type-1 SCD logic, denormalized categorical attributes

{{
    config(
        materialized='table',
        unique_key='customer_sk',
        tags=['dimension', 'customers']
    )
}}

{#- 
    Type-1 SCD Logic:
    - Simply overwrites existing records with latest values
    - No history tracking - current state only
    - Achieved via materialized='table' which rebuilds entirely each run
-#}

with stg_customers as (
    -- Source from staging layer
    select * from {{ ref('stg_customers') }}
),

-- Denormalized segment attributes lookup
segment_attributes as (
    select
        segment_code,
        segment_name,
        segment_description,
        discount_tier,
        support_level,
        marketing_opt_in_default
    from (
        values
            ('vip', 'VIP', 'High-value customers with premium benefits', 'gold', 'priority', true),
            ('retail', 'Retail', 'Standard retail customers', 'standard', 'standard', true),
            ('wholesale', 'Wholesale', 'Business and bulk purchase customers', 'bulk', 'dedicated', false),
            ('unknown', 'Unknown', 'Unclassified customer segment', 'none', 'standard', false)
    ) as t(segment_code, segment_name, segment_description, discount_tier, support_level, marketing_opt_in_default)
),

-- Customer lifetime value tier denormalization
ltv_tiers as (
    select
        tier_code,
        tier_name,
        min_tenure_days,
        max_tenure_days
    from (
        values
            ('new', 'New Customer', 0, 30),
            ('established', 'Established Customer', 31, 365),
            ('loyal', 'Loyal Customer', 366, 1095),
            ('veteran', 'Veteran Customer', 1096, 999999)
    ) as t(tier_code, tier_name, min_tenure_days, max_tenure_days)
),

enriched_customers as (
    select
        -- Generate surrogate key using Jinja macro
        {{ generate_surrogate_key(['c.customer_id']) }} as customer_sk,
        
        -- Natural key (business key)
        c.customer_id as customer_nk,
        
        -- Customer attributes
        c.first_name,
        c.last_name,
        c.full_name,
        c.email_address,
        c.phone_number,
        c.has_phone,
        c.email_domain,
        
        -- Status attributes
        c.is_active,
        case 
            when c.is_active = true then 'Active'
            else 'Inactive'
        end as customer_status,
        
        -- Denormalized segment attributes (categorical)
        c.customer_segment as segment_code,
        coalesce(s.segment_name, 'Unknown') as segment_name,
        coalesce(s.segment_description, 'Unclassified') as segment_description,
        coalesce(s.discount_tier, 'none') as discount_tier,
        coalesce(s.support_level, 'standard') as support_level,
        coalesce(s.marketing_opt_in_default, false) as marketing_opt_in_default,
        c.segment_priority,
        
        -- Denormalized tenure/LTV tier attributes
        c.customer_tenure_days,
        coalesce(lt.tier_code, 'new') as tenure_tier_code,
        coalesce(lt.tier_name, 'New Customer') as tenure_tier_name,
        
        -- Derived tenure groupings
        case
            when c.customer_tenure_days <= 7 then '0-7 days'
            when c.customer_tenure_days <= 30 then '8-30 days'
            when c.customer_tenure_days <= 90 then '31-90 days'
            when c.customer_tenure_days <= 365 then '91-365 days'
            else '365+ days'
        end as tenure_bucket,
        
        -- Activity metrics
        c.days_since_last_update,
        case
            when c.days_since_last_update <= 7 then 'Recently Active'
            when c.days_since_last_update <= 30 then 'Active'
            when c.days_since_last_update <= 90 then 'Dormant'
            else 'Inactive'
        end as activity_status,
        
        -- Timestamps from source
        c.created_at,
        c.updated_at,
        
        -- Type-1 SCD metadata
        current_timestamp as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_batch_id,
        true as is_current  -- Always true for Type-1 (no history)
        
    from stg_customers c
    
    -- Left join to denormalize segment attributes
    left join segment_attributes s
        on c.customer_segment = s.segment_code
    
    -- Left join to denormalize tenure tier
    left join ltv_tiers lt
        on c.customer_tenure_days >= lt.min_tenure_days
        and c.customer_tenure_days <= lt.max_tenure_days
),

final as (
    select
        -- Surrogate key (primary key)
        customer_sk,
        
        -- Natural/business key
        customer_nk,
        
        -- Core customer attributes
        first_name,
        last_name,
        full_name,
        email_address,
        phone_number,
        has_phone,
        email_domain,
        
        -- Status
        is_active,
        customer_status,
        activity_status,
        
        -- Denormalized segment dimension (flattened)
        segment_code,
        segment_name,
        segment_description,
        segment_priority,
        discount_tier,
        support_level,
        marketing_opt_in_default,
        
        -- Denormalized tenure dimension (flattened)
        customer_tenure_days,
        tenure_tier_code,
        tenure_tier_name,
        tenure_bucket,
        
        -- Activity tracking
        days_since_last_update,
        
        -- Audit timestamps
        created_at as customer_created_at,
        updated_at as customer_updated_at,
        
        -- SCD Type-1 metadata
        dbt_loaded_at,
        dbt_batch_id,
        is_current
        
    from enriched_customers
)

select * from final