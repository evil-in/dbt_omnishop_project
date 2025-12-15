-- models/staging/stg_customers.sql
-- Staging model for raw customers data
-- Applies: renaming, casting, null handling, lowercasing, DRY principles

with source as (
    -- Step 1: Reference raw source data
    select * from {{ source('raw_sources', 'customers_raw') }}
),

renamed as (
    -- Step 2: Rename columns to business-friendly snake_case names
    select
        customer_id as customer_id,
        first_name as first_name,
        last_name as last_name,
        email as email_address,
        phone as phone_number,
        created_at as created_at,
        updated_at as updated_at,
        is_active as is_active,
        customer_segment as customer_segment
    from source
),

casted as (
    -- Step 3: Cast datatypes explicitly
    select
        cast(customer_id as varchar(50)) as customer_id,
        cast(first_name as varchar(100)) as first_name,
        cast(last_name as varchar(100)) as last_name,
        cast(email_address as varchar(255)) as email_address,
        cast(phone_number as varchar(20)) as phone_number,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(is_active as boolean) as is_active,
        cast(customer_segment as varchar(50)) as customer_segment
    from renamed
),

cleaned as (
    -- Step 4: Handle nulls and lowercase text fields
    select
        lower(coalesce(customer_id, 'unknown')) as customer_id,
        lower(coalesce(first_name, 'unknown')) as first_name,
        lower(coalesce(last_name, 'unknown')) as last_name,
        lower(coalesce(email_address, 'unknown@unknown.com')) as email_address,
        phone_number,  -- Allow null for missing phone numbers
        created_at,
        updated_at,
        coalesce(is_active, false) as is_active,
        lower(coalesce(customer_segment, 'unknown')) as customer_segment
    from casted
),

final as (
    -- Step 5: Add derived columns and final transformations
    select
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email_address,
        phone_number,
        case
            when phone_number is not null then true
            else false
        end as has_phone,
        
        -- Extract email domain for analytics
        case
            when email_address like '%@%' 
            then split_part(email_address, '@', 2)
            else null
        end as email_domain,
        
        created_at,
        updated_at,
        is_active,
        customer_segment,
        
        -- Segment priority for sorting/filtering
        case
            when customer_segment = 'vip' then 1
            when customer_segment = 'retail' then 2
            else 3
        end as segment_priority,
        
        -- Calculate customer tenure in days
        datediff('day', created_at, current_timestamp) as customer_tenure_days,
        
        -- Days since last update
        datediff('day', updated_at, current_timestamp) as days_since_last_update
        
    from cleaned
)

select * from final
