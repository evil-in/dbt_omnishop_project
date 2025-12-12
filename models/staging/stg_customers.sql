SELECT 
    customer_id
    , first_name
    , last_name
    , email AS email_id
    , phone AS phone_number
    , created_at
    , updated_at
    , is_active
    , customer_segment
FROM {{ source('raw_sources', 'customers_raw') }}
