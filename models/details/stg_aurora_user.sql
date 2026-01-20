WITH source_data AS (
    SELECT 
        uid,
        email,
        status,
        create_time,
        subscribe_email,
        os_type
    FROM
        {{ source('Aurora', 'langogo_user_space_users') }}
    WHERE 
        uid IS NOT NULL AND
        email IS NOT NULL AND
        email != '' AND
        email != ' ' AND
        NOT REGEXP_CONTAINS(email, r'(airgram.io|langogo|nqmo.com|uuf.me)') AND
        status IN (1)
),

transformed_data AS (
    SELECT 
        uid,
        email,
        subscribe_email,
        TIMESTAMP_SECONDS(create_time) AS created_at,
        os_type,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY create_time) AS row_num
    FROM source_data
)

SELECT 
    uid,
    email,
    created_at,
    subscribe_email,
    os_type
FROM
    transformed_data 
WHERE
    row_num = 1