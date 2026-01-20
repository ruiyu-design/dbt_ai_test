/*
 * @field platform
 * @description User singup channel
 * @type integer
 * @value 1 google
 * @value 2 azure
 * @value 3 apple
 */

 WITH source_data AS (
    SELECT
        id,
        uid,
        email,
        platform,
        status,
        create_time
    FROM
        {{ source('Aurora', 'langogo_user_space_third_login')  }}
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
        platform,
        TIMESTAMP_SECONDS(create_time) AS created_at,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY id ASC) AS row_num
    FROM
        source_data
)

SELECT
    uid,
    email,
    platform,
    created_at
FROM
    transformed_data 
WHERE
    row_num = 1
