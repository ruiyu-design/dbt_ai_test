WITH vocabulary AS (
    SELECT
        workspace_id,
        TIMESTAMP_MILLIS(CAST(create_time AS INT64)) AS created_at,
        word_type,
        industry_category
    FROM
        {{ source('Aurora', 'langogo_user_space_vocabulary') }}
)

SELECT
    workspace_id,
    created_at,
    word_type,
    industry_category
FROM
    vocabulary
WHERE
    workspace_id IS NOT NULL