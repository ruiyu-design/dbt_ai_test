WITH workspace AS (
    SELECT
        workspace_id,
        owner_uid,
        create_time AS created_at,
        workspace_name
    FROM
        {{ source('Aurora', 'langogo_user_space_workspace') }}
)

SELECT
    workspace_id,
    owner_uid,
    created_at,
    workspace_name
FROM
    workspace
WHERE
    workspace_id IS NOT NULL
    AND owner_uid IS NOT NULL