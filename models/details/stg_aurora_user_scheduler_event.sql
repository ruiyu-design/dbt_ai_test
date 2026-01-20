WITH scheduler_event AS (
    SELECT
        CAST(workspace_id AS INT64) AS workspace_id,
        meeting_tools,
        TIMESTAMP_MILLIS(create_time) AS created_at
    FROM
        {{ source('Aurora', 'calendar_user_scheduler_event') }}
)

SELECT
    workspace_id,
    meeting_tools,
    created_at
FROM
    scheduler_event
WHERE
    workspace_id IS NOT NULL
    AND meeting_tools IS NOT NULL