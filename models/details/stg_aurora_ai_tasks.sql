/*
 * @field type
 * @description Type of the prompt
 * @type string
 * @value general General prompt
 * @value custom Custom prompt (default)
 * @value private Private prompt
 */
WITH ai_tasks AS (
    SELECT
        workspace_id,
        TIMESTAMP_MILLIS(create_time) AS created_at,
        task_id,
        type,
        title,
        creator AS uid
    FROM
        {{ source('Aurora', 'langogo_user_space_ai_tasks') }}
)
SELECT
    workspace_id,
    created_at,
    task_id,
    type,
    title,
    uid
FROM
    ai_tasks
WHERE
    workspace_id IS NOT NULL
    AND task_id IS NOT NULL
    AND type IS NOT NULL