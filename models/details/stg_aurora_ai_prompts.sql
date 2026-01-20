/*
 * @field type
 * @description Type of the prompt
 * @type string
 * @value general General prompt
 * @value custom Custom prompt (default)
 * @value private Private prompt
 */
WITH ai_prompts AS (
    SELECT
        workspace_id,
        TIMESTAMP_MILLIS(create_time) AS created_at,
        prompt_id,
        template_id,
        type,
        title,
        creator AS uid
    FROM
        {{ source('Aurora', 'langogo_user_space_ai_prompts') }}
)
SELECT
    workspace_id,
    created_at,
    prompt_id,
    template_id,
    type,
    title,
    uid
FROM
    ai_prompts
WHERE
    workspace_id IS NOT NULL
    AND prompt_id IS NOT NULL
    AND type IS NOT NULL