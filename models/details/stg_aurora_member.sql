/*
 * @field status
 * @description Member status
 * @type integer
 * @value 0 To be confirmed
 * @value 1 Joined
 * @value 2 Rejected
 * @value 3 Expired
 * @value 4 Cancelled
 * @value 5 Removed
 * @value 6 Failed (SSO)
 */

WITH member AS (
    SELECT
        workspace_id,
        status,
        role,
        weekly_insight_switch
    FROM
        {{ source('Aurora', 'langogo_user_space_member') }}
    WHERE
        status = 1
)

SELECT
    workspace_id,
    status,
    role,
    weekly_insight_switch
FROM
    member
WHERE
    workspace_id IS NOT NULL
    AND status IS NOT NULL