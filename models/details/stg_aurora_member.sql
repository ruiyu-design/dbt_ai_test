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
    
    -- [修改点] 将角色 ID 转换为具体业务名称
    CASE 
        WHEN role = 1 THEN 'Owner'
        WHEN role = 2 THEN 'Admin'
        WHEN role = 3 THEN 'Member'
        ELSE 'Guest' 
    END AS role,

    weekly_insight_switch
FROM
    member
WHERE
    workspace_id IS NOT NULL
    AND status IS NOT NULL
