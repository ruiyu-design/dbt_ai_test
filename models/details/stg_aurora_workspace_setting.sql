/*
 * @field sso_status
 * @description User opens workspace SSO login
 * @type integer
 * @value 0 SSO disabled
 * @value 1 SSO enabled

 * @field share_enabled
 * @description Open workspace sharing
 * @type integer
 * @value 0 Share disabled
 * @value 1 Share enabled
 
 * @field ip_whitelist_enabled
 * @description Open IP whitelisting
 * @type integer
 * @value 0 IP whitelist disabled
 * @value 1 IP whitelist enabled
 */

WITH workspace_setting AS (
    SELECT
        workspace_id,
        IFNULL(sso_status, 0) AS sso_status,
        IFNULL(can_share, 0) AS share_enabled,
        CASE
            WHEN ip_whitelist IS NULL THEN 0
            WHEN ip_whitelist = '' THEN 0
            ELSE 1
        END AS ip_whitelist_enabled,
        ARRAY_LENGTH(
            JSON_QUERY_ARRAY(
                CASE
                    WHEN ip_whitelist IS NULL THEN '[]'
                    WHEN ip_whitelist = '' THEN '[]'
                    ELSE ip_whitelist
                END
            )
        ) AS ip_whitelist_num
    FROM
        {{ source('Aurora', 'langogo_user_space_workspace_setting') }}
)

SELECT
    workspace_id,
    sso_status,
    share_enabled,
    ip_whitelist_enabled,
    ip_whitelist_num
FROM
    workspace_setting
WHERE
    workspace_id IS NOT NULL