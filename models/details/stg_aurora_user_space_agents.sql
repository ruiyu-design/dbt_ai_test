{{ config(
    materialized='table',
    full_refresh=True
) }}

WITH latest_user_details AS (
    SELECT
        uid
    FROM {{ ref('user_details') }}
    WHERE
      pt = (SELECT MAX(pt) FROM {{ ref('user_details') }})
),
ai_agents AS (
  SELECT
    uid,
    workspace_id,
    agent_id,
    agent_name,
    UNIX_MILLIS(TIMESTAMP_SECONDS(create_time)) as create_time,
    resource_group_id,
    UNIX_MILLIS(TIMESTAMP_SECONDS(update_time)) as update_time,
    hubspot_setting,
    agent_status,
    resource_group_name,
    datastream_metadata,
    follow_up_email_switch,
    crm_source,
    crm_switch
  FROM `notta-data-analytics.notta_aurora.langogo_user_space_agents`
  WHERE
    uid NOT IN (1183, 102577)
)
SELECT
    a.uid,
    workspace_id,
    agent_id,
    agent_name,
    create_time,
    resource_group_id,
    update_time,
    hubspot_setting,
    agent_status,
    resource_group_name,
    datastream_metadata,
    follow_up_email_switch,
    crm_source,
    crm_switch
FROM
  latest_user_details as lud
INNER JOIN
  ai_agents as a
  ON lud.uid = a.uid