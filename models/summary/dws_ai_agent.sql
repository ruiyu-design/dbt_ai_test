WITH agent_chat AS (
  SELECT
    2 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(agent_id AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(NULL AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(NULL AS STRING) AS profession 
  FROM `notta-data-analytics.dbt_models_details.notta_agent_chat_view`
  WHERE request_type = 'chat'
),
agent_insight AS(
  SELECT
    1 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(agent_id AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(dimension AS STRING) AS profession 
  FROM  `notta-data-analytics.dbt_models_details.notta_agent_insight_view`
  WHERE request_type = 'insight'
),
agent_link_deal AS(
  SELECT
    3 AS event_name,
    CAST(create_time AS INT64) AS create_time,
    CAST(agent_id AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(record_id AS STRING) AS record_id,
    CAST(NULL AS STRING) AS profession 
  FROM {{ ref('stg_aurora_user_space_agent_record_relation') }} -- `notta-data-analytics.dbt_models_details.stg_aurora_user_space_agent_record_relation`
  WHERE deal_id IS NOT NULL OR deal_id != ""
),
agent_sync_deal AS (
  SELECT
    4 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(NULL AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(NULL AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(NULL AS STRING) AS profession
  FROM  `notta-data-analytics.dbt_models_details.integration_deals_view`
  WHERE request_type = 'deals_update'
),
agent_sync_task AS (
  SELECT
    5 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(NULL AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(NULL AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(NULL AS STRING) AS profession
  FROM `notta-data-analytics.dbt_models_details.integration_tasks_view`
  WHERE request_type = 'task_creat'
),
agent_create AS(
  SELECT
    CAST(uid AS STRING) AS uid,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(agent_id AS STRING) AS agent_id,
    agent_name,
    create_time AS agent_creat_time,
    hubspot_setting,
    follow_up_email_switch
  FROM {{ ref('stg_aurora_user_space_agents') }} -- `notta-data-analytics.dbt_models_details.stg_aurora_user_space_agents`
),
latest_user_details AS (
  SELECT
    CAST(uid AS STRING) AS uid,
    email,
    current_plan_type
  FROM {{ ref('user_details') }} -- `notta-data-analytics.dbt_models_details.user_details`
  WHERE
    pt = (SELECT MAX(pt) FROM {{ ref('user_details') }}) -- (SELECT MAX(pt) FROM `notta-data-analytics.dbt_models_details.user_details`)
),
matched_speakers_record AS(
  SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    allocate_num
    WHERE pt = (SELECT MAX(pt) FROM {{ ref('user_details') }})
  FROM {{ ref('stg_aurora_matched_speakers_record') }} -- `notta-data-analytics.dbt_models_details.stg_aurora_matched_speakers_record`
),
agents_record AS (
  SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    record_num
  FROM {{ ref('stg_aurora_agents_record') }} -- `notta-data-analytics.dbt_models_details.stg_aurora_agents_record`
  WHERE pt = (SELECT MAX(pt) FROM {{ ref('stg_aurora_agents_record') }})
),
all_events AS (
  SELECT * FROM agent_chat
  UNION ALL
  SELECT * FROM agent_insight
  UNION ALL
  SELECT * FROM agent_link_deal
  UNION ALL
  SELECT * FROM agent_sync_deal
  UNION ALL
  SELECT * FROM agent_sync_task
)
SELECT
  ac.uid AS uid,
  ac.workspace_id,
  ac.agent_id,
  ac.agent_name,
  ac.agent_creat_time,
  ac.hubspot_setting,
  ac.follow_up_email_switch,
  ud.email,
  ud.current_plan_type,
  ae.create_time,
  ae.event_name,
  ae.record_id,
  msr.allocate_num,
  ar.record_num,
  ae.profession
FROM agent_create ac
JOIN latest_user_details ud ON ac.uid = ud.uid
LEFT JOIN all_events ae ON 
  (ae.uid = ac.uid AND 
   (ae.agent_id = ac.agent_id OR ae.agent_id IS NULL) AND
   (ae.workspace_id = ac.workspace_id OR ae.workspace_id IS NULL))
LEFT JOIN matched_speakers_record msr ON ac.workspace_id = msr.workspace_id
LEFT JOIN agents_record ar ON ac.workspace_id = ar.workspace_id