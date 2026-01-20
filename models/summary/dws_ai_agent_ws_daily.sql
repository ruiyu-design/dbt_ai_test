{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
        'field': 'pt',
        'data_type': 'date'
    }
) }}

{% set pt_date = var('pt_date', none) %}

-- 预先获取所有需要用到的最大pt值，减少重复查询
WITH max_pts AS (
  SELECT 
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS max_user_details_pt,
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS max_matched_speakers_record_pt,
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS max_agents_record_pt
),
-- 当天的events，只获取指定当天的事件
agent_insight AS(
  SELECT
    1 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(agent_id AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS deal_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(dimension AS STRING) AS dimension 
  FROM  `notta-data-analytics.dbt_models_details.notta_agent_insight_view`
  WHERE request_type = 'insight' 
  {% if pt_date %}
    AND DATE(request_time) = '{{ pt_date }}'
  {% else %}
    AND DATE(request_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
),
agent_chat AS (
  SELECT
    2 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(agent_id AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(NULL AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS deal_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(NULL AS STRING) AS dimension 
  FROM `notta-data-analytics.dbt_models_details.notta_agent_chat_view`
  WHERE request_type = 'chat' 
  {% if pt_date %}
    AND DATE(request_time) = '{{ pt_date }}'
  {% else %}
    AND DATE(request_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
),
agent_link_deal AS(
  SELECT
    3 AS event_name,
    CAST(create_time AS INT64) AS create_time,
    CAST(agent_id AS STRING) AS agent_id,
    CAST(uid AS STRING) AS uid,
    CAST(deal_id AS STRING) AS deal_id,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(record_id AS STRING) AS record_id,
    CAST(NULL AS STRING) AS dimension 
  FROM {{ ref('stg_aurora_user_space_agent_record_relation') }}
  WHERE deal_id IS NOT NULL AND deal_id != "" 
  {% if pt_date %}
    AND DATE(TIMESTAMP_MILLIS(create_time)) = '{{ pt_date }}'
  {% else %}
    AND DATE(TIMESTAMP_MILLIS(create_time)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
),
-- 创建deal_id到workspace_id的映射表 (保留全量数据用于关联)
deal_workspace_map AS (
  SELECT
    deal_id,
    CAST(workspace_id AS STRING) AS workspace_id
  FROM {{ ref('stg_aurora_user_space_agent_record_relation') }}
  WHERE deal_id IS NOT NULL AND workspace_id IS NOT NULL
  GROUP BY deal_id, workspace_id
),
agent_sync_deal AS (
  SELECT
    4 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(NULL AS STRING) AS agent_id,
    CAST(asd.uid AS STRING) AS uid,
    CAST(asd.deal_id AS STRING) AS deal_id,
    -- 确保workspace_id统一为STRING类型
    CAST(COALESCE(dwm.workspace_id, '') AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(NULL AS STRING) AS dimension
  FROM `notta-data-analytics.dbt_models_details.integration_deals_view` asd
  LEFT JOIN deal_workspace_map dwm ON asd.deal_id = dwm.deal_id
  WHERE request_type = 'deals_update'
  {% if pt_date %}
    AND DATE(request_time) = '{{ pt_date }}'
  {% else %}
    AND DATE(request_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
),
agent_sync_task AS (
  SELECT
    5 AS event_name,
    UNIX_MILLIS(TIMESTAMP(request_time)) AS create_time,
    CAST(NULL AS STRING) AS agent_id,
    CAST(ast.uid AS STRING) AS uid,
    CAST(ast.deal_id AS STRING) AS deal_id,
    -- 确保workspace_id统一为STRING类型
    CAST(COALESCE(dwm.workspace_id, '') AS STRING) AS workspace_id,
    CAST(NULL AS STRING) AS record_id,
    CAST(NULL AS STRING) AS dimension
  FROM `notta-data-analytics.dbt_models_details.integration_tasks_view` ast
  LEFT JOIN deal_workspace_map dwm ON ast.deal_id = dwm.deal_id
  WHERE request_type = 'task_create'
  {% if pt_date %}
    AND DATE(request_time) = '{{ pt_date }}'
  {% else %}
    AND DATE(request_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
),
agent_create AS(
  SELECT
    CAST(uid AS STRING) AS uid,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(agent_id AS STRING) AS agent_id,
    agent_name,
    CAST(create_time AS INT64) AS agent_create_time,
    crm_source,
    crm_switch,
    follow_up_email_switch
  FROM {{ ref('stg_aurora_user_space_agents') }}
  -- 获取当前计算日期(pt_date)存在的所有agent
  {% if pt_date %}
    WHERE DATE(TIMESTAMP_MILLIS(create_time)) <= '{{ pt_date }}'
  {% else %}
    WHERE DATE(TIMESTAMP_MILLIS(create_time)) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
),
-- 创建agent_id到workspace_id的映射表
agent_workspace_map AS (
  SELECT
    agent_id,
    CAST(workspace_id AS STRING) AS workspace_id
  FROM agent_create
  WHERE agent_id IS NOT NULL AND workspace_id IS NOT NULL
  GROUP BY agent_id, workspace_id
),
-- 增强agent_chat表，添加workspace_id
enriched_agent_chat AS (
  SELECT
    ac.event_name,
    ac.create_time,
    ac.agent_id,
    ac.uid,
    -- 确保workspace_id统一为STRING类型
    CAST(COALESCE(awm.workspace_id, '') AS STRING) AS workspace_id,
    ac.deal_id,
    ac.record_id,
    ac.dimension
  FROM agent_chat ac
  LEFT JOIN agent_workspace_map awm ON ac.agent_id = awm.agent_id
),
latest_user_details AS (
  SELECT
    CAST(uid AS STRING) AS uid,
    email,
    current_plan_type
  FROM {{ ref('user_details') }} 
  CROSS JOIN max_pts
  
  {% if pt_date %}
    WHERE pt = '{{ pt_date }}'
  {% else %}
    WHERE pt = max_pts.max_user_details_pt
  {% endif %}
),
matched_speakers_record AS(
  SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(allocate_num AS INT64) AS allocate_num
  FROM {{ ref('stg_aurora_matched_speakers_record') }}
  CROSS JOIN max_pts
  {% if pt_date %}
    WHERE pt = '{{ pt_date }}'
  {% else %}
    WHERE pt = max_pts.max_matched_speakers_record_pt
  {% endif %}
),
agents_record AS (
  SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(record_num AS INT64) AS record_num
  FROM {{ ref('stg_aurora_agents_record') }}
  CROSS JOIN max_pts
  {% if pt_date %}
    WHERE pt = '{{ pt_date }}'
  {% else %}
    WHERE pt = max_pts.max_agents_record_pt
  {% endif %}
),
-- 计算当天每个workspace的事件数量
insight_counts AS (
  SELECT 
    workspace_id,
    COUNT(*) AS insight_count
  FROM agent_insight
  WHERE workspace_id IS NOT NULL
  GROUP BY workspace_id
),
chat_counts AS (
  SELECT 
    workspace_id,
    COUNT(*) AS chat_count
  FROM enriched_agent_chat
  WHERE workspace_id IS NOT NULL
  GROUP BY workspace_id
),
link_deal_counts AS (
  SELECT 
    workspace_id,
    COUNT(*) AS link_deal_count
  FROM agent_link_deal
  WHERE workspace_id IS NOT NULL
  GROUP BY workspace_id
),
sync_deal_counts AS (
  SELECT 
    workspace_id,
    COUNT(*) AS sync_deal_count
  FROM agent_sync_deal
  WHERE workspace_id IS NOT NULL
  GROUP BY workspace_id
),
sync_task_counts AS (
  SELECT 
    workspace_id,
    COUNT(*) AS sync_task_count
  FROM agent_sync_task
  WHERE workspace_id IS NOT NULL
  GROUP BY workspace_id
),
user_counts AS (
  SELECT 
    workspace_id,
    COUNT(DISTINCT uid) AS user_count
  FROM (
    SELECT workspace_id, uid FROM enriched_agent_chat WHERE workspace_id IS NOT NULL
    UNION ALL
    SELECT workspace_id, uid FROM agent_insight WHERE workspace_id IS NOT NULL
    UNION ALL
    SELECT workspace_id, uid FROM agent_link_deal WHERE workspace_id IS NOT NULL
    UNION ALL
    SELECT workspace_id, uid FROM agent_sync_deal WHERE workspace_id IS NOT NULL
    UNION ALL
    SELECT workspace_id, uid FROM agent_sync_task WHERE workspace_id IS NOT NULL
  )
  GROUP BY workspace_id
)
SELECT
  ac.uid AS owner_uid,
  ac.workspace_id,
  ac.agent_id,
  ac.agent_name,
  TIMESTAMP_MILLIS(ac.agent_create_time) AS agent_create_time,
  ac.crm_source,
  ac.crm_switch,
  ac.follow_up_email_switch,
  ud.email,
  ud.current_plan_type,
  -- 当天数据（新增）
  COALESCE(cc.chat_count, 0) AS daily_chat_num,
  COALESCE(ic.insight_count, 0) AS daily_insight_num,
  COALESCE(ldc.link_deal_count, 0) AS daily_link_deal_num,
  COALESCE(sdc.sync_deal_count, 0) AS daily_sync_deal_num,
  COALESCE(stc.sync_task_count, 0) AS daily_sync_task_num,
  COALESCE(uc.user_count, 0) AS daily_active_user_num,
  -- 其他信息
  COALESCE(msr.allocate_num, 0) AS allocate_num,
  COALESCE(ar.record_num, 0) AS record_num,
  {% if pt_date %}
    DATE('{{ pt_date }}') as pt
  {% else %}
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as pt
  {% endif %}
FROM agent_create ac
JOIN latest_user_details ud ON ac.uid = ud.uid
LEFT JOIN chat_counts cc ON cc.workspace_id = ac.workspace_id
LEFT JOIN insight_counts ic ON ic.workspace_id = ac.workspace_id
LEFT JOIN link_deal_counts ldc ON ldc.workspace_id = ac.workspace_id
LEFT JOIN sync_deal_counts sdc ON sdc.workspace_id = ac.workspace_id
LEFT JOIN sync_task_counts stc ON stc.workspace_id = ac.workspace_id
LEFT JOIN user_counts uc ON uc.workspace_id = ac.workspace_id
LEFT JOIN matched_speakers_record msr ON ac.workspace_id = msr.workspace_id
LEFT JOIN agents_record ar ON ac.workspace_id = ar.workspace_id
GROUP BY
  ac.uid,
  ac.workspace_id,
  ac.agent_id,
  ac.agent_name,
  ac.agent_create_time,
  ac.crm_source,
  ac.crm_switch,
  ac.follow_up_email_switch,
  ud.email,
  ud.current_plan_type,
  cc.chat_count,
  ic.insight_count,
  ldc.link_deal_count,
  sdc.sync_deal_count,
  stc.sync_task_count,
  uc.user_count,
  msr.allocate_num,
  ar.record_num
