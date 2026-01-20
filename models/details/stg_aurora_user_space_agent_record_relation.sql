{{ config(
    materialized='table',
    full_refresh=True
) }}
WITH latest_user_details AS (
    SELECT
        uid,
    FROM {{ ref('user_details') }}-- `notta-data-analytics.dbt_models_details.user_details`
    WHERE
      pt = (SELECT MAX(pt) FROM {{ ref('user_details') }})
),
agent_record_relation AS (
  SELECT
    uid,
    workspace_id,
    agent_id,
    deal_id,
    create_time, -- 保留毫秒级时间戳，不再转换为日期
    record_id,
    deal_name,
    crm_source,
    datastream_metadata
  FROM {{ source('Aurora', 'langogo_user_space_agent_record_relation') }} -- `notta-data-analytics.notta_aurora.langogo_user_space_agent_record_relation`
  WHERE
    uid != 1183
    AND
    deal_id IS NOT NULL
    AND
    deal_id !=""
)
SELECT
    a.uid,
    workspace_id,
    agent_id,
    deal_id,
    create_time,
    record_id,
    deal_name,
    crm_source,
    datastream_metadata
FROM
  latest_user_details as lud
INNER JOIN
  agent_record_relation as a
  ON lud.uid = a.uid