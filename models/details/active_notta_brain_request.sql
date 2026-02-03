  11{{ config(
    materialized = 'table',
    partition_by = {
      "field": "raw_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["request_id", "uid", "workspace_id"]
) }}

WITH
-- 1. 基础数据清洗：解析 JSON 并转换时间格式
parsed_data AS (
    SELECT
        JSON_VALUE(event_json, '$.event_type') as event_type,
        timestamp AS raw_timestamp, -- 原始表的时间戳，用于排序
        JSON_VALUE(event_json, '$.request_id') AS request_id,
        JSON_VALUE(event_json, '$.session_id') AS session_id,
        JSON_VALUE(event_json, '$.uid') AS uid,
        JSON_VALUE(event_json, '$.workspace_id') AS workspace_id,
        -- 将毫秒级 event_time 转换为 TIMESTAMP 类型
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(event_json, '$.event_time') AS INT64)) AS event_time,
        JSON_VALUE(event_json, '$.error_code') AS error_code,
        JSON_VALUE(event_json, '$.error_msg') AS error_msg
    FROM
        `notta-data-analytics.mc_data_statistics.server_trace_event`
    WHERE
        -- 预过滤notta_brain的所有埋点
        event_type = 'brain_session_lifecycle'
        AND JSON_VALUE(event_json, '$.request_id') IS NOT NULL
        AND JSON_VALUE(event_json, '$.request_id')!=''
        AND JSON_VALUE(event_json, '$.uid') IS NOT NULL
        AND JSON_VALUE(event_json, '$.uid')!=''
        AND JSON_VALUE(event_json, '$.workspace_id') IS NOT NULL
        AND JSON_VALUE(event_json, '$.workspace_id')!=''
),

-- 2. 构建各事件类型的去重子表
-- 逻辑：按 request_id 分组，保留 raw_timestamp 最小的那一行

t_start AS (
    SELECT * FROM parsed_data
    WHERE event_type = 'notta_brain_backend_query_starting'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY raw_timestamp ASC) = 1
),

t_success AS (
    SELECT * FROM parsed_data
    WHERE event_type = 'notta_brain_backend_query_success'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY raw_timestamp ASC) = 1
),

t_failure AS (
    SELECT * FROM parsed_data
    WHERE event_type = 'notta_brain_backend_query_failure'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY raw_timestamp ASC) = 1
),

t_agent_start AS (
    SELECT * FROM parsed_data
    WHERE event_type = 'notta_brain_agent_query_starting'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY raw_timestamp ASC) = 1
),

t_agent_success AS (
    SELECT * FROM parsed_data
    WHERE event_type = 'notta_brain_agent_query_success'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY raw_timestamp ASC) = 1
),

t_agent_failure AS (
    SELECT * FROM parsed_data
    WHERE event_type = 'notta_brain_agent_query_failure'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY raw_timestamp ASC) = 1
)

-- 3. 执行 Left Join 并生成最终宽表
SELECT
    -- 基础字段
    base.request_id,
    base.raw_timestamp,
    base.event_time,
    base.session_id,
    base.uid,
    base.workspace_id,

    -- Backend Success
    CASE WHEN s.request_id IS NOT NULL THEN 1 ELSE 0 END AS is_success,

    -- Backend Failure
    CASE WHEN f.request_id IS NOT NULL THEN 1 ELSE 0 END AS is_failure,
    f.error_code AS failure_error_code,
    f.error_msg AS failure_error_msg,

    -- Agent Start
    CASE WHEN ag_st.request_id IS NOT NULL THEN 1 ELSE 0 END AS is_agent_start,

    -- Agent Success
    CASE WHEN ag_s.request_id IS NOT NULL THEN 1 ELSE 0 END AS is_agent_success,

    -- Agent Failure
    CASE WHEN ag_f.request_id IS NOT NULL THEN 1 ELSE 0 END AS is_agent_failure,
    ag_f.error_code AS agent_failure_error_code,
    ag_f.error_msg AS agent_failure_error_msg

FROM
    t_start AS base

-- Backend Join Success
LEFT JOIN t_success AS s
    ON base.request_id = s.request_id
    AND base.session_id = s.session_id
    AND base.uid = s.uid
    AND base.workspace_id = s.workspace_id

-- Backend Join Failure
LEFT JOIN t_failure AS f
    ON base.request_id = f.request_id
    AND base.session_id = f.session_id
    AND base.uid = f.uid
    AND base.workspace_id = f.workspace_id

-- Agent Join Start
LEFT JOIN t_agent_start AS ag_st
    ON base.request_id = ag_st.request_id
    AND base.session_id = ag_st.session_id
    AND base.uid = ag_st.uid
    AND base.workspace_id = ag_st.workspace_id

-- Agent Join Success
LEFT JOIN t_agent_success AS ag_s
    ON base.request_id = ag_s.request_id
    AND base.session_id = ag_s.session_id
    AND base.uid = ag_s.uid
    AND base.workspace_id = ag_s.workspace_id

-- Agent Join Failure
LEFT JOIN t_agent_failure AS ag_f
    ON base.request_id = ag_f.request_id
    AND base.session_id = ag_f.session_id
    AND base.uid = ag_f.uid
    AND base.workspace_id = ag_f.workspace_id
