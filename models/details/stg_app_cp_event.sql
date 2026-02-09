{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        }
    )
}}

WITH source AS (
    SELECT
        event_name,
        timestamp_micros(event_timestamp) AS event_timestamp,
        parse_date('%Y%m%d',event_date) AS event_date_dt,
        -- [修改点 1] 平台字段增加归类逻辑：区分 Mobile 和 Desktop
        CASE 
            WHEN upper(device.operating_system) IN ('ANDROID', 'IOS') THEN 'Mobile'
            ELSE 'Web/Desktop'
        END AS platform,
        device.mobile_brand_name as mobile_brand_name,
        device.mobile_model_name as mobile_model_name,
        device.operating_system_version as operating_system_version,
        app_info.version as app_version,
        user_id,
        user_pseudo_id,
        event_params,
        user_properties,  -- 新增：用于提取 client_type
        geo.country as country  -- 新增：地区字段
    FROM  `notta-data-analytics.analytics_234597866.events_*`
    WHERE
        event_name LIKE 'cp_%'
        AND _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -15 day))
        AND date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -15 day)
)

    SELECT
        event_name,
        event_timestamp,
        event_date_dt, -- 用于分区
        platform,
        COALESCE(user_id,CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'user_id') AS STRING)) AS user_id,
        user_pseudo_id,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'workspace_id') AS STRING) AS workspace_id,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'plan_type') AS INT64) AS plan_type,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'timestamp') AS INT64) AS timestamp,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transaction_id') AS STRING) AS transaction_id,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'error_type') AS INT64) AS error_type,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'smart_device_type') AS STRING) AS smart_device_type,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'trans_file_size') AS INT64) AS trans_file_size,
        
        -- [修改点 2] 转写时长逻辑变更：将毫秒转换为秒，并处理空值
        COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'trans_duration') AS INT64), 0) / 1000 AS trans_duration_sec,
        
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'drop_count') AS INT64) AS drop_count,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'record_duration') AS INT64) AS record_duration,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'is_cache') AS INT64) AS is_cache,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'record_id') AS STRING) AS record_id,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'device_ble_mac') AS STRING) AS device_ble_mac,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'is_manual') AS INT64) AS is_manual,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'is_folder') AS INT64) AS is_folder,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'is_preload') AS INT64) AS is_preload,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'is_websocket') AS INT64) AS is_websocket,
        mobile_brand_name,
        mobile_model_name,
        operating_system_version,
        app_version,
        -- 新增：从 user_properties 提取 client_type (flutter_debug/flutter_test/flutter_online)
        CAST((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'client_type') AS STRING) AS client_type,
        -- 新增：国家/地区
        country
    FROM source
