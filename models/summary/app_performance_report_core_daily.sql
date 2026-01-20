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

-- 核心性能看板表：专注于3个关键事件（首字延迟、列表加载、详情页）
-- 过滤条件：1) 只要正式包 2) 冷启动配对>=200
-- 数据质量优化：去重 + user_pseudo_id 唯一性

-- CTE 1: 基础数据（只取正式包的核心事件）
WITH base_events AS (
    SELECT
        event_name,
        event_date_dt,
        platform,
        app_version,
        country,
        user_pseudo_id,  -- 设备级别唯一ID，永远有值
        transaction_id,
        timestamp,
        error_type,
        is_websocket  -- 新增：用于区分 realtime_first_word_delay 和 realtime_first_word_delay_new
    FROM `dbt_models_details.stg_app_cp_event`
    WHERE 
        client_type = 'flutter_online'  -- 只要正式包
        AND event_name IN (
            'cp_real_time_record_first_word_start',
            'cp_real_time_record_first_word_end',
            'cp_record_list_start',
            'cp_record_list_end',
            'cp_record_detail_start',
            'cp_record_detail_end',
            'cp_coldstart_start',
            'cp_coldstart_end'
        )
        AND transaction_id IS NOT NULL
        AND event_date_dt >= date_sub(current_date(), INTERVAL 15 DAY)  -- 查询最近15天数据
),

-- CTE 2: 去重 START 事件（保留最早的完整记录）
dedup_starts AS (
    SELECT 
        user_pseudo_id,
        transaction_id,
        event_name,
        ARRAY_AGG(
            STRUCT(
                event_date_dt, 
                platform, 
                app_version, 
                country, 
                timestamp, 
                error_type,
                is_websocket
            ) 
            ORDER BY timestamp ASC 
            LIMIT 1
        )[OFFSET(0)].* 
    FROM base_events
    WHERE event_name LIKE '%_start'
    GROUP BY user_pseudo_id, transaction_id, event_name
),

-- CTE 3: 去重 END 事件（保留最早的完整记录）
dedup_ends AS (
    SELECT 
        user_pseudo_id,
        transaction_id,
        event_name,
        ARRAY_AGG(
            STRUCT(
                event_date_dt, 
                platform, 
                app_version, 
                country, 
                timestamp, 
                error_type,
                is_websocket
            ) 
            ORDER BY timestamp ASC 
            LIMIT 1
        )[OFFSET(0)].* 
    FROM base_events
    WHERE event_name LIKE '%_end'
    GROUP BY user_pseudo_id, transaction_id, event_name
),

-- CTE 4: 配对事件并计算时长
paired_events AS (
    SELECT
        s.event_date_dt,
        s.platform,
        s.app_version,
        s.country,
        -- 提取事件类型（根据 is_websocket 区分首字延迟类型）
        CASE 
            WHEN s.event_name LIKE '%first_word%' AND COALESCE(s.is_websocket, 0) = 0 THEN 'realtime_first_word_delay'
            WHEN s.event_name LIKE '%first_word%' AND s.is_websocket = 1 THEN 'realtime_first_word_delay_new'
            WHEN s.event_name LIKE '%record_list%' THEN 'record_list'
            WHEN s.event_name LIKE '%record_detail%' THEN 'record_detail'
            WHEN s.event_name LIKE '%coldstart%' THEN 'coldstart'
        END AS event_type,
        s.transaction_id,
        COALESCE(e.error_type, 0) AS error_type,
        s.timestamp AS start_ms,
        e.timestamp AS end_ms,
        (e.timestamp - s.timestamp) AS duration_ms
    FROM 
        dedup_starts s
    INNER JOIN 
        dedup_ends e
    ON 
        s.user_pseudo_id = e.user_pseudo_id
        AND s.transaction_id = e.transaction_id
        AND REPLACE(s.event_name, '_start', '') = REPLACE(e.event_name, '_end', '')
        AND s.platform = e.platform
        AND s.app_version = e.app_version
        AND s.country = e.country
),

-- CTE 5: 冷启动计数（按日期+平台+版本+国家组合统计，只统计日本和美国）
coldstart_counts AS (
    SELECT
        event_date_dt,
        platform,
        app_version,
        country,
        COUNT(*) AS coldstart_count
    FROM paired_events
    WHERE event_type = 'coldstart'
        AND country IN ('Japan', 'United States')  -- 只统计日本和美国
    GROUP BY event_date_dt, platform, app_version, country
),

-- CTE 6: 有效组合（冷启动配对>=200）
valid_combinations AS (
    SELECT DISTINCT
        event_date_dt,
        platform,
        app_version,
        country
    FROM coldstart_counts
    WHERE coldstart_count >= 200
),

-- CTE 7: 只保留有效组合的核心事件（包含新的 realtime_first_word_delay_new）
filtered_events AS (
    SELECT 
        pe.*
    FROM paired_events pe
    INNER JOIN valid_combinations vc
        ON pe.event_date_dt = vc.event_date_dt
        AND pe.platform = vc.platform
        AND pe.app_version = vc.app_version
        AND pe.country = vc.country
    WHERE 
        pe.event_type IN ('realtime_first_word_delay', 'realtime_first_word_delay_new', 'record_list', 'record_detail')
),

-- CTE 8: 具体版本 + 具体国家
version_country_detail AS (
    SELECT
        event_date_dt,
        platform,
        app_version,  -- 具体版本号
        country,      -- 具体国家
        event_type,
        COUNT(*) AS sample_count,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)], 1) AS p50_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(80)], 1) AS p80_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(90)], 1) AS p90_ms,
        SAFE_DIVIDE(COUNTIF(error_type != 0), COUNT(*)) AS error_rate
    FROM filtered_events
    GROUP BY event_date_dt, platform, app_version, country, event_type
),

-- CTE 9: 具体版本 + ALL 国家
version_detail_country_all AS (
    SELECT
        event_date_dt,
        platform,
        app_version,  -- 具体版本号
        'ALL' AS country,  -- 所有国家汇总
        event_type,
        COUNT(*) AS sample_count,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)], 1) AS p50_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(80)], 1) AS p80_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(90)], 1) AS p90_ms,
        SAFE_DIVIDE(COUNTIF(error_type != 0), COUNT(*)) AS error_rate
    FROM filtered_events
    GROUP BY event_date_dt, platform, app_version, event_type
),

-- CTE 10: ALL 版本 + 具体国家
version_all_country_detail AS (
    SELECT
        event_date_dt,
        platform,
        'ALL' AS app_version,  -- 所有版本汇总
        country,  -- 具体国家
        event_type,
        COUNT(*) AS sample_count,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)], 1) AS p50_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(80)], 1) AS p80_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(90)], 1) AS p90_ms,
        SAFE_DIVIDE(COUNTIF(error_type != 0), COUNT(*)) AS error_rate
    FROM filtered_events
    GROUP BY event_date_dt, platform, country, event_type
),

-- CTE 11: ALL 版本 + ALL 国家
version_all_country_all AS (
    SELECT
        event_date_dt,
        platform,
        'ALL' AS app_version,  -- 所有版本汇总
        'ALL' AS country,  -- 所有国家汇总
        event_type,
        COUNT(*) AS sample_count,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)], 1) AS p50_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(80)], 1) AS p80_ms,
        ROUND(APPROX_QUANTILES(duration_ms, 100)[OFFSET(90)], 1) AS p90_ms,
        SAFE_DIVIDE(COUNTIF(error_type != 0), COUNT(*)) AS error_rate
    FROM filtered_events
    GROUP BY event_date_dt, platform, event_type
)

-- 最终输出：4种组合全部汇总
SELECT * FROM version_country_detail
UNION ALL
SELECT * FROM version_detail_country_all
UNION ALL
SELECT * FROM version_all_country_detail
UNION ALL
SELECT * FROM version_all_country_all
