WITH
-- =================================================
-- 1. 基础维表
-- =================================================
user_dim_dedup AS (
    SELECT
        uid,
        email,
        CASE WHEN signup_country IN ('Japan','United States','unknown','United Kingdom','France','Canada')  THEN signup_country ELSE 'Other' END AS signup_country
    FROM `notta-data-analytics.dbt_models_details.user_details`
    WHERE pt = date_add(date(current_timestamp()), interval -1 DAY)
),

ws_owner AS (
    SELECT
        workspace_id,
        CAST(owner_uid AS INT64) as owner_uid,
        DATE(TIMESTAMP_MILLIS(CAST(create_time AS INT64))) AS create_date,
        DATE_TRUNC(DATE(TIMESTAMP_MILLIS(CAST(create_time AS INT64))), WEEK(MONDAY)) as create_week,
        DATE_TRUNC(DATE(TIMESTAMP_MILLIS(CAST(create_time AS INT64))), MONTH) as create_month
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
    WHERE owner_uid IS NOT NULL AND workspace_id IS NOT NULL AND status != 2
),

-- =================================================
-- 2. 套餐等级 (Max Plan Logic)
-- =================================================
week_plans AS (
    SELECT
        workspace_id,
        DATE(TIMESTAMP_TRUNC(calendar_date, ISOWEEK)) as week,
        MAX(goods_plan_type) as goods_plan_type
    FROM `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    WHERE is_trial = 0
    GROUP BY 1, 2
),

month_plans AS (
    SELECT
        workspace_id,
        DATE(TIMESTAMP_TRUNC(calendar_date, MONTH)) as month,
        MAX(goods_plan_type) as goods_plan_type
    FROM `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    WHERE is_trial = 0
    GROUP BY 1, 2
),

-- =================================================
-- 3. 全量录音统计 (修正：严格按 UID + Date 聚合，防止膨胀)
-- =================================================
daily_record_stats_base AS (
    SELECT
        record_date as stat_date,
        uid,
        -- 这里只保留计算 Meeting User 需要的原子指标
        SUM(record_duration) / 60.0 as total_duration_min, -- 转为分钟
        SUM(record_count) as total_record_cnt
    FROM `notta-data-analytics.dbt_models_details.stg_active_user_records_daily`
    -- 建议加上时间过滤减少数据量
    WHERE record_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)
    GROUP BY 1, 2
),

-- =================================================
-- 4. 会议用户标签预计算 (Label Pre-calculation)
--    核心逻辑：平均时长 > 30分钟
-- =================================================

-- Daily Label
daily_meeting_labels AS (
    SELECT
        stat_date,
        uid,
        CASE
            WHEN SAFE_DIVIDE(total_duration_min, total_record_cnt) > 30 THEN 'Meeting User'
            ELSE 'Non-Meeting User'
        END AS label
    FROM daily_record_stats_base
),

-- Weekly Label
weekly_meeting_labels AS (
    SELECT
        DATE_TRUNC(stat_date, WEEK(MONDAY)) as week_start,
        uid,
        CASE
            -- 周总时长 / 周总条数 > 30
            WHEN SAFE_DIVIDE(SUM(total_duration_min), SUM(total_record_cnt)) > 30 THEN 'Meeting User'
            ELSE 'Non-Meeting User'
        END AS label
    FROM daily_record_stats_base
    GROUP BY 1, 2
),

-- Monthly Label
monthly_meeting_labels AS (
    SELECT
        DATE_TRUNC(stat_date, MONTH) as month_start,
        uid,
        CASE
            WHEN SAFE_DIVIDE(SUM(total_duration_min), SUM(total_record_cnt)) > 30 THEN 'Meeting User'
            ELSE 'Non-Meeting User'
        END AS label
    FROM daily_record_stats_base
    GROUP BY 1, 2
),

-- =================================================
-- 5. AI Summary 行为底表
-- =================================================
daily_behavior AS (
    SELECT
        DATE(TIMESTAMP_SECONDS(timestamp)) AS stat_date,
        CAST(uid AS INT64) AS uid,
        workspace_id,
        CASE WHEN platform IN ('Web','Server') THEN 'WEB' ELSE platform END AS platform_group,
        -- Transcription Type 简化写法
        CASE
           WHEN transcription_type is null then 'Unknown'
           WHEN transcription_type=1 then 'File'
           WHEN transcription_type=2 then 'Real Time'
           WHEN transcription_type=3 then 'Multilingual Meeting'
           WHEN transcription_type=4 then 'Meeting'
           WHEN transcription_type=5 then 'Accurate'
           WHEN transcription_type=6 then 'Screen'
           WHEN transcription_type=7 then 'Media Download'
           WHEN transcription_type=8 then 'Multilingual File Transcribe'
           WHEN transcription_type=9 then 'Subtitle'
           WHEN transcription_type=10 then 'Multilingual RealTime Transcribe'
           WHEN transcription_type=11 then 'Calendar Events Auto Join Meeting'
           WHEN transcription_type=12 then 'Youtube'
           ELSE 'Other'
        END as transcription_type_code, -- 也可以保留您原来的长 CASE WHEN
        COUNT(1) AS usage_count,
        COUNTIF(trigger_type = 1) as manual_usage_count,
        COUNTIF(trigger_type = 2) as auto_usage_count
    FROM `notta-data-analytics.mc_data_statistics.notta_summary_ai_records`
    WHERE uid IS NOT NULL
      AND DATE(TIMESTAMP_SECONDS(timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)
    GROUP BY 1, 2, 3, 4, 5
),

-- =================================================
-- 6. Source CTEs (Daily/Weekly/Monthly)
-- =================================================

-- [Source Daily]
source_daily AS (
    SELECT
        'Daily' as grain,
        b.stat_date as stat_date,
        b.uid,

        -- Lifecycle
        CASE WHEN wo.create_date = b.stat_date THEN 'New User' ELSE 'Existing User' END AS user_lifecycle,

        -- Meeting Status (直接 Join 算好的 Label)
        COALESCE(ml.label, 'Non-Meeting User') AS meeting_user_status,

        -- Dimensions
        IFNULL(u.signup_country, 'unknown') AS signup_country,
        CASE
            WHEN u.uid IS NULL THEN 'Guest/Deleted/Internal'
            WHEN b1.goods_plan_type IS NULL THEN 'Free'
            WHEN b1.goods_plan_type = 0 THEN 'Starter'
            WHEN b1.goods_plan_type = 1 THEN 'Pro'
            WHEN b1.goods_plan_type = 2 THEN 'Biz'
            WHEN b1.goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type,

        b.platform_group,
        b.transcription_type_code as transcription_type,
        b.manual_usage_count,
        b.auto_usage_count

    FROM daily_behavior b
    INNER JOIN ws_owner wo ON b.workspace_id = wo.workspace_id
    LEFT JOIN user_dim_dedup u ON wo.owner_uid = u.uid
    LEFT JOIN `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan` b1
        ON b1.workspace_id = b.workspace_id AND b1.calendar_date = b.stat_date
    -- [修正] Join daily_meeting_labels，而不是直接计算
    LEFT JOIN daily_meeting_labels ml
        ON b.uid = ml.uid AND b.stat_date = ml.stat_date
    WHERE
       -- 统一黑名单过滤
       (u.email IS NULL OR (
            u.email NOT LIKE '%@airgram.io' AND u.email NOT LIKE '%@notta.ai'
            AND u.email NOT LIKE '%@langogo%'
            AND u.email NOT LIKE '%@chacuo.net'
            AND u.email NOT LIKE '%@uuf.me'
            AND u.email NOT LIKE '%@nqmo.com'
            AND u.email NOT LIKE '%@linshiyouxiang.net'
            AND u.email NOT LIKE '%@besttempmail.com'
            AND u.email NOT LIKE '%@celebrityfull.com'
            AND u.email NOT LIKE '%@comparisions.net'
            AND u.email NOT LIKE '%@mediaholy.com'
            AND u.email NOT LIKE '%@maillazy.com'
            AND u.email NOT LIKE '%@justdefinition.com'
            AND u.email NOT LIKE '%@inctart.com'
            AND u.email NOT LIKE '%@deepyinc.com'
       ))
),

-- [Source Weekly]
source_weekly AS (
    SELECT
        'Weekly' as grain,
        DATE_TRUNC(b.stat_date, WEEK(MONDAY)) as stat_date,
        b.uid,

        CASE WHEN wo.create_week = DATE_TRUNC(b.stat_date, WEEK(MONDAY)) THEN 'New User' ELSE 'Existing User' END AS user_lifecycle,
        COALESCE(ml.label, 'Non-Meeting User') AS meeting_user_status,

        IFNULL(u.signup_country, 'unknown') AS signup_country,
        CASE
            WHEN u.uid IS NULL THEN 'Guest/Deleted/Internal'
            WHEN wp.goods_plan_type IS NULL THEN 'Free'
            WHEN wp.goods_plan_type = 0 THEN 'Starter'
            WHEN wp.goods_plan_type = 1 THEN 'Pro'
            WHEN wp.goods_plan_type = 2 THEN 'Biz'
            WHEN wp.goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type,

        b.platform_group,
        b.transcription_type_code as transcription_type,
        b.manual_usage_count,
        b.auto_usage_count

    FROM daily_behavior b
    INNER JOIN ws_owner wo ON b.workspace_id = wo.workspace_id
    LEFT JOIN user_dim_dedup u ON wo.owner_uid = u.uid
    LEFT JOIN week_plans wp
        ON wp.workspace_id = b.workspace_id AND wp.week = DATE_TRUNC(b.stat_date, WEEK(MONDAY))
    LEFT JOIN weekly_meeting_labels ml
        ON b.uid = ml.uid AND ml.week_start = DATE_TRUNC(b.stat_date, WEEK(MONDAY))
    WHERE
       (u.email IS NULL OR (
            u.email NOT LIKE '%@airgram.io' AND u.email NOT LIKE '%@notta.ai'
            AND u.email NOT LIKE '%@langogo%'
            AND u.email NOT LIKE '%@chacuo.net'
            AND u.email NOT LIKE '%@uuf.me'
            AND u.email NOT LIKE '%@nqmo.com'
            AND u.email NOT LIKE '%@linshiyouxiang.net'
            AND u.email NOT LIKE '%@besttempmail.com'
            AND u.email NOT LIKE '%@celebrityfull.com'
            AND u.email NOT LIKE '%@comparisions.net'
            AND u.email NOT LIKE '%@mediaholy.com'
            AND u.email NOT LIKE '%@maillazy.com'
            AND u.email NOT LIKE '%@justdefinition.com'
            AND u.email NOT LIKE '%@inctart.com'
            AND u.email NOT LIKE '%@deepyinc.com'
       ))
),

-- [Source Monthly]
source_monthly AS (
    SELECT
        'Monthly' as grain,
        DATE_TRUNC(b.stat_date, MONTH) as stat_date,
        b.uid,

        CASE WHEN wo.create_month = DATE_TRUNC(b.stat_date, MONTH) THEN 'New User' ELSE 'Existing User' END AS user_lifecycle,
        COALESCE(ml.label, 'Non-Meeting User') AS meeting_user_status,

        IFNULL(u.signup_country, 'unknown') AS signup_country,
        CASE
            WHEN u.uid IS NULL THEN 'Guest/Deleted/Internal'
            WHEN mp.goods_plan_type IS NULL THEN 'Free'
            WHEN mp.goods_plan_type = 0 THEN 'Starter'
            WHEN mp.goods_plan_type = 1 THEN 'Pro'
            WHEN mp.goods_plan_type = 2 THEN 'Biz'
            WHEN mp.goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type,

        b.platform_group,
        b.transcription_type_code as transcription_type,
        b.manual_usage_count,
        b.auto_usage_count

    FROM daily_behavior b
    INNER JOIN ws_owner wo ON b.workspace_id = wo.workspace_id
    LEFT JOIN user_dim_dedup u ON wo.owner_uid = u.uid
    LEFT JOIN month_plans mp
        ON mp.workspace_id = b.workspace_id AND mp.month = DATE_TRUNC(b.stat_date, MONTH)
    LEFT JOIN monthly_meeting_labels ml
        ON b.uid = ml.uid AND ml.month_start = DATE_TRUNC(b.stat_date, MONTH)
    WHERE
       (u.email IS NULL OR (
            u.email NOT LIKE '%@airgram.io' AND u.email NOT LIKE '%@notta.ai'
            AND u.email NOT LIKE '%@langogo%'
            AND u.email NOT LIKE '%@chacuo.net'
            AND u.email NOT LIKE '%@uuf.me'
            AND u.email NOT LIKE '%@nqmo.com'
            AND u.email NOT LIKE '%@linshiyouxiang.net'
            AND u.email NOT LIKE '%@besttempmail.com'
            AND u.email NOT LIKE '%@celebrityfull.com'
            AND u.email NOT LIKE '%@comparisions.net'
            AND u.email NOT LIKE '%@mediaholy.com'
            AND u.email NOT LIKE '%@maillazy.com'
            AND u.email NOT LIKE '%@justdefinition.com'
            AND u.email NOT LIKE '%@inctart.com'
            AND u.email NOT LIKE '%@deepyinc.com'
       ))
),

-- 8. 最终合并与输出
all_source_data AS (
    SELECT * FROM source_daily
    UNION ALL
    SELECT * FROM source_weekly
    UNION ALL
    SELECT * FROM source_monthly
)

SELECT
    grain,
    stat_date,

    -- 维度列 (COALESCE 处理 ALL)
    COALESCE(signup_country, 'ALL') AS dim_country,
    COALESCE(plan_type, 'ALL') AS dim_plan_type,
    COALESCE(platform_group, 'ALL') AS dim_platform,
    COALESCE(transcription_type, 'ALL') AS dim_transcription,
    COALESCE(user_lifecycle, 'ALL') AS dim_lifecycle,

    -- 【基础指标】
    COUNT(DISTINCT uid) AS Active_Users,

    -- 【行为分层指标】
    COUNT(DISTINCT CASE WHEN manual_usage_count > 0 THEN uid END) AS Manual_Active_Users,
    COUNT(DISTINCT CASE WHEN auto_usage_count > 0 THEN uid END) AS Auto_Active_Users,

    -- 【会议用户指标】
    COUNT(DISTINCT CASE WHEN meeting_user_status = 'Meeting User' THEN uid END) AS Meeting_User_Active_Users

FROM all_source_data
GROUP BY GROUPING SETS (
    -- 1. 全局大盘 (保留，以防万一)
    (grain, stat_date),

    -- 2. 各维度组合 (您要求的)
    (grain, stat_date, signup_country, plan_type, user_lifecycle),
    (grain, stat_date, signup_country, plan_type, user_lifecycle, transcription_type),
    (grain, stat_date, signup_country, plan_type, user_lifecycle, platform_group)
)