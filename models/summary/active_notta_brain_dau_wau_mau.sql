

WITH
-- 1. 基础维表 (核心清洗层)
user_basic AS (
    SELECT
        CAST(u.uid AS INT64) AS uid,

        -- 清洗国家
        CASE
            WHEN u.signup_country IN ('Japan','United States','unknown','United Kingdom','France','Canada')  THEN u.signup_country
            ELSE 'Other'
        END AS signup_country,

        -- 清洗注册端
        CASE
            WHEN u.signup_platform IN ('WEB', 'ANDROID', 'IOS') THEN u.signup_platform
            ELSE 'Other'
        END AS signup_platform,

        -- 清洗注册来源 (处理 NULL、空字符串、'null' 字符串)
        CASE
            WHEN raw_u.register_platform IS NULL THEN 'meeting'
            WHEN TRIM(CAST(raw_u.register_platform AS STRING)) = '' THEN 'meeting'
            WHEN LOWER(TRIM(CAST(raw_u.register_platform AS STRING))) = 'null' THEN 'meeting'
            ELSE CAST(raw_u.register_platform AS STRING)
        END AS register_platform

    FROM `notta-data-analytics.dbt_models_details.user_details` u
    LEFT JOIN `notta-data-analytics.notta_aurora.langogo_user_space_users` raw_u
        ON CAST(u.uid AS INT64) = CAST(raw_u.uid AS INT64)
    WHERE u.pt = date_add(date(current_timestamp()), interval -1 DAY)
    -- 黑名单过滤
    AND (u.email IS NULL OR (
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

ws_owner AS (
    SELECT
        CAST(workspace_id AS INT64) as workspace_id,
        CAST(owner_uid AS INT64) as owner_uid,
        DATE(TIMESTAMP_MILLIS(CAST(create_time AS INT64))) AS create_date,
        DATE_TRUNC(DATE(TIMESTAMP_MILLIS(CAST(create_time AS INT64))), WEEK(MONDAY)) as create_week,
        DATE_TRUNC(DATE(TIMESTAMP_MILLIS(CAST(create_time AS INT64))), MONTH) as create_month
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
    WHERE owner_uid IS NOT NULL AND workspace_id IS NOT NULL AND status != 2
),

-- 2. 套餐等级
week_plans AS (
    SELECT
        CAST(workspace_id AS INT64) as workspace_id,
        DATE(TIMESTAMP_TRUNC(calendar_date, ISOWEEK)) as week,
        MAX(goods_plan_type) as goods_plan_type
    FROM `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    WHERE is_trial = 0
    GROUP BY 1, 2
),

month_plans AS (
    SELECT
        CAST(workspace_id AS INT64) as workspace_id,
        DATE(TIMESTAMP_TRUNC(calendar_date, MONTH)) as month,
        MAX(goods_plan_type) as goods_plan_type
    FROM `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    WHERE is_trial = 0
    GROUP BY 1, 2
),

-- 3. 全量录音统计 (用于计算 Meeting User 标签)
daily_record_stats_base AS (
    SELECT
        record_date as stat_date,
        CAST(uid AS INT64) as uid,
        SUM(record_duration) / 60.0 as total_duration_min,
        SUM(record_count) as total_record_cnt
    FROM `notta-data-analytics.dbt_models_details.stg_active_user_records_daily`
    WHERE record_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)
    GROUP BY 1, 2
),

-- 4. 会议用户标签预计算
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

-- 5. Notta Brain 行为底表
daily_brain_behavior AS (
    SELECT
        DATE(raw_timestamp) AS stat_date,
        CAST(uid AS INT64) AS uid,
        CAST(workspace_id AS INT64) AS workspace_id,

        -- 基础活跃计数
        COUNT(request_id) AS request_count,

        -- 分类活跃标记
        MAX(is_success) AS has_success_usage,
        MAX(is_agent_start) AS has_agent_usage,
        MAX(is_failure) AS has_failure_usage

    FROM `notta-data-analytics.dbt_models_details.active_notta_brain_request`
    WHERE uid IS NOT NULL
      AND uid != ''
      AND DATE(raw_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 750 DAY)
    GROUP BY 1, 2, 3
),

-- 6. Source CTEs (Daily/Weekly/Monthly)
source_daily AS (
    SELECT
        'Daily' as grain, -- 1
        b.stat_date as stat_date, -- 2
        b.uid, -- 3

        -- 维度
        CASE WHEN wo.create_date = b.stat_date THEN 'New User' ELSE 'Existing User' END AS user_lifecycle, -- 4
        COALESCE(ml.label, 'Non-Meeting User') AS meeting_user_status, -- 5

        -- [稳健写法] 这里的维度如果为NULL，补全为'unknown'或'meeting'等默认值，防止GROUP BY问题
        COALESCE(u.signup_country, 'Other') AS signup_country, -- 6
        COALESCE(u.register_platform, 'meeting') AS register_platform, -- 7
        COALESCE(u.signup_platform, 'Other') AS signup_platform, -- 8

        CASE
            WHEN u.uid IS NULL THEN 'Free'
            WHEN b1.goods_plan_type IS NULL THEN 'Free'
            WHEN b1.goods_plan_type = 0 THEN 'Starter'
            WHEN b1.goods_plan_type = 1 THEN 'Pro'
            WHEN b1.goods_plan_type = 2 THEN 'Biz'
            WHEN b1.goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type, -- 9

        -- 指标
        b.has_success_usage,
        b.has_agent_usage

    FROM daily_brain_behavior b
    INNER JOIN ws_owner wo ON b.workspace_id = wo.workspace_id
    LEFT JOIN user_basic u ON wo.owner_uid = u.uid
    LEFT JOIN `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan` b1
        ON b1.workspace_id = b.workspace_id AND b1.calendar_date = b.stat_date
    LEFT JOIN daily_meeting_labels ml
        ON b.uid = ml.uid AND b.stat_date = ml.stat_date
    -- 黑名单过滤已在 user_basic 中完成，这里只需过滤 u.uid 不为空的情况（如果只看有效用户）
    -- 或者保留 NULL 用户归为 Guest
),

-- [Source Weekly]
source_weekly AS (
    SELECT
        'Weekly' as grain, -- 1
        DATE_TRUNC(b.stat_date, WEEK(MONDAY)) as stat_date, -- 2
        b.uid, -- 3

        CASE WHEN wo.create_week = DATE_TRUNC(b.stat_date, WEEK(MONDAY)) THEN 'New User' ELSE 'Existing User' END AS user_lifecycle, -- 4
        COALESCE(ml.label, 'Non-Meeting User') AS meeting_user_status, -- 5
        COALESCE(u.signup_country, 'Other') AS signup_country, -- 6
        COALESCE(u.register_platform, 'meeting') AS register_platform, -- 7
        COALESCE(u.signup_platform, 'Other') AS signup_platform, -- 8

        CASE
            WHEN u.uid IS NULL THEN 'Free'
            WHEN wp.goods_plan_type IS NULL THEN 'Free'
            WHEN wp.goods_plan_type = 0 THEN 'Starter'
            WHEN wp.goods_plan_type = 1 THEN 'Pro'
            WHEN wp.goods_plan_type = 2 THEN 'Biz'
            WHEN wp.goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type, -- 9

        MAX(b.has_success_usage) as has_success_usage,
        MAX(b.has_agent_usage) as has_agent_usage

    FROM daily_brain_behavior b
    INNER JOIN ws_owner wo ON b.workspace_id = wo.workspace_id
    LEFT JOIN user_basic u ON wo.owner_uid = u.uid
    LEFT JOIN week_plans wp
        ON wp.workspace_id = b.workspace_id AND wp.week = DATE_TRUNC(b.stat_date, WEEK(MONDAY))
    LEFT JOIN weekly_meeting_labels ml
        ON b.uid = ml.uid AND ml.week_start = DATE_TRUNC(b.stat_date, WEEK(MONDAY))

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- [Source Monthly]
source_monthly AS (
    SELECT
        'Monthly' as grain, -- 1
        DATE_TRUNC(b.stat_date, MONTH) as stat_date, -- 2
        b.uid, -- 3

        CASE WHEN wo.create_month = DATE_TRUNC(b.stat_date, MONTH) THEN 'New User' ELSE 'Existing User' END AS user_lifecycle, -- 4
        COALESCE(ml.label, 'Non-Meeting User') AS meeting_user_status, -- 5
        COALESCE(u.signup_country, 'Other') AS signup_country, -- 6
        COALESCE(u.register_platform, 'meeting') AS register_platform, -- 7
        COALESCE(u.signup_platform, 'Other') AS signup_platform, -- 8

        CASE
            WHEN u.uid IS NULL THEN 'Free'
            WHEN mp.goods_plan_type IS NULL THEN 'Free'
            WHEN mp.goods_plan_type = 0 THEN 'Starter'
            WHEN mp.goods_plan_type = 1 THEN 'Pro'
            WHEN mp.goods_plan_type = 2 THEN 'Biz'
            WHEN mp.goods_plan_type = 3 THEN 'Enterprise'
            ELSE 'Other'
        END AS plan_type, -- 9

        MAX(b.has_success_usage) as has_success_usage,
        MAX(b.has_agent_usage) as has_agent_usage

    FROM daily_brain_behavior b
    INNER JOIN ws_owner wo ON b.workspace_id = wo.workspace_id
    LEFT JOIN user_basic u ON wo.owner_uid = u.uid
    LEFT JOIN month_plans mp
        ON mp.workspace_id = b.workspace_id AND mp.month = DATE_TRUNC(b.stat_date, MONTH)
    LEFT JOIN monthly_meeting_labels ml
        ON b.uid = ml.uid AND ml.month_start = DATE_TRUNC(b.stat_date, MONTH)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- 7. 最终合并与输出
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

    -- 维度列 (COALESCE 使用 'Total' 替代 'ALL')
    COALESCE(signup_country, 'Total') AS dim_country,
    COALESCE(plan_type, 'Total') AS dim_plan_type,
    COALESCE(user_lifecycle, 'Total') AS dim_lifecycle,
    COALESCE(register_platform, 'Total') AS dim_register_platform,
--    COALESCE(signup_platform, 'Total') AS dim_signup_platform,

    -- 【基础活跃指标】
    COUNT(DISTINCT uid) AS Brain_Active_Users,

    -- 【行为分层指标】
    COUNT(DISTINCT CASE WHEN has_success_usage = 1 THEN uid END) AS Brain_Success_Active_Users,
    COUNT(DISTINCT CASE WHEN has_agent_usage = 1 THEN uid END) AS Brain_Agent_Active_Users,

    -- 【会议用户指标】
    COUNT(DISTINCT CASE WHEN meeting_user_status = 'Meeting User' THEN uid END) AS Meeting_User_Brain_Active_Users

FROM all_source_data
GROUP BY GROUPING SETS (
    -- 1. 全局大盘
--    (grain, stat_date),
    -- (grain, stat_date, signup_country, signup_platform),

    -- 4. 复杂组合
    (grain, stat_date, signup_country, plan_type, user_lifecycle, register_platform)
)