

WITH
-- 1. 基础维表
user_basic AS (
    SELECT
        CAST(u.uid AS INT64) AS uid,

        -- 国家维度
        CASE
            WHEN u.signup_country IN ('Japan','United States','unknown','United Kingdom','France','Canada')  THEN u.signup_country
            ELSE 'Other'
        END AS country_group,

        -- 注册端 (作为 Device 维度)
        CASE
            WHEN u.signup_platform IN ('WEB', 'ANDROID', 'IOS') THEN u.signup_platform
            ELSE 'Other'
        END AS signup_platform,

        -- 注册来源 (清洗 NULL/'null'/空字符串 -> 'meeting')
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

-- 2. 月套餐逻辑
month_plans as (
    select
        CAST(workspace_id AS INT64) as workspace_id,
        date(timestamp_trunc(calendar_date, MONTH)) as month,
        max(goods_plan_type) as goods_plan_type,
        max_by(period_unit, goods_plan_type) as period_unit,
        max_by(period_count, goods_plan_type) as period_count
    from `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    where is_trial = 0
    group by 1, 2
),

-- 3. 录音行为统计 (基础数据)
daily_record_stats_base AS (
    SELECT
        DATE(create_date) AS stat_date,
        CAST(creator_uid AS INT64) AS uid,
        COUNT(record_id) AS total_record_cnt,
        SUM(audio_duration) as record_duration
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_record`
    WHERE TIMESTAMP(create_date) >= TIMESTAMP('2023-01-01')
    GROUP BY 1, 2
),

-- 4. Notta Brain 行为底表
daily_brain_behavior AS (
    SELECT
        DATE(raw_timestamp) AS stat_date,
        CAST(uid AS INT64) AS uid,
        CAST(workspace_id AS INT64) AS workspace_id,
        COUNT(request_id) AS usage_count
    FROM `notta-data-analytics.dbt_models_details.active_notta_brain_request`
    WHERE uid IS NOT NULL
      AND uid != ''
      AND DATE(raw_timestamp) >= '2024-01-01'
    GROUP BY 1, 2, 3
),

-- 5. Brain 每月活跃流水 (Monthly Activity Stream)
brain_monthly_activity AS (
    SELECT
        b.uid,
        DATE_TRUNC(b.stat_date, MONTH) as activity_month,
        -- 取该月最后活跃的 workspace 用于关联套餐
        MAX_BY(b.workspace_id, b.stat_date) as workspace_id,
        SUM(b.usage_count) as monthly_usage
    FROM daily_brain_behavior b
    GROUP BY 1, 2
),

-- 6. 定义 Cohort (用户首次使用 Brain 的月)
user_first_brain_month AS (
    SELECT
        CAST(uid AS INT64) as uid,
        MIN(DATE_TRUNC(DATE(raw_timestamp), MONTH)) as first_active_month
    FROM `notta-data-analytics.dbt_models_details.active_notta_brain_request`
    WHERE uid IS NOT NULL AND uid != ''
    GROUP BY 1
),

-- 7. 丰富每月用户状态 (打标签：Plan, Meeting, New/Old)
monthly_user_status_enriched AS (
    SELECT
        a.uid,
        a.activity_month,

        -- 维度：设备 (使用注册端作为代理)
        COALESCE(u.signup_platform, 'Other') as device,
        -- 维度：注册来源
        COALESCE(u.register_platform, 'meeting') as register_platform,
        -- 维度：国家
        COALESCE(u.country_group, 'unknown') as country_group,

        -- [核心] 用户生命周期状态 (New vs Old)
        CASE
            WHEN a.activity_month = ufb.first_active_month THEN 'New User'
            WHEN a.activity_month > ufb.first_active_month THEN 'Old User'
            ELSE 'Other' -- 理论上不应存在
        END as user_lifecycle_type,

        -- 标签：套餐 (Plan)
        CASE
            WHEN u.uid IS NULL THEN 'Guest/Deleted/Internal' -- 关联不到用户
            WHEN mp.goods_plan_type IS NULL THEN 'Free'
                    WHEN mp.goods_plan_type=0 THEN 'Starter'
                    WHEN mp.goods_plan_type=1 THEN 'Pro'
                    WHEN mp.goods_plan_type=2 THEN 'Biz'
                    WHEN mp.goods_plan_type=3 THEN 'Enterprise'
                    ELSE 'else'
        END as plan_detail,

        -- 标签：会议用户 (Meeting Status)
        -- 逻辑：月平均录音时长 > 30分钟
        CASE
            WHEN SAFE_DIVIDE(SUM(rs.record_duration)/60, SUM(rs.total_record_cnt)) > 30 THEN 'Meeting User'
            ELSE 'Non-Meeting User'
        END as meeting_status

    FROM brain_monthly_activity a
    LEFT JOIN user_basic u ON a.uid = u.uid
    LEFT JOIN user_first_brain_month ufb ON a.uid = ufb.uid
    -- 关联月度套餐
    LEFT JOIN month_plans mp ON a.workspace_id = mp.workspace_id AND a.activity_month = mp.month
    -- 关联录音统计 (按月聚合)
    LEFT JOIN daily_record_stats_base rs ON a.uid = rs.uid AND DATE_TRUNC(rs.stat_date, MONTH) = a.activity_month

    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

-- 8. 聚合输出：本月活跃 vs 次月留存
SELECT
    -- 维度
    base.activity_month as month,
    base.user_lifecycle_type,   -- New User / Old User
    base.country_group,
    base.register_platform,     -- meeting / brain
    base.plan_detail,
    base.meeting_status,
    base.device,                -- WEB / ANDROID / IOS / Other

    -- 指标 1：本月活跃用户数（基数）
    COUNT(DISTINCT base.uid) as current_month_active_users,

    -- 指标 2：次月留存用户数（分子）
    -- 逻辑：用户在 Month N 活跃，且在 Month N+1 也活跃
    COUNT(DISTINCT next_month.uid) as next_month_retained_users

FROM monthly_user_status_enriched base
LEFT JOIN monthly_user_status_enriched next_month
    ON base.uid = next_month.uid
    AND next_month.activity_month = DATE_ADD(base.activity_month, INTERVAL 1 MONTH) -- 仅匹配下一月

GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY month DESC, user_lifecycle_type, device