WITH
-- 1. 基础维表：用户注册信息 & 黑名单过滤
user_basic AS (
    SELECT
        uid,
        -- 原有的注册时间逻辑保留，用于排除黑名单
        CASE
            WHEN signup_country IN ('Japan','United States','unknown','United Kingdom','France','Canada')  THEN signup_country
            ELSE 'Other'
        END as country_group
    FROM `notta-data-analytics.dbt_models_details.user_details`
    WHERE pt = date_add(date(current_timestamp()), interval -1 DAY)
    AND (email IS NULL OR (
        email NOT LIKE '%@airgram.io' AND email NOT LIKE '%@notta.ai'
        AND email NOT LIKE '%@langogo%'
        AND email NOT LIKE '%@chacuo.net'
        AND email NOT LIKE '%@uuf.me'
        AND email NOT LIKE '%@nqmo.com'
        AND email NOT LIKE '%@linshiyouxiang.net'
        AND email NOT LIKE '%@besttempmail.com'
        AND email NOT LIKE '%@celebrityfull.com'
        AND email NOT LIKE '%@comparisions.net'
        AND email NOT LIKE '%@mediaholy.com'
        AND email NOT LIKE '%@maillazy.com'
        AND email NOT LIKE '%@justdefinition.com'
        AND email NOT LIKE '%@inctart.com'
        AND email NOT LIKE '%@deepyinc.com'
    ))
),

-- 2. 辅助维表：月套餐 & 录音行为
month_plans as (
    select
        workspace_id,
        date(timestamp_trunc(calendar_date, MONTH)) as month, -- 修改：按月截断
        max(goods_plan_type) as goods_plan_type,
        max_by(period_unit, goods_plan_type) as period_unit,
        max_by(period_count, goods_plan_type) as period_count
    from `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    where is_trial = 0
    and period_count != 3
    group by 1, 2
),

daily_record_stats_base AS (
    SELECT
        DATE(create_date) AS stat_date,
        CAST(creator_uid AS INT64) AS uid,
        COUNT(record_id) AS total_record_cnt,
        COUNT(CASE
            WHEN transcription_type IN (3,4,11) THEN record_id
            WHEN transcription_type NOT IN (7,9,12) AND audio_duration > 1800 THEN record_id
            ELSE NULL
        END) AS meeting_record_cnt,
        sum(audio_duration) as record_duration
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_record`
    WHERE TIMESTAMP(create_date) >= TIMESTAMP('2023-01-01')
    GROUP BY 1, 2
),

-- 3. 计算每个用户"首次使用 AI Summary"的月份
user_first_summary_month AS (
    SELECT
        CAST(uid AS INT64) as uid,
        MIN(DATE_TRUNC(DATE(TIMESTAMP_SECONDS(timestamp)), MONTH)) as first_active_month
    FROM `notta-data-analytics.mc_data_statistics.notta_summary_ai_records`
    WHERE uid IS NOT NULL
    GROUP BY 1
),

-- 4. 核心行为流：AI Summary 每月活跃
summary_monthly_raw AS (
    SELECT
        CAST(uid AS INT64) AS uid,
        DATE_TRUNC(DATE(TIMESTAMP_SECONDS(timestamp)), MONTH) as activity_month, -- 修改：按月截断
        CASE WHEN platform IN ('Web','Server') THEN 'WEB' ELSE platform END AS device_source,
        MAX_BY(workspace_id, timestamp) as workspace_id
    FROM `notta-data-analytics.mc_data_statistics.notta_summary_ai_records`
    WHERE uid IS NOT NULL
    AND DATE(TIMESTAMP_SECONDS(timestamp)) >= '2024-01-01'
    GROUP BY 1, 2, 3
),

activity_stream_expanded AS (
    -- 分端数据
    SELECT uid, activity_month, device_source as device, workspace_id
    FROM summary_monthly_raw
    UNION ALL
    -- 汇总端数据
    SELECT uid, activity_month, 'Total' as device, MAX(workspace_id) as workspace_id
    FROM summary_monthly_raw
    GROUP BY 1, 2
),

-- 5. 丰富每月用户状态（打标签：Plan, Meeting, New/Old）
monthly_user_status_enriched AS (
    SELECT
        a.uid,
        a.activity_month,
        a.device,

        -- 【修改点】：基于首次使用月份判断新老用户
        CASE
            -- 如果本月是用户历史上第一次使用 -> New User
            WHEN a.activity_month = ufs.first_active_month THEN 'New User'
            -- 如果本月晚于首次使用月份 -> Old User
            WHEN a.activity_month > ufs.first_active_month THEN 'Old User'
            ELSE 'Other'
        END as user_lifecycle_type,
        IFNULL(u.country_group, 'unknown') as country_group,
        -- Plan Logic (Month)
        CASE
--            WHEN u.uid IS NULL THEN 'Guest/Deleted/Internal Cohort'
            WHEN mp.goods_plan_type IS NULL THEN 'Free'
            WHEN mp.goods_plan_type=0 THEN 'Starter'
            WHEN mp.goods_plan_type=1 THEN 'Pro'
            WHEN mp.goods_plan_type=2 THEN 'Biz'
            WHEN mp.goods_plan_type=3 THEN 'Enterprise'
            ELSE 'else'
        END as plan_detail,

        -- Meeting Status Logic (Month)
--        CASE
--            WHEN SUM(rs.total_record_cnt) <= 4 AND SAFE_DIVIDE(SUM(rs.meeting_record_cnt), SUM(rs.total_record_cnt)) >= 0.5 THEN 'Meeting User'
--            WHEN SUM(rs.total_record_cnt) > 4 AND SAFE_DIVIDE(SUM(rs.meeting_record_cnt), SUM(rs.total_record_cnt)) >= 0.8 THEN 'Meeting User'
--            ELSE 'Non-Meeting User'
--        END as cohort_meeting_status,
        case when safe_divide(sum(rs.record_duration)/60,sum(rs.total_record_cnt))>30 then 'Meeting User'
        else 'Non-Meeting User' end as meeting_status,

    FROM activity_stream_expanded a
    LEFT JOIN user_basic u ON a.uid = u.uid
    LEFT JOIN user_first_summary_month ufs ON a.uid = ufs.uid
    LEFT JOIN month_plans mp ON a.workspace_id = mp.workspace_id AND a.activity_month = mp.month
    LEFT JOIN daily_record_stats_base rs ON a.uid = rs.uid AND DATE_TRUNC(rs.stat_date, MONTH) = a.activity_month
    GROUP BY 1, 2, 3, 4, 5, 6
)

-- 6. 聚合输出：本月活跃 vs 次月留存
SELECT
    -- 维度
    base.activity_month as month,
    base.user_lifecycle_type,   -- 基于功能的 New User / Old User
    base.country_group,
    base.plan_detail,
    base.meeting_status,
    base.device,                -- WEB / iOS / Android / Total

    -- 指标 1：本月活跃用户数（基数）
    COUNT(DISTINCT base.uid) as current_month_active_users,

    -- 指标 2：次月留存用户数（分子）
    COUNT(DISTINCT next_month.uid) as next_month_retained_users

FROM monthly_user_status_enriched base
LEFT JOIN monthly_user_status_enriched next_month
    ON base.uid = next_month.uid
    AND base.device = next_month.device -- 严格匹配端
    AND next_month.activity_month = DATE_ADD(base.activity_month, INTERVAL 1 MONTH) -- 修改：匹配下一月

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY month DESC, user_lifecycle_type, device