WITH
-- =================================================
-- 1-6. 基础数据准备 (保持不变)
-- =================================================
user_dim_dedup AS (
    SELECT
        CAST(u.uid AS INT64) AS uid,
        CASE WHEN u.signup_country IN ('Japan','United States','unknown','United Kingdom','France','Canada')  THEN u.signup_country ELSE 'Other' END AS signup_country,
        CASE WHEN u.signup_platform IN ('WEB', 'ANDROID', 'IOS') THEN u.signup_platform ELSE 'Other' END AS signup_platform,
        CASE WHEN raw_u.register_platform IS NULL OR TRIM(CAST(raw_u.register_platform AS STRING)) = '' OR LOWER(TRIM(CAST(raw_u.register_platform AS STRING))) = 'null' THEN 'meeting' ELSE CAST(raw_u.register_platform AS STRING) END AS register_platform
    FROM `notta-data-analytics.dbt_models_details.user_details` u
    LEFT JOIN `notta-data-analytics.notta_aurora.langogo_user_space_users` raw_u ON CAST(u.uid AS INT64) = CAST(raw_u.uid AS INT64)
    WHERE u.pt = date_add(date(current_timestamp()), interval -1 DAY)
    AND (u.email IS NULL OR (u.email NOT LIKE '%@airgram.io' AND u.email NOT LIKE '%@notta.ai' AND u.email NOT LIKE '%@langogo%' AND u.email NOT LIKE '%@chacuo.net' AND u.email NOT LIKE '%@uuf.me' AND u.email NOT LIKE '%@nqmo.com' AND u.email NOT LIKE '%@linshiyouxiang.net' AND u.email NOT LIKE '%@besttempmail.com' AND u.email NOT LIKE '%@celebrityfull.com' AND u.email NOT LIKE '%@comparisions.net' AND u.email NOT LIKE '%@mediaholy.com' AND u.email NOT LIKE '%@maillazy.com' AND u.email NOT LIKE '%@justdefinition.com' AND u.email NOT LIKE '%@inctart.com' AND u.email NOT LIKE '%@deepyinc.com'))
),
week_plans as (
    select
        CAST(workspace_id AS INT64) as workspace_id,
        date(timestamp_trunc(calendar_date, ISOWEEK)) as week,
        max(goods_plan_type) as goods_plan_type,
        max_by(period_unit, goods_plan_type) as period_unit,
        max_by(period_count, goods_plan_type) as period_count
    from `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    where is_trial = 0 and period_count != 3
    group by 1, 2
),
daily_record_stats_base AS (
    SELECT DATE(create_date) AS stat_date, CAST(creator_uid AS INT64) AS uid, COUNT(record_id) AS total_record_cnt, SUM(audio_duration) as record_duration
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_record`
    WHERE TIMESTAMP(create_date) >= TIMESTAMP('2023-01-01') GROUP BY 1, 2
),
daily_brain_behavior AS (
    SELECT DATE(raw_timestamp) AS stat_date, CAST(uid AS INT64) AS uid, CAST(workspace_id AS INT64) AS workspace_id, COUNT(request_id) AS usage_count
    FROM `notta-data-analytics.dbt_models_details.active_notta_brain_request`
    WHERE uid IS NOT NULL AND uid != '' AND DATE(raw_timestamp) >= '2024-01-01' GROUP BY 1, 2, 3
),
brain_weekly_activity AS (
    SELECT b.uid, DATE_TRUNC(b.stat_date, ISOWEEK) as record_week, b.workspace_id, SUM(b.usage_count) as weekly_usage
    FROM daily_brain_behavior b GROUP BY 1, 2, 3
),
user_first_brain_active AS (
    SELECT a.uid, MIN(a.record_week) as first_active_week, MIN_BY(a.workspace_id, a.record_week) as first_workspace_id
    FROM brain_weekly_activity a GROUP BY 1
),
-- 清洗用户属性 (每个用户的单行记录)
cohort_users_enriched AS (
    SELECT
        fa.uid,
        fa.first_active_week as cohort_week,
        COALESCE(u.signup_country, 'Other') as country_group,
        COALESCE(u.register_platform, 'meeting') AS register_platform,
        COALESCE(u.signup_platform, 'Other') AS signup_platform,
        CASE WHEN wp.goods_plan_type IS NULL THEN 'Free' WHEN wp.goods_plan_type=0 THEN 'Starter' WHEN wp.goods_plan_type=1 THEN 'Pro' WHEN wp.goods_plan_type=2 THEN 'Biz' WHEN wp.goods_plan_type=3 THEN 'Enterprise' ELSE 'else' END as cohort_plan_detail,
        CASE WHEN SAFE_DIVIDE(SUM(rs.record_duration)/60, SUM(rs.total_record_cnt)) > 30 THEN 'Meeting User' ELSE 'Non-Meeting User' END as cohort_meeting_status,
        CASE WHEN wp.goods_plan_type IS NOT NULL THEN 'Paid Cohort' ELSE 'Free Cohort' END as cohort_type
    FROM user_first_brain_active fa
    LEFT JOIN user_dim_dedup u ON CAST(fa.uid AS INT64) = CAST(u.uid AS INT64)
    LEFT JOIN week_plans wp ON CAST(fa.first_workspace_id AS INT64) = CAST(wp.workspace_id AS INT64) AND fa.first_active_week = wp.week
    LEFT JOIN daily_record_stats_base rs ON CAST(fa.uid AS INT64) = CAST(rs.uid AS INT64) AND DATE_TRUNC(rs.stat_date, ISOWEEK) = fa.first_active_week
    GROUP BY 1, 2, 3, 4, 5, 6, 8
),

-- =================================================
-- 7. 计算【固定初始人数】(分母来源)
--    这是唯一真理来源，不管后续怎么 Join，这个数不变
-- =================================================
fixed_initial_stats AS (
    SELECT
        cohort_type, cohort_week, country_group, register_platform, signup_platform, cohort_plan_detail, cohort_meeting_status,
        COUNT(DISTINCT uid) as fixed_initial_users,
        COUNT(DISTINCT CASE WHEN cohort_meeting_status = 'Meeting User' THEN uid ELSE NULL END) as fixed_initial_meeting_users
    FROM cohort_users_enriched
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- =================================================
-- 8. 计算【实际活跃人数】(分子来源)
-- =================================================
all_activity_stream AS ( SELECT uid, record_week FROM brain_weekly_activity GROUP BY 1, 2 ),
weekly_meeting_labels AS (
    SELECT DATE_TRUNC(stat_date, ISOWEEK) as week_start, uid, CASE WHEN SAFE_DIVIDE(SUM(record_duration)/60, SUM(total_record_cnt)) > 30 THEN 'Meeting User' ELSE 'Non-Meeting User' END as label
    FROM daily_record_stats_base GROUP BY 1, 2
),
retention_activity AS (
    SELECT
        c.cohort_type, c.cohort_week, c.country_group, c.register_platform, c.signup_platform, c.cohort_plan_detail, c.cohort_meeting_status,
        DATE_DIFF(a.record_week, c.cohort_week, WEEK) as week_diff,
        COUNT(DISTINCT c.uid) as active_users,
        COUNT(DISTINCT CASE WHEN ml.label = 'Meeting User' THEN c.uid ELSE NULL END) as retained_meeting_active_users
    FROM cohort_users_enriched c
    JOIN all_activity_stream a ON c.uid = a.uid
    LEFT JOIN weekly_meeting_labels ml ON c.uid = ml.uid AND a.record_week = ml.week_start
    WHERE a.record_week >= c.cohort_week
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- =================================================
-- 9. 【核心改进】：构建稠密骨架 (Scaffolding)
--    解决 Week N 无人活跃导致行丢失的问题
-- =================================================
week_series AS (
    -- 生成 0 到 52 的序列 (根据需要调整最大周数)
    SELECT * FROM UNNEST(GENERATE_ARRAY(0, 52)) as week_diff
),
skeleton AS (
    SELECT
        i.*, -- 这里包含了所有的维度 + fixed_initial_users
        ws.week_diff
    FROM fixed_initial_stats i
    CROSS JOIN week_series ws
    -- 过滤掉未来的时间，避免生成无效行
    WHERE DATE_ADD(i.cohort_week, INTERVAL ws.week_diff WEEK) <= CURRENT_DATE()
),

-- =================================================
-- 10. 骨架回填
--    把真实活跃数据 Join 进骨架，没有活跃的填 0
-- =================================================
final_dataset AS (
    SELECT
        s.cohort_type,
        s.cohort_week,
        s.country_group,
        s.register_platform,
        s.signup_platform,
        s.cohort_plan_detail,
        s.cohort_meeting_status,
        s.week_diff,

        -- 1. 初始人数 (来自骨架，绝不丢失)
        s.fixed_initial_users as cohort_initial_users,
        s.fixed_initial_meeting_users as cohort_initial_meeting_active_users,

        -- 2. 活跃人数 (关联不上则补0)
        COALESCE(r.active_users, 0) as active_users,
        COALESCE(r.retained_meeting_active_users, 0) as retained_meeting_active_users

    FROM skeleton s
    LEFT JOIN retention_activity r
        ON s.cohort_type = r.cohort_type
        AND s.cohort_week = r.cohort_week
        AND s.country_group = r.country_group
        AND s.register_platform = r.register_platform
        AND s.signup_platform = r.signup_platform
        AND s.cohort_plan_detail = r.cohort_plan_detail
        AND s.cohort_meeting_status = r.cohort_meeting_status
        AND s.week_diff = r.week_diff
)

-- =================================================
-- 11. 最终输出 (应用 LEAD)
-- =================================================
SELECT
    *,
    -- 计算次周活跃 (LEAD)
    LEAD(active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_week, country_group, register_platform, signup_platform, cohort_plan_detail, cohort_meeting_status
        ORDER BY week_diff ASC
    ) as next_week_active_users,

    -- 计算次周会议活跃 (LEAD)
    LEAD(retained_meeting_active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_week, country_group, register_platform, signup_platform, cohort_plan_detail, cohort_meeting_status
        ORDER BY week_diff ASC
    ) as next_week_meeting_active_users

FROM final_dataset
ORDER BY cohort_type, cohort_week, week_diff