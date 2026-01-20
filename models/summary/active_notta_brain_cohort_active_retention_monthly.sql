WITH
-- =================================================
-- 1-6. 基础数据准备 (User, Plans, Brain Activity)
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
month_plans as (
    select
        CAST(workspace_id AS INT64) as workspace_id,
        date(timestamp_trunc(calendar_date, MONTH)) as month, -- Truncate to Month
        max(goods_plan_type) as goods_plan_type
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
brain_monthly_activity AS (
    SELECT b.uid, DATE_TRUNC(b.stat_date, MONTH) as record_month, b.workspace_id, SUM(b.usage_count) as monthly_usage
    FROM daily_brain_behavior b GROUP BY 1, 2, 3
),
user_first_brain_active AS (
    SELECT a.uid, MIN(a.record_month) as first_active_month, MIN_BY(a.workspace_id, a.record_month) as first_workspace_id
    FROM brain_monthly_activity a GROUP BY 1
),

-- =================================================
-- 7. Define Cohort Attributes (Group By)
-- =================================================
cohort_users_enriched AS (
    SELECT
        fa.uid,
        fa.first_active_month as cohort_month,
        COALESCE(u.signup_country, 'Other') as country_group,
        COALESCE(u.register_platform, 'meeting') AS register_platform,
        COALESCE(u.signup_platform, 'Other') AS signup_platform,
        CASE WHEN mp.goods_plan_type IS NULL THEN 'Free' WHEN mp.goods_plan_type=0 THEN 'Starter' WHEN mp.goods_plan_type=1 THEN 'Pro' WHEN mp.goods_plan_type=2 THEN 'Biz' WHEN mp.goods_plan_type=3 THEN 'Enterprise' ELSE 'else' END as cohort_plan_detail,
        CASE WHEN SAFE_DIVIDE(SUM(rs.record_duration)/60, SUM(rs.total_record_cnt)) > 30 THEN 'Meeting User' ELSE 'Non-Meeting User' END as cohort_meeting_status,
        CASE WHEN mp.goods_plan_type IS NOT NULL THEN 'Paid Cohort' ELSE 'Free Cohort' END as cohort_type
    FROM user_first_brain_active fa
    LEFT JOIN user_dim_dedup u ON CAST(fa.uid AS INT64) = CAST(u.uid AS INT64)
    LEFT JOIN month_plans mp ON CAST(fa.first_workspace_id AS INT64) = CAST(mp.workspace_id AS INT64) AND fa.first_active_month = mp.month
    LEFT JOIN daily_record_stats_base rs ON CAST(fa.uid AS INT64) = CAST(rs.uid AS INT64) AND DATE_TRUNC(rs.stat_date, MONTH) = fa.first_active_month
    GROUP BY 1, 2, 3, 4, 5, 6, 8
),

-- =================================================
-- 8. Calculate Fixed Initial Users (The Denominator)
--    This is the "Source of Truth" for cohort size.
-- =================================================
fixed_initial_stats AS (
    SELECT
        cohort_type, cohort_month, country_group, register_platform, signup_platform, cohort_plan_detail, cohort_meeting_status,
        COUNT(DISTINCT uid) as fixed_initial_users,
        COUNT(DISTINCT CASE WHEN cohort_meeting_status = 'Meeting User' THEN uid ELSE NULL END) as fixed_initial_meeting_users
    FROM cohort_users_enriched
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- =================================================
-- 9. Calculate Actual Retention Activity (The Numerator)
-- =================================================
all_activity_stream AS ( SELECT uid, record_month FROM brain_monthly_activity GROUP BY 1, 2 ),
monthly_meeting_labels AS (
    SELECT DATE_TRUNC(stat_date, MONTH) as month_start, uid, CASE WHEN SAFE_DIVIDE(SUM(record_duration)/60, SUM(total_record_cnt)) > 30 THEN 'Meeting User' ELSE 'Non-Meeting User' END as label
    FROM daily_record_stats_base GROUP BY 1, 2
),
retention_activity AS (
    SELECT
        c.cohort_type, c.cohort_month, c.country_group, c.register_platform, c.signup_platform, c.cohort_plan_detail, c.cohort_meeting_status,
        DATE_DIFF(a.record_month, c.cohort_month, MONTH) as month_diff,
        COUNT(DISTINCT c.uid) as active_users,
        COUNT(DISTINCT CASE WHEN ml.label = 'Meeting User' THEN c.uid ELSE NULL END) as retained_meeting_active_users
    FROM cohort_users_enriched c
    JOIN all_activity_stream a ON c.uid = a.uid
    LEFT JOIN monthly_meeting_labels ml ON c.uid = ml.uid AND a.record_month = ml.month_start
    WHERE a.record_month >= c.cohort_month
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- =================================================
-- 10. Build Monthly Skeleton (Scaffolding)
--    Create all combinations of [Cohort Dimensions] * [Month Diff 0-24]
-- =================================================
month_series AS (
    -- Generate 0 to 24 for Monthly Retention (2 Years)
    SELECT * FROM UNNEST(GENERATE_ARRAY(0, 24)) as month_diff
),
skeleton AS (
    SELECT
        i.*, -- Contains all cohort dimensions + fixed_initial_users
        ms.month_diff
    FROM fixed_initial_stats i
    CROSS JOIN month_series ms
    -- Optional: filter out future months
    WHERE DATE_ADD(i.cohort_month, INTERVAL ms.month_diff MONTH) <= CURRENT_DATE()
),

-- =================================================
-- 11. Fill Skeleton with Data
-- =================================================
final_dataset AS (
    SELECT
        s.cohort_type,
        s.cohort_month,
        s.country_group,
        s.register_platform,
        s.signup_platform,
        s.cohort_plan_detail,
        s.cohort_meeting_status,
        s.month_diff,

        -- 1. Initial Users (From Skeleton, never missing)
        s.fixed_initial_users as cohort_initial_users,
        s.fixed_initial_meeting_users as cohort_initial_meeting_active_users,

        -- 2. Active Users (Fill 0 if missing)
        COALESCE(r.active_users, 0) as active_users,
        COALESCE(r.retained_meeting_active_users, 0) as retained_meeting_active_users

    FROM skeleton s
    LEFT JOIN retention_activity r
        ON s.cohort_type = r.cohort_type
        AND s.cohort_month = r.cohort_month
        AND s.country_group = r.country_group
        AND s.register_platform = r.register_platform
        AND s.signup_platform = r.signup_platform
        AND s.cohort_plan_detail = r.cohort_plan_detail
        AND s.cohort_meeting_status = r.cohort_meeting_status
        AND s.month_diff = r.month_diff
)

-- =================================================
-- 12. Final Output with LEAD
-- =================================================
SELECT
    *,
    -- Next Month Active (Safe to use LEAD now)
    LEAD(active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_month, country_group, register_platform, signup_platform, cohort_plan_detail, cohort_meeting_status
        ORDER BY month_diff ASC
    ) as next_month_active_users,

    -- Next Month Meeting Active
    LEAD(retained_meeting_active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_month, country_group, register_platform, signup_platform, cohort_plan_detail, cohort_meeting_status
        ORDER BY month_diff ASC
    ) as next_month_meeting_active_users

FROM final_dataset
ORDER BY cohort_type, cohort_month, month_diff