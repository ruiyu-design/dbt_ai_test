WITH
-- =================================================
-- 1-6. 基础数据准备 (保持原逻辑)
-- =================================================
user_dim_dedup AS (
    SELECT
        uid,
        case when signup_country in ('Japan','United States','unknown','United Kingdom','France','Canada')  then signup_country else 'Other' end as signup_country
    FROM `notta-data-analytics.dbt_models_details.user_details`
    WHERE pt = date_add(date(current_timestamp()), interval -1 DAY)
    AND (email IS NULL OR (email NOT LIKE '%@airgram.io' AND email NOT LIKE '%@notta.ai' AND email NOT LIKE '%@langogo%' AND email NOT LIKE '%@chacuo.net' AND email NOT LIKE '%@uuf.me' AND email NOT LIKE '%@nqmo.com' AND email NOT LIKE '%@linshiyouxiang.net' AND email NOT LIKE '%@besttempmail.com' AND email NOT LIKE '%@celebrityfull.com' AND email NOT LIKE '%@comparisions.net' AND email NOT LIKE '%@mediaholy.com' AND email NOT LIKE '%@maillazy.com' AND email NOT LIKE '%@justdefinition.com' AND email NOT LIKE '%@inctart.com' AND email NOT LIKE '%@deepyinc.com'))
),
ws_owner AS (
    SELECT workspace_id, CAST(owner_uid AS INT64) as owner_uid
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
    WHERE owner_uid IS NOT NULL AND workspace_id IS NOT NULL AND status != 2
),
week_plans as (
    select workspace_id, date(timestamp_trunc(calendar_date, ISOWEEK)) as week, max(goods_plan_type) as goods_plan_type, max_by(period_unit, goods_plan_type) as period_unit, max_by(period_count, goods_plan_type) as period_count
    from `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    where is_trial = 0 and period_count != 3
    group by 1, 2
),
daily_record_stats_base AS (
    SELECT DATE(create_date) AS stat_date, CAST(creator_uid AS INT64) AS uid, COUNT(record_id) AS total_record_cnt,
    COUNT(CASE WHEN transcription_type IN (3,4,11) THEN record_id WHEN transcription_type NOT IN (7,9,12) AND audio_duration > 1800 THEN record_id ELSE NULL END) AS meeting_record_cnt,
    sum(audio_duration) as record_duration
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_record`
    WHERE TIMESTAMP(create_date) >= TIMESTAMP('2023-01-01') GROUP BY 1, 2
),
daily_behavior AS (
    SELECT DATE(TIMESTAMP_SECONDS(timestamp)) AS stat_date, CAST(uid AS INT64) AS uid, workspace_id, CASE WHEN platform IN ('Web','Server') THEN 'WEB' ELSE platform END AS platform_group, COUNT(1) AS usage_count
    FROM `notta-data-analytics.mc_data_statistics.notta_summary_ai_records`
    WHERE uid IS NOT NULL GROUP BY 1, 2, 3, 4
),
summary_weekly_activity AS (
    SELECT b.uid, DATE_TRUNC(b.stat_date, WEEK(MONDAY)) as record_week, b.platform_group as device, b.workspace_id, SUM(b.usage_count) as weekly_usage
    FROM daily_behavior b GROUP BY 1, 2, 3, 4
),
user_first_summary_active AS (
    SELECT a.uid, MIN(a.record_week) as first_active_week, MIN_BY(a.workspace_id, a.record_week) as first_workspace_id, MIN_BY(a.device, a.record_week) as first_device
    FROM summary_weekly_activity a GROUP BY 1
),

-- =================================================
-- 7. 丰富 Cohort 属性 (定义每个用户的画像)
-- =================================================
cohort_users_enriched AS (
    SELECT
        fa.uid,
        fa.first_active_week as cohort_week,
        IFNULL(u.signup_country, 'unknown') as country_group,
        CASE
            WHEN wp.goods_plan_type IS NULL THEN 'Free'
            ELSE concat(
                CASE WHEN wp.goods_plan_type=0 THEN 'Starter' WHEN wp.goods_plan_type=1 THEN 'Pro' WHEN wp.goods_plan_type=2 THEN 'Biz' WHEN wp.goods_plan_type=3 THEN 'Enterprise' ELSE 'else' END, '-',
                CASE WHEN wp.period_unit = 2 AND wp.period_count = 12 THEN 'annually-1' WHEN wp.period_unit = 1 AND wp.period_count = 1 THEN 'annually-1' ELSE concat(CASE WHEN wp.period_unit=2 THEN 'monthly' ELSE 'annually' END, '-', wp.period_count) END
            )
        END as cohort_plan_detail,
        -- Meeting Status
        case when safe_divide(sum(rs.record_duration)/60,sum(rs.total_record_cnt))>30 then 'Meeting User' else 'Non-Meeting User' end as cohort_meeting_status,
        -- Cohort Type
        CASE WHEN wp.goods_plan_type IS NOT NULL THEN 'Paid Cohort' ELSE 'Free Cohort' END as cohort_type
    FROM user_first_summary_active fa
    LEFT JOIN user_dim_dedup u ON fa.uid = u.uid
    LEFT JOIN week_plans wp ON wp.workspace_id = fa.first_workspace_id AND wp.week = fa.first_active_week
    LEFT JOIN daily_record_stats_base rs ON fa.uid = rs.uid AND DATE_TRUNC(rs.stat_date, WEEK(MONDAY)) = fa.first_active_week
    WHERE fa.first_active_week >= '2024-01-01'
    GROUP BY 1, 2, 3, 4, 6
),

-- =================================================
-- 8. 构造活跃流水 (包含分端和Total)
-- =================================================
all_activity_stream AS (
    SELECT uid, record_week, device FROM summary_weekly_activity
    UNION ALL
    SELECT uid, record_week, 'Total' as device FROM summary_weekly_activity
    GROUP BY 1, 2, 3
),

weekly_meeting_labels AS (
    SELECT DATE_TRUNC(stat_date, WEEK(MONDAY)) as week_start, uid, case when safe_divide(sum(record_duration)/60,sum(total_record_cnt))>30 then 'Meeting User' else 'Non-Meeting User' end as label
    FROM daily_record_stats_base GROUP BY 1, 2
),

-- =================================================
-- 9. 计算【固定初始人数】 (分母来源)
--    关键：这里要 Join all_activity_stream (Week 0) 来区分 'Total' 和具体 Device
-- =================================================
fixed_initial_stats AS (
    SELECT
        c.cohort_type,
        c.cohort_week,
        c.country_group,
        c.cohort_plan_detail,
        c.cohort_meeting_status,
        -- [关键] 按照 Week 0 活跃的设备分组 (包含 Total)
        start_active.device,

        COUNT(DISTINCT c.uid) as fixed_initial_users,
        COUNT(DISTINCT CASE WHEN c.cohort_meeting_status = 'Meeting User' THEN c.uid END) as fixed_initial_meeting_users
    FROM cohort_users_enriched c
    -- 关联 Week 0 的活跃记录，膨胀出 Total 行
    JOIN all_activity_stream start_active
        ON c.uid = start_active.uid
        AND c.cohort_week = start_active.record_week
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- =================================================
-- 10. 计算【实际活跃人数】 (分子来源)
-- =================================================
retention_activity AS (
    SELECT
        c.cohort_type,
        c.cohort_week,
        c.country_group,
        c.cohort_plan_detail,
        c.cohort_meeting_status,
        start_active.device, -- 保持与分母一致的设备维度

        DATE_DIFF(a.record_week, c.cohort_week, WEEK) as week_diff,

        COUNT(DISTINCT c.uid) as active_users,
        COUNT(DISTINCT CASE WHEN ml.label = 'Meeting User' THEN c.uid ELSE NULL END) as retained_meeting_active_users
    FROM cohort_users_enriched c
    -- 1. 先找到用户的起始设备 (含 Total)
    JOIN all_activity_stream start_active
        ON c.uid = start_active.uid AND c.cohort_week = start_active.record_week
    -- 2. 再找未来的活跃 (必须是同设备类型)
    LEFT JOIN all_activity_stream a
        ON c.uid = a.uid
        AND a.record_week >= c.cohort_week
        AND start_active.device = a.device
    LEFT JOIN weekly_meeting_labels ml
        ON c.uid = ml.uid AND a.record_week = ml.week_start
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- =================================================
-- 11. 构建周度骨架 (Skeleton)
--    生成所有维度的 0-24 周组合
-- =================================================
week_series AS (
    -- 生成 0 到 24 周的序列 (约半年，可根据需求改为 52)
    SELECT * FROM UNNEST(GENERATE_ARRAY(0, 24)) as week_diff
),
skeleton AS (
    SELECT
        i.*, -- 包含所有维度 + fixed_initial_users
        ws.week_diff
    FROM fixed_initial_stats i
    CROSS JOIN week_series ws
    -- 过滤掉未来的时间
    WHERE DATE_ADD(i.cohort_week, INTERVAL ws.week_diff WEEK) <= CURRENT_DATE()
),

-- =================================================
-- 12. 骨架回填 (Fill Data)
-- =================================================
final_dataset AS (
    SELECT
        s.cohort_type,
        s.cohort_week,
        s.country_group,
        s.cohort_plan_detail,
        s.cohort_meeting_status,
        s.device,
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
        AND s.cohort_plan_detail = r.cohort_plan_detail
        AND s.cohort_meeting_status = r.cohort_meeting_status
        AND s.device = r.device
        AND s.week_diff = r.week_diff
)

-- =================================================
-- 13. 最终输出 (含 LEAD 计算)
-- =================================================
SELECT
    *,
    -- Next Week Active
    LEAD(active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_week, country_group, cohort_plan_detail, cohort_meeting_status, device
        ORDER BY week_diff ASC
    ) as next_week_active_users,

    -- Next Week Meeting Active
    LEAD(retained_meeting_active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_week, country_group, cohort_plan_detail, cohort_meeting_status, device
        ORDER BY week_diff ASC
    ) as next_week_meeting_active_users

FROM final_dataset
ORDER BY cohort_type, cohort_week, device, week_diff