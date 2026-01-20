WITH
-- =================================================
-- 1-6. 基础数据准备 (保持不变)
-- =================================================
user_dim_dedup AS (
    SELECT
        uid,
        case when signup_country in ('Japan','United States','unknown','United Kingdom','France','Canada')  then signup_country else 'Other' end as signup_country
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

ws_owner AS (
    SELECT
        workspace_id,
        CAST(owner_uid AS INT64) as owner_uid
    FROM `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
    WHERE owner_uid IS NOT NULL AND workspace_id IS NOT NULL AND status != 2
),

-- 2. 月套餐逻辑
month_plans as (
    select
        workspace_id,
        date(timestamp_trunc(calendar_date, MONTH)) as month,
        max(goods_plan_type) as goods_plan_type,
        max_by(period_unit, goods_plan_type) as period_unit,
        max_by(period_count, goods_plan_type) as period_count
    from `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
    where is_trial = 0
    and period_count != 3
    group by 1, 2
),

-- 3. 录音行为统计 (基础数据)
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

-- 4. AI Summary 行为底表
daily_behavior AS (
    SELECT
        DATE(TIMESTAMP_SECONDS(timestamp)) AS stat_date,
        CAST(uid AS INT64) AS uid,
        workspace_id,
        CASE WHEN platform IN ('Web','Server') THEN 'WEB' ELSE platform END AS platform_group,
        COUNT(1) AS usage_count
    FROM `notta-data-analytics.mc_data_statistics.notta_summary_ai_records`
    WHERE uid IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- 5. 用户 AI Summary 每月活跃流水
summary_monthly_activity AS (
    SELECT
        b.uid,
        DATE_TRUNC(b.stat_date, MONTH) as record_month,
        b.platform_group as device,
        b.workspace_id,
        SUM(b.usage_count) as monthly_usage
    FROM daily_behavior b
    GROUP BY 1, 2, 3, 4
),

-- 6. 定义 Cohort (首次活跃月)
user_first_summary_active AS (
    SELECT
        a.uid,
        MIN(a.record_month) as first_active_month,
        MIN_BY(a.workspace_id, a.record_month) as first_workspace_id,
        MIN_BY(a.device, a.record_month) as first_device
    FROM summary_monthly_activity a
    GROUP BY 1
),

-- 7. 丰富 Cohort 属性
cohort_users_enriched AS (
    SELECT
        fa.uid,
        fa.first_active_month as cohort_month,
        IFNULL(u.signup_country, 'unknown') as country_group,
        CASE
            WHEN wp.goods_plan_type IS NULL THEN 'Free'
            ELSE concat(
                CASE
                    WHEN wp.goods_plan_type=0 THEN 'Starter'
                    WHEN wp.goods_plan_type=1 THEN 'Pro'
                    WHEN wp.goods_plan_type=2 THEN 'Biz'
                    WHEN wp.goods_plan_type=3 THEN 'Enterprise'
                    ELSE 'else'
                END, '-',
                CASE
                    WHEN wp.period_unit = 2 AND wp.period_count = 12 THEN 'annually-1'
                    WHEN wp.period_unit = 1 AND wp.period_count = 1 THEN 'annually-1'
                    ELSE concat(CASE WHEN wp.period_unit=2 THEN 'monthly' ELSE 'annually' END, '-', wp.period_count)
                END
            )
        END as cohort_plan_detail,

        case when safe_divide(sum(rs.record_duration)/60,sum(rs.total_record_cnt))>30 then 'Meeting User'
        else 'Non-Meeting User' end as cohort_meeting_status,

        CASE
        WHEN wp.goods_plan_type IS NOT NULL THEN 'Paid Cohort'
            ELSE 'Free Cohort' END as cohort_type

    FROM user_first_summary_active fa
    LEFT JOIN user_dim_dedup u ON fa.uid = u.uid
    LEFT JOIN month_plans wp ON wp.workspace_id = fa.first_workspace_id AND wp.month = fa.first_active_month
    LEFT JOIN daily_record_stats_base rs ON fa.uid = rs.uid AND DATE_TRUNC(rs.stat_date, MONTH) = fa.first_active_month
    WHERE fa.first_active_month >= '2024-01-01'

    GROUP BY 1, 2, 3, 4, 6
),

-- 8. 构造活跃流水 (包含分端和Total)
all_activity_stream AS (
    SELECT uid, record_month, device FROM summary_monthly_activity
    UNION ALL
    SELECT uid, record_month, 'Total' as device FROM summary_monthly_activity
    GROUP BY 1, 2, 3
),

-- 9. 每月会议用户状态标签
monthly_meeting_labels AS (
    SELECT
        DATE_TRUNC(stat_date, MONTH) as month_start,
        uid,
        case when safe_divide(sum(record_duration)/60,sum(total_record_cnt))>30 then 'Meeting User'
        else 'Non-Meeting User' end as label
    FROM daily_record_stats_base
    GROUP BY 1, 2
),

-- =================================================
-- 10. [关键] 计算固定初始人数 (分母来源)
--    这里关联 Month 0 的活跃设备，确定每个 Cohort-Device 组合的初始分母
-- =================================================
fixed_initial_stats AS (
    SELECT
        c.cohort_type,
        c.cohort_month,
        c.country_group,
        c.cohort_plan_detail,
        c.cohort_meeting_status,
        -- 按照初始活跃设备分组 (含 Total)
        start_active.device,

        COUNT(DISTINCT c.uid) as fixed_initial_users,
        COUNT(DISTINCT CASE WHEN c.cohort_meeting_status = 'Meeting User' THEN c.uid END) as fixed_initial_meeting_users
    FROM cohort_users_enriched c
    -- 关联 Month 0 活跃，膨胀出 Total 行
    JOIN all_activity_stream start_active
        ON c.uid = start_active.uid
        AND c.cohort_month = start_active.record_month
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- =================================================
-- 11. 计算实际留存活跃 (分子来源)
-- =================================================
retention_activity AS (
    SELECT
        c.cohort_type,
        c.cohort_month,
        c.country_group,
        c.cohort_plan_detail,
        c.cohort_meeting_status,
        start_active.device, -- 保持与分母一致的设备维度

        DATE_DIFF(a.record_month, c.cohort_month, MONTH) as month_diff,

        COUNT(DISTINCT c.uid) as active_users,
        COUNT(DISTINCT CASE WHEN ml.label = 'Meeting User' THEN c.uid ELSE NULL END) as retained_meeting_active_users
    FROM cohort_users_enriched c
    -- 1. 确定初始设备
    JOIN all_activity_stream start_active
        ON c.uid = start_active.uid AND c.cohort_month = start_active.record_month
    -- 2. 关联未来活跃 (同设备类型)
    LEFT JOIN all_activity_stream a
        ON c.uid = a.uid
        AND a.record_month >= c.cohort_month
        AND start_active.device = a.device
    LEFT JOIN monthly_meeting_labels ml
        ON c.uid = ml.uid AND a.record_month = ml.month_start
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- =================================================
-- 12. 构建月度骨架 (Skeleton)
--    生成 0-24 月的所有组合
-- =================================================
month_series AS (
    -- 生成 0 到 24 的序列 (2年)
    SELECT * FROM UNNEST(GENERATE_ARRAY(0, 24)) as month_diff
),
skeleton AS (
    SELECT
        i.*, -- 包含所有维度 + fixed_initial_users
        ms.month_diff
    FROM fixed_initial_stats i
    CROSS JOIN month_series ms
    -- 过滤掉未来的时间
    WHERE DATE_ADD(i.cohort_month, INTERVAL ms.month_diff MONTH) <= CURRENT_DATE()
),

-- =================================================
-- 13. 骨架回填 (Fill Data)
-- =================================================
final_dataset AS (
    SELECT
        s.cohort_type,
        s.cohort_month,
        s.country_group,
        s.cohort_plan_detail,
        s.cohort_meeting_status,
        s.device,
        s.month_diff,

        -- 1. 初始人数 (来自骨架，绝不丢失)
        s.fixed_initial_users as cohort_initial_users,
        s.fixed_initial_meeting_users as cohort_initial_meeting_active_users,

        -- 2. 活跃人数 (关联不上则补0)
        COALESCE(r.active_users, 0) as active_users,
        COALESCE(r.retained_meeting_active_users, 0) as retained_meeting_active_users

    FROM skeleton s
    LEFT JOIN retention_activity r
        ON s.cohort_type = r.cohort_type
        AND s.cohort_month = r.cohort_month
        AND s.country_group = r.country_group
        AND s.cohort_plan_detail = r.cohort_plan_detail
        AND s.cohort_meeting_status = r.cohort_meeting_status
        AND s.device = r.device
        AND s.month_diff = r.month_diff
)

-- =================================================
-- 14. 最终输出 (含 LEAD 计算)
-- =================================================
SELECT
    *,
    -- 【指标 1】: 下一月的留存活跃用户数
    LEAD(active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_month, country_group, cohort_plan_detail, cohort_meeting_status, device
        ORDER BY month_diff ASC
    ) as next_month_active_users,

    -- 【指标 2】: 下一月的留存活跃会议用户数
    LEAD(retained_meeting_active_users, 1, 0) OVER (
        PARTITION BY cohort_type, cohort_month, country_group, cohort_plan_detail, cohort_meeting_status, device
        ORDER BY month_diff ASC
    ) as next_month_meeting_active_users

FROM final_dataset
ORDER BY cohort_type, cohort_month, device, month_diff