{{ config(
    materialized='table',
    full_refresh=True
) }}

-- 1. 基础用户活动数据
WITH base_activity AS (
    SELECT
        a.uid,
        DATE_TRUNC(DATE(a.created_at), WEEK(MONDAY)) AS week_start,
        a.language,
        -- 标准化国家信息
        CASE 
            WHEN u.signup_country IN ('United States','Japan','unknown') THEN u.signup_country
            WHEN u.signup_country IN ('Sweden','Spain','Germany','Switzerland','Portugal','Italy',
                                     'United Kingdom','France','Belgium','Romania','Finland',
                                     'Russia','Serbia','Netherlands','Luxembourg','Malta',
                                     'Ukraine','Norway','Latvia','Austria','Greece','Slovenia',
                                     'Czechia','Bulgaria','Hungary','Moldova','Ireland',
                                     'Bosnia & Herzegovina','Albania','Croatia','Denmark',
                                     'Lithuania','Poland','Slovakia','Kosovo','Montenegro',
                                     'North Macedonia','Liechtenstein','Belarus','Iceland',
                                     'Estonia','Andorra','Gibraltar','Guernsey','Isle of Man',
                                     'Jersey','San Marino','Monaco','Svalbard & Jan Mayen') 
            THEN 'Europ'
            ELSE 'Others' 
        END AS country,
        -- 提取计划级别
        COALESCE(p.goods_plan_type, 0) AS plan
    FROM {{ ref('stg_aurora_ai_records') }} a
    INNER JOIN {{ ref('user_details') }} u 
        ON a.uid = u.uid 
    LEFT JOIN `notta-data-analytics.dbt_models_details.stg_aurora_interest` p
        ON CAST(a.workspace_id AS STRING) = CAST(p.workspace_id AS STRING)
        AND timestamp_seconds(p.start_valid_time) < CURRENT_TIMESTAMP() 
        AND timestamp_seconds(p.flush_time) > CURRENT_TIMESTAMP()
        AND p.goods_type IN (1, 3, 4, 7)
    WHERE a.uid IS NOT NULL
        AND u.pt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

-- 2. 所有可能的周数据 (预先计算好所有周的关系)
week_dates AS (
    SELECT DISTINCT
        week_start,
        DATE_ADD(week_start, INTERVAL 1 WEEK) AS next_week,
        DATE_ADD(week_start, INTERVAL 2 WEEK) AS third_week
    FROM base_activity
),

-- 3. 用户周活动计数
user_weekly_activity AS (
    SELECT
        uid,
        week_start,
        COUNT(*) AS activity_count,
        CASE WHEN COUNT(*) >= 3 THEN 1 ELSE 0 END AS is_active,
        -- 语言使用计数
        STRING_AGG(language ORDER BY language) AS languages
    FROM base_activity
    GROUP BY uid, week_start
),

-- 4. 用户首选语言
user_language_preference AS (
    SELECT
        uid,
        week_start,
        language,
        ROW_NUMBER() OVER(PARTITION BY uid, week_start ORDER BY count DESC, language) AS rank
    FROM (
        SELECT 
            uid, 
            week_start, 
            language, 
            COUNT(*) AS count
        FROM base_activity
        GROUP BY uid, week_start, language
    )
),

-- 5. 用户首选国家
user_country_preference AS (
    SELECT
        uid,
        week_start,
        country,
        ROW_NUMBER() OVER(PARTITION BY uid, week_start ORDER BY count DESC, country) AS rank
    FROM (
        SELECT 
            uid, 
            week_start, 
            country, 
            COUNT(*) AS count
        FROM base_activity
        GROUP BY uid, week_start, country
    )
),

-- 6. 用户维度汇总
user_profile AS (
    SELECT
        a.uid,
        a.week_start,
        a.activity_count,
        a.is_active,
        -- 首选语言 (rank=1的记录)
        l.language AS top_language,
        -- 首选国家 (rank=1的记录)
        c.country AS top_country,
        -- 最高级别计划
        MAX(b.plan) AS highest_plan
    FROM user_weekly_activity a
    LEFT JOIN base_activity b ON a.uid = b.uid AND a.week_start = b.week_start
    LEFT JOIN user_language_preference l ON a.uid = l.uid AND a.week_start = l.week_start AND l.rank = 1
    LEFT JOIN user_country_preference c ON a.uid = c.uid AND a.week_start = c.week_start AND c.rank = 1
    GROUP BY a.uid, a.week_start, a.activity_count, a.is_active, l.language, c.country
),

-- 7. 留存数据准备: 扁平化为每周的用户活动
flat_user_activity AS (
    SELECT 
        w.week_start AS current_week,
        w.next_week,
        w.third_week,
        u.uid,
        u.top_language AS language,
        u.top_country AS country,
        u.highest_plan AS plan,
        u.is_active,
        -- 标记用户在次周和三周的活动
        CASE WHEN un.uid IS NOT NULL THEN 1 ELSE 0 END AS active_in_next_week,
        CASE WHEN ut.uid IS NOT NULL THEN 1 ELSE 0 END AS active_in_third_week
    FROM week_dates w
    INNER JOIN user_profile u ON w.week_start = u.week_start
    -- 查找次周的活动
    LEFT JOIN user_profile un ON u.uid = un.uid AND w.next_week = un.week_start
    -- 查找三周的活动
    LEFT JOIN user_profile ut ON u.uid = ut.uid AND w.third_week = ut.week_start
),

-- 8. 留存指标计算
retention_results AS (
    SELECT
        current_week AS week_start,
        plan,
        country,
        language,
        
        -- 基本用户统计
        COUNT(DISTINCT uid) AS total_users,
        SUM(is_active) AS active_users,
        
        -- 留存统计
        SUM(active_in_next_week) AS second_week_retained_users,
        SUM(CASE WHEN is_active = 1 AND active_in_next_week = 1 THEN 1 ELSE 0 END) 
            AS active_second_week_retained_users,
        SUM(CASE WHEN active_in_next_week = 1 AND active_in_third_week = 1 THEN 1 ELSE 0 END) 
            AS third_week_retained_users,
        SUM(CASE WHEN is_active = 1 AND active_in_next_week = 1 AND active_in_third_week = 1 THEN 1 ELSE 0 END) 
            AS active_third_week_retained_users
    FROM flat_user_activity
    GROUP BY current_week, plan, country, language
)

-- 最终结果
SELECT
    week_start,
    FORMAT_DATE('%Y年第%V周', week_start) AS first_week,
    FORMAT_DATE('%Y年第%V周', DATE_ADD(week_start, INTERVAL 1 WEEK)) AS second_week,
    FORMAT_DATE('%Y年第%V周', DATE_ADD(week_start, INTERVAL 2 WEEK)) AS third_week,
    CASE
        WHEN plan = 0 THEN 'Free'
        WHEN plan = 1 THEN 'Pro'
        WHEN plan = 2 THEN 'Biz'
        WHEN plan = 3 THEN 'Enterprise'
        ELSE 'Unknown'
    END AS plan,
    country,
    language,
    total_users,
    active_users,
    second_week_retained_users,
    active_second_week_retained_users,
    third_week_retained_users,
    active_third_week_retained_users
FROM retention_results
ORDER BY week_start DESC, plan, country, language