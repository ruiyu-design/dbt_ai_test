{{ config(
    materialized='table',
    full_refresh=True
) }}

-- Step 1: 用户数据预处理
WITH user_data AS (
    SELECT
        uid,
        profession,
        CASE 
            WHEN signup_country IN ('United States', 'Japan', 'unknown') THEN signup_country
            WHEN signup_country IN (
                'Sweden', 'Spain', 'Germany', 'Switzerland', 'Portugal', 'Italy', 'United Kingdom', 
                'France', 'Belgium', 'Romania', 'Finland', 'Russia', 'Serbia', 'Netherlands', 
                'Luxembourg', 'Malta', 'Ukraine', 'Norway', 'Latvia', 'Austria', 'Greece', 
                'Slovenia', 'Czechia', 'Bulgaria', 'Hungary', 'Moldova', 'Ireland', 
                'Bosnia & Herzegovina', 'Albania', 'Croatia', 'Denmark', 'Lithuania', 
                'Poland', 'Slovakia', 'Kosovo', 'Montenegro', 'North Macedonia', 
                'Liechtenstein', 'Belarus', 'Iceland', 'Estonia', 'Andorra', 'Gibraltar', 
                'Guernsey', 'Isle of Man', 'Jersey', 'San Marino', 'Monaco', 'Svalbard & Jan Mayen'
            ) THEN 'Europe'
            ELSE 'Others'
        END AS signup_country,
        current_plan_type
    -- FROM `notta-data-analytics.dbt_models_details.user_details`
    FROM {{ ref('user_details') }} u
    WHERE u.pt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

-- Step 2: 日活记录用户数据
record_user_data_day AS (
    SELECT
        DATE(r.create_date) AS create_date,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        COUNT(DISTINCT u.uid) AS daily_record_user_count
    -- FROM `notta-data-analytics.dbt_models_details.stg_aurora_record` AS r
    FROM {{ ref('stg_aurora_record') }} AS r
    inner JOIN user_data AS u ON r.creator_uid = u.uid
    WHERE DATE(r.create_date) >= '2024-10-21'
    GROUP BY DATE(r.create_date), u.profession, u.signup_country, u.current_plan_type
),

-- Step 3: 日活 AI 用户数据
ai_user_data_day AS (
    SELECT
        DATE(r.created_at) AS created_at,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        COUNT(DISTINCT u.uid) AS daily_ai_summary_user_count,
        COUNT(DISTINCT CASE WHEN r.summary_status = 2 THEN u.uid ELSE NULL END) AS daily_ai_summary_failed_user_count
    -- FROM `notta-data-analytics.dbt_models_details.stg_aurora_ai_records` AS r
    FROM {{ ref('stg_aurora_ai_records') }} AS r
    inner JOIN user_data AS u ON r.uid = u.uid
    WHERE r.uid IS NOT NULL
    GROUP BY DATE(r.created_at), u.profession, u.signup_country, u.current_plan_type
),

-- Step 4: 周活记录用户数据
record_user_data_week AS (
    SELECT
        DATE_TRUNC(r.create_date, WEEK(MONDAY)) AS create_date,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        COUNT(DISTINCT u.uid) AS weekly_record_user_count
    -- FROM `notta-data-analytics.dbt_models_details.stg_aurora_record` AS r
    FROM {{ ref('stg_aurora_record') }} AS r
    inner JOIN user_data AS u ON r.creator_uid = u.uid
    WHERE DATE(r.create_date) >= '2024-10-21'
    GROUP BY DATE_TRUNC(r.create_date, WEEK(MONDAY)), u.profession, u.signup_country, u.current_plan_type
),

-- Step 5: 周活 AI 用户数据
ai_user_data_week AS (
    SELECT
        DATE_TRUNC(r.created_at, WEEK(MONDAY)) AS created_at,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        COUNT(DISTINCT u.uid) AS weekly_ai_summary_user_count,
        COUNT(DISTINCT CASE WHEN r.summary_status = 2 THEN u.uid ELSE NULL END) AS weekly_ai_summary_failed_user_count
    -- FROM `notta-data-analytics.dbt_models_details.stg_aurora_ai_records` AS r
    FROM {{ ref('stg_aurora_ai_records') }} AS r
    inner JOIN user_data AS u ON r.uid = u.uid
    WHERE r.uid IS NOT NULL
    GROUP BY DATE_TRUNC(r.created_at, WEEK(MONDAY)), u.profession, u.signup_country, u.current_plan_type
),

-- Step 6: 月活记录用户数据
record_user_data_month AS (
    SELECT
        DATE_TRUNC(r.create_date, MONTH) AS create_date,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        COUNT(DISTINCT u.uid) AS monthly_record_user_count
    -- FROM `notta-data-analytics.dbt_models_details.stg_aurora_record` AS r
    FROM {{ ref('stg_aurora_record') }} AS r
    inner JOIN user_data AS u ON r.creator_uid = u.uid
    WHERE DATE(r.create_date) >= '2024-10-21'
    GROUP BY DATE_TRUNC(r.create_date, MONTH), u.profession, u.signup_country, u.current_plan_type
),

-- Step 7: 月活 AI 用户数据
ai_user_data_month AS (
    SELECT
        DATE_TRUNC(r.created_at, MONTH) AS created_at,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        COUNT(DISTINCT u.uid) AS monthly_ai_summary_user_count,
        COUNT(DISTINCT CASE WHEN r.summary_status = 2 THEN u.uid ELSE NULL END) AS monthly_ai_summary_failed_user_count
    -- FROM `notta-data-analytics.dbt_models_details.stg_aurora_ai_records` AS r
    FROM {{ ref('stg_aurora_ai_records') }} AS r
    inner JOIN user_data AS u ON r.uid = u.uid
    WHERE r.uid IS NOT NULL
    GROUP BY DATE_TRUNC(r.created_at, MONTH), u.profession, u.signup_country, u.current_plan_type
)

-- 主 SELECT 查询：结合日活、周活和月活数据
SELECT
    DATE(COALESCE(r.create_date, a.created_at)) AS date,
    'day' AS granularity,
    COALESCE(r.profession, a.profession) AS profession,
    COALESCE(r.country, a.country) AS country,
    COALESCE(r.plan_type, a.plan_type) AS plan_type,
    COALESCE(daily_record_user_count, 0) AS daily_record_user_count,
    COALESCE(daily_ai_summary_user_count, 0) AS daily_ai_summary_user_count,
    COALESCE(daily_ai_summary_failed_user_count, 0) AS daily_ai_summary_failed_user_count
FROM record_user_data_day AS r
FULL JOIN ai_user_data_day AS a
    ON r.create_date = a.created_at
    AND r.profession = a.profession
    AND r.country = a.country
    AND r.plan_type = a.plan_type

UNION ALL

SELECT
    DATE(COALESCE(r.create_date, a.created_at)) AS date,
    'week' AS granularity,
    COALESCE(r.profession, a.profession) AS profession,
    COALESCE(r.country, a.country) AS country,
    COALESCE(r.plan_type, a.plan_type) AS plan_type,
    COALESCE(weekly_record_user_count, 0) AS weekly_record_user_count,
    COALESCE(weekly_ai_summary_user_count, 0) AS weekly_ai_summary_user_count,
    COALESCE(weekly_ai_summary_failed_user_count, 0) AS weekly_ai_summary_failed_user_count
FROM record_user_data_week AS r
FULL JOIN ai_user_data_week AS a
    ON r.create_date = a.created_at
    AND r.profession = a.profession
    AND r.country = a.country
    AND r.plan_type = a.plan_type

UNION ALL

SELECT
    DATE(COALESCE(r.create_date, a.created_at)) AS date,
    'month' AS granularity,
    COALESCE(r.profession, a.profession) AS profession,
    COALESCE(r.country, a.country) AS country,
    COALESCE(r.plan_type, a.plan_type) AS plan_type,
    COALESCE(monthly_record_user_count, 0) AS monthly_record_user_count,
    COALESCE(monthly_ai_summary_user_count, 0) AS monthly_ai_summary_user_count,
    COALESCE(monthly_ai_summary_failed_user_count, 0) AS monthly_ai_summary_failed_user_count
FROM record_user_data_month AS r
FULL JOIN ai_user_data_month AS a
    ON r.create_date = a.created_at
    AND r.profession = a.profession
    AND r.country = a.country
    AND r.plan_type = a.plan_type