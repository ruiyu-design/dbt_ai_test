-- 发起记录数
-- 使用 summary的记录数
-- 使用 summary的次数
-- 失败的 summary的次数
-- 失败的 summary的记录数

{{ config(
    materialized='table',
    full_refresh=True
) }}

WITH user_data AS (
    SELECT
        uid,
        profession,
        case when signup_country in ('United States','Japan','unknown') then signup_country
    when signup_country in (
                            'Sweden',
                            'Spain',
                            'Germany',
                            'Switzerland',
                            'Portugal',
                            'Italy',
                            'United Kingdom',
                            'France',
                            'Belgium',
                            'Romania',
                            'Finland',
                            'Russia',
                            'Serbia',
                            'Netherlands',
                            'Luxembourg',
                            'Malta',
                            'Ukraine',
                            'Norway',
                            'Latvia',
                            'Austria',
                            'Greece',
                            'Slovenia',
                            'Czechia',
                            'Bulgaria',
                            'Hungary',
                            'Moldova',
                            'Ireland',
                            'Bosnia & Herzegovina',
                            'Albania',
                            'Croatia',
                            'Denmark',
                            'Lithuania',
                            'Poland',
                            'Slovakia',
                            'Kosovo',
                            'Montenegro',
                            'North Macedonia',
                            'Liechtenstein',
                            'Belarus',
                            'Iceland',
                            'Estonia',
                            'Andorra',
                            'Gibraltar',
                            'Guernsey',
                            'Isle of Man',
                            'Jersey',
                            'San Marino',
                            'Monaco',
                            'Svalbard & Jan Mayen') then 'Europ'
    else 'Others' end as signup_country,
        current_plan_type
    FROM `notta-data-analytics.dbt_models_details.user_details` u
    WHERE u.pt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
record_data AS (
    SELECT
        DATE(r.create_date) AS create_date,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        r.transcription_type,
        CASE
            WHEN r.transcribe_language = 'ja-JP' THEN 'Japanese'
            WHEN r.transcribe_language = 'en-US' THEN 'English'
            ELSE 'Other'
        END AS language,
        count(record_id) AS record_count
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_record` AS r
    LEFT JOIN user_data AS u ON r.creator_uid = u.uid
    where DATE(r.create_date)>='2024-10-21'
    GROUP BY
        DATE(r.create_date),
        u.profession,
        u.signup_country,
        u.current_plan_type,
        r.transcription_type,
        CASE
            WHEN r.transcribe_language = 'ja-JP' THEN 'Japanese'
            WHEN r.transcribe_language = 'en-US' THEN 'English'
            ELSE 'Other'
        END
),
ai_data AS (
    SELECT
        DATE(r.created_at) AS created_at,
        u.profession,
        u.signup_country AS country,
        u.current_plan_type AS plan_type,
        r.transcription_type,
        CASE
            WHEN r.language = 'ja-JP' THEN 'Japanese'
            WHEN r.language = 'en-US' THEN 'English'
            ELSE 'Other'
        END AS language,
        count(record_id) AS ai_summary_count,
        count(distinct record_id) AS ai_summary_record_count,
        count(case when summary_status=2 then record_id else null end) AS ai_summary_failed_count,
        count(distinct case when summary_status=2 then record_id else null end) AS ai_summary_failed_record_count
    FROM `notta-data-analytics.dbt_models_details.stg_aurora_ai_records` AS r
    LEFT JOIN user_data AS u ON r.uid = u.uid
    where r.uid is not null
    GROUP BY
        DATE(r.created_at),
        u.profession,
        u.signup_country,
        u.current_plan_type,
        r.transcription_type,
        CASE
            WHEN r.language = 'ja-JP' THEN 'Japanese'
            WHEN r.language = 'en-US' THEN 'English'
            ELSE 'Other'
        END
)
SELECT
    COALESCE(r.create_date,a.created_at) as date,
    COALESCE(r.profession,a.profession) as profession,
    COALESCE(r.country,a.country) as country,
    COALESCE(r.plan_type,a.plan_type) as plan_type,
    COALESCE(r.transcription_type,a.transcription_type) as transcription_type,
    COALESCE(r.language,a.language) as language,
    COALESCE(r.record_count,0) AS record_count,
    COALESCE(ai_summary_count,0) AS ai_summary_count,
    COALESCE(ai_summary_record_count,0) AS ai_summary_record_count,
    COALESCE(ai_summary_failed_count,0) AS ai_summary_failed_count,
    COALESCE(ai_summary_failed_record_count,0) AS ai_summary_failed_record_count
FROM
    record_data AS r
    full JOIN ai_data AS a
        ON r.create_date = a.created_at
        AND r.profession = a.profession
        AND r.country = a.country
        AND r.plan_type = a.plan_type
        AND r.transcription_type = a.transcription_type
        AND r.language = a.language