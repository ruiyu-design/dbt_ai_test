WITH user_data AS (
    SELECT 
        uid,
        profession,
        signup_country,
        current_plan_type
    FROM {{ ref('user_details') }}
    WHERE pt = (
            SELECT max(pt)
            FROM {{ ref('user_details') }}
        )
),
-- 获取每次失败记录，并将失败时间设为 create_date
ai_data AS (
    SELECT 
        record_id,
        uid,
        summary_status,
        created_at as failed_date
    FROM {{ ref('stg_aurora_ai_records') }}
),
record_data AS (
select
        a.record_id,
        a.failed_date,
        c.transcribe_language AS language,
        a.uid,
        c.transcription_type,
        b.profession,
        b.signup_country AS country,
        b.current_plan_type AS plan_type,
        c.create_date,
        a.summary_status
    from ai_data a
    inner join user_data b on a.uid=b.uid
    left join {{ ref('stg_aurora_record') }} c on a.record_id=c.record_id
)
SELECT
    record_data.record_id,
    record_data.create_date,
    record_data.failed_date,
    record_data.language,
    record_data.uid,
    record_data.transcription_type,
    record_data.profession,
    record_data.country,
    record_data.plan_type,
    record_data.summary_status
FROM
    record_data 
