/*
 * @field signup_channel
 * @description User singup channel
 * @type integer
 * @value 0 mail
 * @value 1 google
 * @value 2 azure
 * @value 3 apple
 */

WITH user_data AS (
    SELECT
        u.uid,
        u.email,
        u.created_at AS signup_time,
        u.subscribe_email,
        CASE
            WHEN tl.created_at IS NULL THEN 0
            WHEN ABS(TIMESTAMP_DIFF(tl.created_at, u.created_at, SECOND)) < 60 THEN tl.platform
            ELSE 0
        END AS signup_channel
    FROM
        {{ ref('stg_aurora_user') }} u
        LEFT JOIN {{ ref('stg_aurora_thrid_login') }} tl ON u.uid = tl.uid
    WHERE
        -- Company E-mail
            u.email NOT LIKE '%@airgram.io'
        AND u.email NOT LIKE '%@notta.ai'
        AND u.email NOT LIKE '%@langogo.test'
        -- Temporary E-mail
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
        
),

exchange_rates AS (
    SELECT 
        currency,
        rate
    FROM
        {{ source('Summary', 'exchange_rates') }}
),

invoice_and_goods_data AS (
    SELECT
        i.uid,
        i.goods_id,
        i.workspace_id,
        i.subscription_id,
        i.is_trial,
        i.invoice_id,
        i.status,
        i.channel,
        i.amount,
        i.currency,
        i.seats_size,
        i.created_at,
        i.period_start,
        i.period_end,
        g.plan_type AS plan,
        g.data_learning_disabled
    FROM
        {{ ref('stg_aurora_invoice') }} i
        LEFT JOIN {{ ref('stg_aurora_goods') }} g ON i.goods_id = g.goods_id
    WHERE
        i.is_trial = 0 AND g.plan_type NOT IN (0, 99)
),

active_subscriptions AS (
    SELECT
        uid,
        MAX(plan) AS plan
    FROM
        invoice_and_goods_data
    WHERE
        period_end > CURRENT_TIMESTAMP
    GROUP BY uid
),

first_subscriptions AS (
    SELECT
        uid,
        plan,
        channel
    FROM (
            SELECT
                uid,
                plan,
                channel,
                ROW_NUMBER() OVER(PARTITION BY uid ORDER BY created_at) as rn
            FROM
                invoice_and_goods_data
        ) t
    WHERE t.rn = 1
),

invoice_data AS (
    SELECT 
        ig.uid,
        MIN(CASE WHEN ig.is_trial = 1 THEN ig.created_at ELSE NULL END) AS first_trial_time,
        MIN(CASE WHEN ig.is_trial = 0 and ig.plan not in (0, 99) THEN ig.created_at ELSE NULL END) AS first_subscription_time,
        COUNT(CASE WHEN ig.is_trial = 0 and ig.plan not in (0, 99) THEN 1 END) AS total_subscription_count,
        MAX(CASE WHEN ig.is_trial = 0 and ig.plan not in (0, 99) THEN ig.period_end ELSE NULL END) AS last_subscription_end,
        SUM(ig.amount / er.rate / 100) AS total_sales, -- 转换所有销售额到美元
    FROM
        invoice_and_goods_data ig
        LEFT JOIN {{ source('Summary', 'exchange_rates') }} er ON UPPER(ig.currency) = er.currency
    GROUP BY ig.uid
),

ga4_web_sign_up AS (
    SELECT * FROM {{ ref('stg_ga4_web_sign_up') }}
),

ga4_app_sign_up AS (
    SELECT * FROM {{ ref('stg_ga4_app_sign_up') }}
),

user_extra AS (
    SELECT * FROM {{ source('Aurora', 'user_extra') }}
),

stg_aurora_record AS (
    SELECT 
        r.creator_uid AS uid,
        MAX(r.create_date) AS last_transcription_date,
        SUM(CASE WHEN r.transcription_type = 1 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_file,
        COUNT(CASE WHEN r.transcription_type = 1 THEN 1 ELSE NULL END) AS total_transcriptions_count_file,
        SUM(CASE WHEN r.transcription_type = 2 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_realtime,
        COUNT(CASE WHEN r.transcription_type = 2 THEN 1 ELSE NULL END) AS total_transcriptions_count_realtime,
        SUM(CASE WHEN r.transcription_type = 3 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_multilingual,
        COUNT(CASE WHEN r.transcription_type = 3 THEN 1 ELSE NULL END) AS total_transcriptions_count_multilingual,
        SUM(CASE WHEN r.transcription_type = 4 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_meeting,
        COUNT(CASE WHEN r.transcription_type = 4 THEN 1 ELSE NULL END) AS total_transcriptions_count_meeting,
        SUM(CASE WHEN r.transcription_type = 5 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_accurate,
        COUNT(CASE WHEN r.transcription_type = 5 THEN 1 ELSE NULL END) AS total_transcriptions_count_accurate,
        SUM(CASE WHEN r.transcription_type = 6 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_screen,
        COUNT(CASE WHEN r.transcription_type = 6 THEN 1 ELSE NULL END) AS total_transcriptions_count_screen,
        SUM(CASE WHEN r.transcription_type = 7 THEN r.audio_duration ELSE 0 END) AS total_transcriptions_duration_download,
        COUNT(CASE WHEN r.transcription_type = 7 THEN 1 ELSE NULL END) AS total_transcriptions_count_download,
    FROM
        {{ ref('stg_aurora_record') }} r
    GROUP BY r.creator_uid
),


profession_info AS (
    SELECT * FROM {{ ref('stg_aurora_profession') }}
),

final_table AS (
    SELECT 
        u.uid,
        u.email,
        u.signup_time,
        u.signup_channel,
        COALESCE(u.subscribe_email, 1) as subscribe_email,
        COALESCE(w.country, a.country, ext.country, 'unknown') AS signup_country,
        COALESCE(w.city, a.city, ext.city, 'unknown') AS signup_city,
        COALESCE(w.device, a.device, 99) AS signup_platform,
        (CASE WHEN act.uid IS NOT NULL THEN act.plan ELSE 0 END) AS current_subscription_plan,
        (CASE WHEN i.uid IS NOT NULL THEN i.first_trial_time ELSE NULL END) AS first_trial_time,
        (CASE WHEN i.uid IS NOT NULL THEN i.first_subscription_time ELSE NULL END) AS first_subscription_time,
        (CASE WHEN fir.uid IS NOT NULL THEN fir.plan ELSE 0 END) AS first_subscription_plan,
        (CASE WHEN fir.uid IS NOT NULL THEN fir.channel ELSE NULL END) AS first_subscription_channel,
        (CASE WHEN i.uid IS NOT NULL THEN i.last_subscription_end ELSE NULL END) AS last_subscription_end,
        (CASE WHEN i.uid IS NOT NULL THEN COALESCE(i.total_sales, 0) ELSE 0 END) AS total_sales,
        (CASE WHEN i.uid IS NOT NULL THEN COALESCE(i.total_subscription_count, 0) ELSE 0 END) AS total_sales_count,
        (CASE WHEN ext.uid IS NOT NULL THEN ext.last_cancel_subscribe_time ELSE NULL END) AS last_cancel_subscribe_time,
        record.last_transcription_date,
        COALESCE(record.total_transcriptions_duration_file, 0) AS total_transcriptions_duration_file,
        COALESCE(record.total_transcriptions_count_file, 0) AS total_transcriptions_count_file,
        COALESCE(record.total_transcriptions_duration_realtime , 0) AS total_transcriptions_duration_realtime,
        COALESCE(record.total_transcriptions_count_realtime, 0) AS total_transcriptions_count_realtime,
        COALESCE(record.total_transcriptions_duration_multilingual , 0) AS total_transcriptions_duration_multilingual,
        COALESCE(record.total_transcriptions_count_multilingual, 0) AS total_transcriptions_count_multilingual,
        COALESCE(record.total_transcriptions_duration_meeting , 0) AS total_transcriptions_duration_meeting,
        COALESCE(record.total_transcriptions_count_meeting, 0) AS total_transcriptions_count_meeting,
        COALESCE(record.total_transcriptions_duration_accurate , 0) AS total_transcriptions_duration_accurate,
        COALESCE(record.total_transcriptions_count_accurate, 0) AS total_transcriptions_count_accurate,
        COALESCE(record.total_transcriptions_duration_screen , 0) AS total_transcriptions_duration_screen,
        COALESCE(record.total_transcriptions_count_screen, 0) AS total_transcriptions_count_screen,
        COALESCE(record.total_transcriptions_duration_download , 0) AS total_transcriptions_duration_download,
        COALESCE(record.total_transcriptions_count_download, 0) AS total_transcriptions_count_download,
        COALESCE(profession.profession , 'unknown') AS profession,
    FROM user_data u
    LEFT JOIN invoice_data i ON u.uid = i.uid
    LEFT JOIN active_subscriptions act ON u.uid = act.uid
    LEFT JOIN first_subscriptions fir ON u.uid = fir.uid
    LEFT JOIN ga4_web_sign_up w ON u.uid = w.uid
    LEFT JOIN ga4_app_sign_up a ON u.uid = CAST(a.uid AS INT64)
    LEFT JOIN user_extra ext ON u.uid = ext.uid
    LEFT JOIN stg_aurora_record record ON u.uid = record.uid
    LEFT JOIN profession_info profession ON u.uid = profession.uid
)

SELECT * FROM final_table
