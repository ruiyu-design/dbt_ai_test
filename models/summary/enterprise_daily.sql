{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
        'field': 'created_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    require_partition_filter=True
) }}
-- The `require_partition_filter` field specifies that the time partitioning key must be used when configuring the query.
-- If it is true, it is required to bring a time partition when querying where, otherwise it is not needed.

{% set yesterday = get_yesterday() %}
{% set yesterday_date = get_date_of_yesterday() %}
{% set WEB = (2, 7) %}
{% set APP = (3, 4, 5) %}
{% set EXTENSION = 6 %}

WITH ti AS (
    SELECT
        TIMESTAMP '{{ yesterday_date }} 00:00:00' AS yesterday_start,
        TIMESTAMP '{{ yesterday_date }} 23:59:59' AS yesterday_end
),

record AS (
    SELECT
        record_id,
        workspace_id,
        create_date,
        audio_duration,
        transcribe_language,
        creator_uid,
        transcription_type,
        media_source,
        enable_speaker
    FROM
        {{ ref('stg_aurora_record') }}
    WHERE
        create_date BETWEEN (SELECT yesterday_start FROM ti) AND (SELECT yesterday_end FROM ti)
),

ga4_buried_events_polymers AS (
    -- Web
    SELECT
        event_name,
        TIMESTAMP_MILLIS(CAST((event_timestamp / 1000) AS INT64)) AS create_date,
        CASE
            WHEN user_property_workspace.key = 'workspace_id' THEN user_property_workspace.value.int_value
        END AS workspace_id,
        CASE
            WHEN user_property_uid.key = 'uid' THEN user_property_uid.value.int_value
        END AS uid,
        CASE
            WHEN event_param.key = 'method' THEN event_param.value.string_value
        END AS method
    FROM
        `notta-data-analytics.analytics_345009513.events_{{ yesterday }}`,
        UNNEST(user_properties) AS user_property_workspace,
        UNNEST(user_properties) AS user_property_uid,
        UNNEST(event_params) AS event_param

    UNION ALL

    -- App
    SELECT
        event_name,
        TIMESTAMP_MILLIS(CAST((event_timestamp / 1000) AS INT64)) AS create_date,
        CASE
            WHEN user_property_workspace.key = 'workspace_id' THEN user_property_workspace.value.int_value
        END AS workspace_id,
        CASE
            WHEN user_property_uid.key = 'uid' THEN user_property_uid.value.int_value
        END AS uid,
        CASE
            WHEN event_param.key = 'method' THEN event_param.value.string_value
        END AS method
    FROM
        `notta-data-analytics.analytics_234597866.events_{{ yesterday }}`,
        UNNEST(user_properties) AS user_property_workspace,
        UNNEST(user_properties) AS user_property_uid,
        UNNEST(event_params) AS event_param
),

ga4_buried_events AS (
    SELECT
        event_name,
        create_date,
        workspace_id,
        uid,
        method
    FROM
        ga4_buried_events_polymers
    WHERE
        create_date BETWEEN (SELECT yesterday_start FROM ti) AND (SELECT yesterday_end FROM ti)
),

exchange_rates AS (
  SELECT 
        currency,
        rate
  FROM
        {{ source('Summary', 'exchange_rates') }}
),

deals AS (
    SELECT
        DISTINCT results.workspace_id,
        results.workspace_name,
        results.current_price
    FROM
        (
            SELECT
                hd.created_at,
                hd.workspace_id,
                hd.workspace_name,
                ROUND(hd.amount / er.rate , 2) AS current_price,
                ROW_NUMBER() OVER (PARTITION BY hd.workspace_id ORDER BY hd.created_at DESC) AS row_number
            FROM
                {{ ref('stg_hubspot_deals') }} hd
                LEFT JOIN exchange_rates er ON UPPER(hd.currency_code) = er.currency
            ORDER BY hd.created_at DESC
        ) AS results
    WHERE
        results.row_number = 1
),

invoice AS (
    SELECT
        invoice_id,
        ROUND(inv.amount / er.rate / 100, 2) AS amount
    FROM
        {{ ref('stg_aurora_invoice') }} inv
        LEFT JOIN exchange_rates er ON UPPER(inv.currency) = er.currency
),

extended_plan AS (
    SELECT
        i.workspace_id,
        i.seats_size,
        i.package_duration,
        g.plan_type,
        g.data_learning_disabled,
        inv.amount,
        i.start_valid_time,
        i.flush_time,
        LEAD(i.start_valid_time) OVER (PARTITION BY i.workspace_id ORDER BY i.start_valid_time) AS next_start_valid_time
    FROM
        {{ ref('stg_aurora_interest') }} i
        LEFT JOIN {{ ref('stg_aurora_goods') }} g ON i.goods_id = g.goods_id
        LEFT JOIN invoice inv ON i.invoice_id = inv.invoice_id
    WHERE
        g.plan_type in (3, 4, 5, 6)
),

filtered_plan AS (
    SELECT
        workspace_id,
        seats_size,
        package_duration,
        plan_type,
        data_learning_disabled,
        amount,
        start_valid_time,
        flush_time,
        next_start_valid_time,
        CASE
            WHEN flush_time <= next_start_valid_time THEN 'Continuous'
            ELSE 'Gap'
        END AS continuity_status
    FROM
        extended_plan
),

plan AS (
    SELECT
        DISTINCT workspace_id,
        seats_size,
        package_duration,
        plan_type,
        data_learning_disabled,
        amount
    FROM
        filtered_plan
    WHERE
        -- Reclaimed interests need to be filtered out
        start_valid_time < flush_time
        AND
        (
            -- Add Subscription
            (
                (SELECT yesterday_start FROM ti) <= start_valid_time
                AND (SELECT yesterday_end FROM ti) > start_valid_time
            )
            OR
            -- Subscription
            (
                (SELECT yesterday_start FROM ti) >= start_valid_time
                AND (SELECT yesterday_end FROM ti) <= flush_time
            )
            OR
            -- Renewals
            (
                continuity_status = 'Continuous'
                AND (SELECT yesterday_start FROM ti) >= start_valid_time
                AND (SELECT yesterday_start FROM ti) <= next_start_valid_time
                AND (SELECT yesterday_end FROM ti) >= flush_time
            )
            OR
            -- Unsubscribe
            (
                continuity_status = 'Gap'
                AND (SELECT yesterday_start FROM ti) >= start_valid_time
                AND (SELECT yesterday_start FROM ti) <= flush_time
                AND (SELECT yesterday_end FROM ti) >= flush_time
            )
        )
    GROUP BY
        workspace_id,
        seats_size,
        package_duration,
        plan_type,
        data_learning_disabled,
        amount
),

workspace AS (
    SELECT
        DISTINCT p.workspace_id,
        w.owner_uid,
        ws.sso_status,
        ws.ip_whitelist_enabled,
        ws.ip_whitelist_num,
        CASE
            WHEN w.workspace_name = '' THEN 'unknown'
            ELSE w.workspace_name
        END AS workspace_name
    FROM
        plan p
        LEFT JOIN {{ ref('stg_aurora_workspace') }} w ON w.workspace_id = p.workspace_id
        LEFT JOIN {{ ref('stg_aurora_workspace_setting') }} ws ON w.workspace_id = ws.workspace_id
    WHERE
        w.workspace_id IS NOT NULL
),

user_data AS (
    SELECT
        uid
    FROM 
        {{ ref('stg_aurora_user') }}
),

ga4_web_and_app_data AS (
    SELECT
        CAST(uid AS INT64) AS uid,
        country,
        city,
        event_date_dt
    FROM
        {{ ref('stg_ga4_web_sign_up') }}
    
    UNION ALL

    SELECT
        CAST(uid AS INT64) AS uid,
        country,
        city,
        event_date_dt
    FROM
        {{ ref('stg_ga4_app_sign_up') }}
),

ga4_web_and_app_filtration AS (
    SELECT
        uid,
        country,
        city,
        ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_date_dt ASC) AS row_number
    FROM
        ga4_web_and_app_data
),

user_extra AS (
    SELECT
        uid,
        country,
        city
    FROM
        {{ source('Aurora', 'user_extra') }}
),

user_info AS (
    SELECT 
        DISTINCT u.uid,
        COALESCE(wa.country, ext.country, 'unknown') AS owner_country,
        COALESCE(wa.city, ext.city, 'unknown') AS owner_city, 
    FROM
        user_data u
        LEFT JOIN ga4_web_and_app_filtration wa ON u.uid = wa.uid
        LEFT JOIN user_extra ext ON u.uid = ext.uid
    WHERE
        wa.row_number = 1
),

member_count AS (
    SELECT
        m.workspace_id,
        COUNT(m.workspace_id) AS member_num
    FROM  
        {{ ref('stg_aurora_member') }} m
    GROUP BY 
        m.workspace_id
),

user_login_count AS (
    SELECT
        workspace_id,
        COUNT(DISTINCT uid) AS active_members_num
    FROM
        ga4_buried_events
    WHERE
        event_name = 'initialize_workspace'
    GROUP BY
        workspace_id
),

record_viewer_count AS (
    SELECT
        workspace_id,
        COUNT(DISTINCT uid) AS viewer_members_num
    FROM
        ga4_buried_events
    WHERE
        event_name IN ('open_detail', 'video_load_finish')
    GROUP BY
        workspace_id
),

record_editor_count AS (
    SELECT
        workspace_id,
        COUNT(DISTINCT uid) AS editor_members_num
    FROM
        ga4_buried_events
    WHERE
        event_name IN ('detail_summary_save_success', 'speakers_click', 'record_add_notes_success')
    GROUP BY
        workspace_id
),

record_create_count AS (
    SELECT
        r.workspace_id,
        COUNT(DISTINCT r.creator_uid) AS create_members_num
    FROM
        record r
    GROUP BY
        r.workspace_id
),

ai_tasks_count AS (
    SELECT
        at2.workspace_id,
        COUNT(at2.workspace_id) AS custom_prompts_num
    FROM
        {{ ref('stg_aurora_ai_tasks') }} as at2
    WHERE 
        at2.type = "custom"
        AND
        created_at BETWEEN (SELECT yesterday_start FROM ti) AND (SELECT yesterday_end FROM ti)
    GROUP BY
        at2.workspace_id
),

ai_prompts_count AS (
    SELECT
        ap.workspace_id,
        COUNT(ap.workspace_id) AS custom_templates_num
    FROM
        {{ ref('stg_aurora_ai_prompts') }} ap
    WHERE 
        ap.type = 'custom'
        AND
        created_at BETWEEN (SELECT yesterday_start FROM ti) AND (SELECT yesterday_end FROM ti)
    GROUP BY
        ap.workspace_id
),

user_scheduler_event_count AS (
    SELECT
        use2.workspace_id,
        COUNT(use2.workspace_id) AS scheduler_events_num
    FROM
        {{ ref('stg_aurora_user_scheduler_event') }} use2
    WHERE
        created_at BETWEEN (SELECT yesterday_start FROM ti) AND (SELECT yesterday_end FROM ti)
    GROUP BY
        use2.workspace_id
),

web_transcription_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS web_recording_transcriptions_num,
        SUM(r.audio_duration) AS web_recording_transcription_duration
    FROM
        record r
    WHERE
        r.media_source IN {{ WEB }}
    GROUP BY
        r.workspace_id
),

app_transcription_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS app_recording_transcriptions_num,
        SUM(r.audio_duration) AS app_recording_transcription_duration
    FROM
        record r  
    WHERE
        r.media_source IN {{ APP }}
    GROUP BY
        r.workspace_id
),

extension_transcription_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS extension_recording_transcriptions_num,
        SUM(r.audio_duration) AS extension_recording_transcription_duration
    FROM
        record r
    WHERE
        r.media_source = {{ EXTENSION }}
    GROUP BY
        r.workspace_id
),

web_file_transcription_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS web_file_transcriptions_num,
        SUM(r.audio_duration) AS web_file_transcription_duration
    FROM
        record r
    WHERE
        r.transcription_type IN {{ WEB }} AND r.transcription_type = 1
    GROUP BY
        r.workspace_id
),

app_file_transcription_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS app_file_transcriptions_num,
        SUM(r.audio_duration) AS app_file_transcription_duration
    FROM
        record r
    WHERE
        r.media_source IN {{ APP }} AND r.transcription_type = 1
    GROUP BY
        r.workspace_id
),

web_file_transcriptions_speaker_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS web_speaker_file_transcriptions_num,
        SUM(r.audio_duration) AS web_speaker_file_transcription_duration
    FROM
        record r
    WHERE
        r.media_source IN {{ WEB }} AND r.transcription_type = 1 AND r.enable_speaker = 1
    GROUP BY
        r.workspace_id
),

app_file_transcriptions_speaker_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS app_speaker_file_transcriptions_num,
        SUM(r.audio_duration) AS app_speaker_file_transcription_duration
    FROM
        record r
    WHERE
        r.media_source IN {{ APP }} AND r.transcription_type = 1 AND r.enable_speaker = 1
    GROUP BY
        r.workspace_id
),

url_transcriptions_data AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS url_transcriptions_num,
        SUM(r.audio_duration) AS url_transcription_duration
    FROM
        record r
    WHERE
        r.transcription_type IN {{ WEB }} AND r.transcription_type = 7
    GROUP BY
        r.workspace_id
),

meeting_data AS (
    SELECT
        r.workspace_id,
        COUNT(CASE WHEN ma.meeting_type = 'zoom' THEN r.workspace_id END) AS zoom_meeting_transcriptions_num,
        SUM(CASE WHEN ma.meeting_type = 'zoom' THEN r.audio_duration ELSE 0 END) AS zoom_meeting_transcription_duration,
        COUNT(CASE WHEN ma.meeting_type = 'google_meet' THEN r.workspace_id END) AS google_meeting_transcriptions_num,
        SUM(CASE WHEN ma.meeting_type = 'google_meet' THEN r.audio_duration ELSE 0 END) AS google_meeting_transcription_duration,
        COUNT(CASE WHEN ma.meeting_type = 'ms_teams' THEN r.workspace_id END) AS teams_meeting_transcriptions_num,
        SUM(CASE WHEN ma.meeting_type = 'ms_teams' THEN r.audio_duration ELSE 0 END) AS teams_meeting_transcription_duration,
        COUNT(CASE WHEN ma.meeting_type = 'webex' THEN r.workspace_id END) AS webx_meeting_transcriptions_num,
        SUM(CASE WHEN ma.meeting_type = 'webex' THEN r.audio_duration ELSE 0 END) AS webx_meeting_transcription_duration
    FROM
        record r
        LEFT JOIN {{ ref('stg_aurora_meeting_analysis') }} ma ON r.record_id = ma.record_id
    GROUP BY
        r.workspace_id
),

vocabularies_count AS (
    SELECT
        r.workspace_id,
        COUNT(r.workspace_id) AS custom_vocabularies_num
    FROM
        record r
        LEFT JOIN {{ ref('stg_aurora_vocabulary') }} v ON r.workspace_id = v.workspace_id
    WHERE
        created_at BETWEEN (SELECT yesterday_start FROM ti) AND (SELECT yesterday_end FROM ti)
    GROUP BY
        r.workspace_id
),

ga4_translate_btn_click_num AS (
    SELECT
        ga.workspace_id,
        COUNT(ga.workspace_id) AS translate_click_num
    FROM
        ga4_buried_events ga
    WHERE
        ga.event_name = 'translation_btn_click'
    GROUP BY
        ga.workspace_id
),

ga4_download_btn_click_num AS (
    SELECT
        ga.workspace_id,
        COUNT(ga.workspace_id) AS download_click_num
    FROM
        ga4_buried_events ga
    WHERE
        ga.event_name = 'record_export_click'
    GROUP BY
        ga.workspace_id
),

ga4_send_to_notion_btn_click_num AS (
    SELECT
        ga.workspace_id,
        COUNT(ga.workspace_id) AS send_to_notion_click_num
    FROM
        ga4_buried_events ga
    WHERE
        ga.event_name = 'record_send_result' AND ga.method = 'notion'
    GROUP BY
        ga.workspace_id
),

ga4_send_to_salesforce_btn_click_num AS (
    SELECT
        ga.workspace_id,
        COUNT(ga.workspace_id) AS send_to_salesforce_click_num
    FROM
        ga4_buried_events ga
    WHERE
        ga.event_name = 'record_send_result' AND ga.method = 'salesforce'
    GROUP BY
        ga.workspace_id
),

ga4_share_btn_click_num AS (
    SELECT
        ga.workspace_id,
        COUNT(ga.workspace_id) AS share_click_num
    FROM
        ga4_buried_events ga
    WHERE
        ga.event_name = 'record_share_click'
    GROUP BY
        ga.workspace_id
),

ga4_add_notes_btn_click_num AS (
    SELECT
        ga.workspace_id,
        COUNT(ga.workspace_id) AS add_notes_click_num
    FROM
        ga4_buried_events ga
    WHERE
        ga.event_name = 'record_add_notes_success'
    GROUP BY
        ga.workspace_id
)

SELECT
    p.workspace_id,
    p.seats_size,
    p.plan_type, 
    p.data_learning_disabled,
    p.package_duration,
    w.owner_uid,
    DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ var('days_to_look_back') }} DAY) AS created_at,
    IFNULL(w.sso_status, 0) AS sso_status,
    COALESCE(d.workspace_name, w.workspace_name, 'unknown') AS workspace_name,
    COALESCE(d.current_price, p.amount, 0) AS current_price,
    IFNULL(w.ip_whitelist_enabled, 0) AS ip_whitelist_enabled,
    IFNULL(w.ip_whitelist_num, 0) AS ip_whitelist_num,
    IFNULL(u.owner_country, 'unknown') AS owner_country,
    IFNULL(u.owner_city, 'unknown') AS owner_city,
    IFNULL(m.member_num, 1) AS member_num,
    IFNULL(ulc.active_members_num, 0) AS active_members_num,
    IFNULL(rvc.viewer_members_num, 0) AS viewer_members_num,
    IFNULL(rec.editor_members_num, 0) AS editor_members_num,
    IFNULL(rcc.create_members_num, 0) AS create_members_num,
    IFNULL(at2.custom_prompts_num, 0) AS custom_prompts_num,
    IFNULL(ap.custom_templates_num, 0) AS custom_templates_num,
    IFNULL(use2.scheduler_events_num, 0) AS scheduler_events_num,
    IFNULL(wtd.web_recording_transcriptions_num, 0) AS web_recording_transcriptions_num,
    IFNULL(wtd.web_recording_transcription_duration, 0) AS web_recording_transcription_duration,
    IFNULL(wftd.web_file_transcriptions_num, 0) AS web_file_transcriptions_num,
    IFNULL(wftd.web_file_transcription_duration, 0) AS web_file_transcription_duration,
    IFNULL(wftsd.web_speaker_file_transcriptions_num, 0) AS web_speaker_file_transcriptions_num,
    IFNULL(wftsd.web_speaker_file_transcription_duration, 0) AS web_speaker_file_transcription_duration,
    IFNULL(atd.app_recording_transcriptions_num, 0) AS app_recording_transcriptions_num,
    IFNULL(atd.app_recording_transcription_duration, 0) AS app_recording_transcription_duration,
    IFNULL(aftd.app_file_transcriptions_num, 0) AS app_file_transcriptions_num,
    IFNULL(aftd.app_file_transcription_duration, 0) AS app_file_transcription_duration,
    IFNULL(aftsd.app_speaker_file_transcriptions_num, 0) AS app_speaker_file_transcriptions_num,
    IFNULL(aftsd.app_speaker_file_transcription_duration, 0) AS app_speaker_file_transcription_duration,
    IFNULL(etd.extension_recording_transcriptions_num, 0) AS extension_recording_transcriptions_num,
    IFNULL(etd.extension_recording_transcription_duration, 0) AS extension_recording_transcription_duration,
    IFNULL(utd.url_transcriptions_num, 0) AS url_transcriptions_num,
    IFNULL(utd.url_transcription_duration, 0) AS url_transcription_duration,
    IFNULL(md.zoom_meeting_transcriptions_num, 0) AS zoom_meeting_transcriptions_num,
    IFNULL(md.zoom_meeting_transcription_duration, 0) AS zoom_meeting_transcription_duration,
    IFNULL(md.google_meeting_transcriptions_num, 0) AS google_meeting_transcriptions_num,
    IFNULL(md.google_meeting_transcription_duration, 0) AS google_meeting_transcription_duration,
    IFNULL(md.teams_meeting_transcriptions_num, 0) AS teams_meeting_transcriptions_num,
    IFNULL(md.teams_meeting_transcription_duration, 0) AS teams_meeting_transcription_duration,
    IFNULL(md.webx_meeting_transcriptions_num, 0) AS webx_meeting_transcriptions_num,
    IFNULL(md.webx_meeting_transcription_duration, 0) AS webx_meeting_transcription_duration,
    IFNULL(vn.custom_vocabularies_num, 0) AS custom_vocabularies_num,
    IFNULL(gtbc.translate_click_num, 0) AS translate_click_num,
    IFNULL(gdbc.download_click_num, 0) AS download_click_num,
    IFNULL(gstnbc.send_to_notion_click_num, 0) AS send_to_notion_click_num,
    IFNULL(gstsbc.send_to_salesforce_click_num, 0) AS send_to_salesforce_click_num,
    IFNULL(gsbt.share_click_num, 0) AS share_click_num,
    IFNULL(ganbc.add_notes_click_num, 0) AS add_notes_click_num
FROM
    plan p
    LEFT JOIN deals d ON p.workspace_id = d.workspace_id
    LEFT JOIN workspace w ON p.workspace_id = w.workspace_id
    LEFT JOIN user_info u ON w.owner_uid = u.uid
    LEFT JOIN member_count m ON p.workspace_id = m.workspace_id
    LEFT JOIN user_login_count ulc ON p.workspace_id = ulc.workspace_id
    LEFT JOIN record_viewer_count rvc ON p.workspace_id = rvc.workspace_id
    LEFT JOIN record_editor_count rec ON p.workspace_id = rec.workspace_id
    LEFT JOIN record_create_count rcc ON p.workspace_id = rcc.workspace_id
    LEFT JOIN ai_tasks_count at2 ON p.workspace_id = at2.workspace_id
    LEFT JOIN ai_prompts_count ap ON p.workspace_id = ap.workspace_id
    LEFT JOIN user_scheduler_event_count use2 ON p.workspace_id = use2.workspace_id
    LEFT JOIN web_transcription_data wtd ON p.workspace_id = wtd.workspace_id
    LEFT JOIN web_file_transcription_data wftd ON p.workspace_id = wftd.workspace_id
    LEFT JOIN web_file_transcriptions_speaker_data wftsd ON p.workspace_id = wftsd.workspace_id
    LEFT JOIN app_transcription_data atd ON p.workspace_id = atd.workspace_id
    LEFT JOIN app_file_transcription_data aftd ON p.workspace_id = aftd.workspace_id
    LEFT JOIN app_file_transcriptions_speaker_data aftsd ON p.workspace_id = aftsd.workspace_id
    LEFT JOIN extension_transcription_data etd ON p.workspace_id = etd.workspace_id
    LEFT JOIN url_transcriptions_data utd ON p.workspace_id = utd.workspace_id
    LEFT JOIN meeting_data md ON p.workspace_id = md.workspace_id
    LEFT JOIN vocabularies_count vn ON p.workspace_id = vn.workspace_id
    LEFT JOIN ga4_translate_btn_click_num gtbc ON p.workspace_id = gtbc.workspace_id
    LEFT JOIN ga4_download_btn_click_num gdbc ON p.workspace_id = gdbc.workspace_id
    LEFT JOIN ga4_send_to_notion_btn_click_num gstnbc ON p.workspace_id = gstnbc.workspace_id
    LEFT JOIN ga4_send_to_salesforce_btn_click_num gstsbc ON p.workspace_id = gstsbc.workspace_id
    LEFT JOIN ga4_share_btn_click_num gsbt ON p.workspace_id = gsbt.workspace_id
    LEFT JOIN ga4_add_notes_btn_click_num ganbc ON p.workspace_id = ganbc.workspace_id