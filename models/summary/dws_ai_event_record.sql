/*
 * @field event_name
 * @description Identifier for the event type. This value indicates different types of user interactions.
 * @type integer
 * @value 0 generated action
 * @value 1 regenerate action
 * @value 2 ai_notes_regenerate_replace_click
 * @value 3 ai_notes_regenerate_insert_below_click
 * @value 4 ai_notes_regenerate_discard_click
 * @value 5 ai_notes_regenerate_click
 * @value 6 ai_notes_section_delete_click
 * @value 7 ai_notes_section_helpful_click
 * @value 8 ai_notes_section_unhelpful_click
 * @value 9 library_template_logo_click
 * @value 10 ai_notes_area_use_template_click
 * @value 11 ai_notes_area_space_use_template_click
 * @value 12 ai_notes_expand_summary_library_click
 * @value 13 ai_notes_collapse_summary_library_click
 * @value 14 ai_notes_download
 * @value 15 ai_notes_copy_click
 * @value 16 ai_notes_send
 * @value 17 ai_notes_ask_question
 * @value 99 unknown event
 */
/*
 * @field plan_type
 * @description Membership level (does not distinguish between annual or monthly payments)
 * @type INTEGER
 * @value 0 Free
 * @value 1 Pro
 * @value 2 Biz
 * @value 3 Enterprise
 */
/*
 * @field trigger_type
 * @description Indicates how the event was triggered.
 * @type integer
 * @value 1 Manual trigger (default for GA records)
 * @value 2 Automatic trigger (from stg_aurora_ai_records)
 */
WITH user_data AS (
    SELECT uid,
        profession,
        signup_country
    FROM {{ ref('user_details') }}
    WHERE pt =(
            SELECT max(pt)
            FROM {{ ref('user_details') }}
        )
),
base_data AS (
    SELECT r.workspace_id,
        r.uid,
        r.record_id,
        r.prompt_id,
        r.task_id,
        r.language,
        r.transcription_type,
        r.trigger_type,
        r.plan_type,
        r.created_at,
        u.profession,
        u.signup_country AS country,
        p.title AS prompt_title,
        p.type AS prompt_type,
        t.title AS task_title,
        t.type AS task_type,
        CASE
            WHEN r.regenerate = 0 THEN 0
            WHEN r.regenerate = 1 THEN 1
            ELSE 99
        END AS event_name
    FROM {{ ref('stg_aurora_ai_records') }} AS r
        INNER JOIN user_data AS u ON r.uid = u.uid
        LEFT JOIN {{ ref('stg_aurora_ai_prompts') }} AS p ON p.prompt_id = r.prompt_id AND r.prompt_id IS NOT NULL
        LEFT JOIN {{ ref('stg_aurora_ai_tasks') }} AS t ON t.task_id = r.task_id AND r.task_id IS NOT NULL
),
ga_data_filled AS (
    SELECT DISTINCT
        r.workspace_id,
        r.uid,
        ga4.record_id,
        ga4.prompt_id,
        ga4.task_id,
        r.language,
        r.transcription_type,
        1 AS trigger_type,
        r.plan_type,
        ga4.created_at,
        u.profession,
        u.signup_country AS country,
        p.title AS prompt_title,
        p.type AS prompt_type,
        t.title AS task_title,
        t.type AS task_type,
        CASE
            WHEN ga4.event_name = 'ai_notes_regenerate_replace_click' THEN 2
            WHEN ga4.event_name = 'ai_notes_regenerate_insert_below_click' THEN 3
            WHEN ga4.event_name = 'ai_notes_regenerate_discard_click' THEN 4
            WHEN ga4.event_name = 'ai_notes_regenerate_click' THEN 5
            WHEN ga4.event_name = 'ai_notes_section_delete_click' THEN 6
            WHEN ga4.event_name = 'ai_notes_section_helpful_click' THEN 7
            WHEN ga4.event_name = 'ai_notes_section_unhelpful_click' THEN 8
            WHEN ga4.event_name = 'library_template_logo_click' THEN 9
            WHEN ga4.event_name = 'ai_notes_area_use_template_click' THEN 10
            WHEN ga4.event_name = 'ai_notes_area_space_use_template_click' THEN 11
            WHEN ga4.event_name = 'ai_notes_expand_summary_library_click' THEN 12
            WHEN ga4.event_name = 'ai_notes_collapse_summary_library_click' THEN 13
            WHEN ga4.event_name = 'ai_notes_download' THEN 14
            WHEN ga4.event_name = 'ai_notes_copy_click' THEN 15
            WHEN ga4.event_name = 'ai_notes_send' THEN 16
            WHEN ga4.event_name = 'ai_notes_ask_question' THEN 17
            ELSE 99
        END AS event_name
    FROM {{ ref('stg_ga4_summery_ai') }} AS ga4
        INNER JOIN {{ ref('stg_aurora_ai_records') }} AS r ON ga4.record_id = r.record_id
        INNER JOIN user_data AS u ON r.uid = u.uid
        LEFT JOIN {{ ref('stg_aurora_ai_prompts') }} AS p ON p.prompt_id = ga4.prompt_id
        LEFT JOIN {{ ref('stg_aurora_ai_tasks') }} AS t ON t.task_id = ga4.task_id
),
combined_data AS (
    SELECT workspace_id,
        uid,
        record_id,
        prompt_id,
        prompt_type,
        prompt_title,
        task_id,
        task_type,
        task_title,
        language,
        transcription_type,
        trigger_type,
        plan_type,
        created_at,
        event_name,
        profession,
        country
    FROM base_data
    UNION ALL
    SELECT workspace_id,
        uid,
        record_id,
        prompt_id,
        prompt_type,
        prompt_title,
        task_id,
        task_type,
        task_title,
        language,
        transcription_type,
        trigger_type,
        plan_type,
        created_at,
        event_name,
        profession,
        country
    FROM ga_data_filled
)
SELECT workspace_id,
    uid,
    record_id,
    prompt_id,
    prompt_type,
    prompt_title,
    task_id,
    task_type,
    task_title,
    language,
    transcription_type,
    trigger_type,
    plan_type,
    created_at,
    event_name,
    profession,
    country
FROM combined_data