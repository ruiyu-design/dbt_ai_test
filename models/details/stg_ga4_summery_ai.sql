WITH summary_ai_data AS (
    SELECT event_name,
        -- Extract 'record_id' from event_params
        MAX(
            CASE
                WHEN event_param.key = 'record_id' THEN event_param.value.string_value
            END
        ) AS record_id,
        -- Process timestamp conversion
        CASE
            WHEN LENGTH(CAST(event_timestamp AS STRING)) = 10 THEN TIMESTAMP_SECONDS(event_timestamp)
            WHEN LENGTH(CAST(event_timestamp AS STRING)) = 13 THEN TIMESTAMP_SECONDS(CAST(event_timestamp / 1000 AS INT64))
            WHEN LENGTH(CAST(event_timestamp AS STRING)) = 16 THEN TIMESTAMP_SECONDS(CAST(event_timestamp / 1000000 AS INT64))
        END AS created_at,
        -- Extract 'question' value if event is 'ai_notes_ask_question'
        MAX(
            CASE
                WHEN event_name = 'ai_notes_ask_question'
                AND event_param.key = 'value' THEN event_param.value.string_value
                ELSE ""
            END
        ) AS question,
        -- Extract 'task_id' for specified events
        MAX(
            CASE
                WHEN event_name IN (
                    'ai_notes_regenerate_replace_click',
                    'ai_notes_regenerate_insert_below_click',
                    'ai_notes_regenerate_discard_click',
                    'ai_notes_regenerate_click',
                    'ai_notes_section_delete_click',
                    'ai_notes_section_helpful_click',
                    'ai_notes_section_unhelpful_click'
                )
                AND event_param.key = 'trigger' THEN event_param.value.string_value
            END
        ) AS task_id,
        -- Extract 'prompt_id' for specified events
        MAX(
            CASE
                WHEN event_name IN (
                    'library_template_logo_click',
                    'ai_notes_area_use_template_click',
                    'ai_notes_area_space_use_template_click',
                    'ai_notes_expand_summary_library_click',
                    'ai_notes_collapse_summary_library_click'
                )
                AND event_param.key = 'template_id' THEN event_param.value.string_value
            END
        ) AS prompt_id,
        -- Extract 'third_platform_name' for 'ai_notes_send' event
        MAX(
            CASE
                WHEN event_name = 'ai_notes_send'
                AND event_param.key = 'trigger' THEN event_param.value.string_value
            END
        ) AS third_platform_name
    FROM `notta-data-analytics.analytics_458569263.events_intraday_*`,
        UNNEST(event_params) AS event_param
    WHERE event_name IN (
            'ai_notes_regenerate_replace_click',
            'ai_notes_regenerate_insert_below_click',
            'ai_notes_regenerate_discard_click',
            'ai_notes_regenerate_click',
            'ai_notes_section_delete_click',
            'ai_notes_section_helpful_click',
            'ai_notes_section_unhelpful_click',
            'library_template_logo_click',
            'ai_notes_area_use_template_click',
            'ai_notes_area_space_use_template_click',
            'ai_notes_expand_summary_library_click',
            'ai_notes_collapse_summary_library_click',
            'ai_notes_download',
            'ai_notes_copy_click',
            'ai_notes_send',
            'ai_notes_ask_question'
        )
    GROUP BY event_name,
        created_at
)
SELECT DISTINCT record_id,
    event_name,
    created_at,
    question,
    task_id,
    prompt_id,
    third_platform_name
FROM summary_ai_data
WHERE record_id IS NOT NULL
    AND created_at >= '2024-10-22'