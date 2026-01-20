/*
 * @field trigger_type
 * @description Trigger type
 * @type INTEGER
 * @value 1 Manual trigger
 * @value 2 Automatic summary
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
 * @field transcription_type
 * @description Transcription type
 * @type INTEGER
 * @value 1 File File transcription
 * @value 2 RealTime Real-time transcription
 * @value 3 MultilingualMeeting Multilingual meeting transcription
 * @value 4 Meeting Meeting transcription
 * @value 5 Accurate Accurate transcription
 * @value 6 Screen Screen recording transcription
 * @value 7 MediaDownload Media download transcription
 * @value 8 MultilingualFileTranscribe Multilingual file transcription
 * @value 9 Subtitle Subtitle transcription
 * @value 10 MultilingualRealTimeTranscribe Multilingual real-time transcription
 * @value 11 CalendarEventsAutoJoinMeeting Calendar events auto-join meeting transcription
 */
WITH ai_records AS (
    SELECT
        workspace_id,
        uid,
        record_id,
        prompt_id,
        task_id,
        language,
        platform,
        transcription_type,
        trigger_type,
        regenerate,
        plan_type,
        summary_status,
        error_code,
        TIMESTAMP_SECONDS(timestamp) AS created_at
    FROM
        {{ source('Statistics', 'notta_summary_ai_records') }}
)
SELECT
    workspace_id,
    uid,
    record_id,
    prompt_id,
    task_id,
    language,
    platform,
    transcription_type,
    trigger_type,
    regenerate,
    plan_type,
    summary_status,
    error_code,
    created_at
FROM
    ai_records
WHERE
    workspace_id IS NOT NULL
    AND uid IS NOT NULL
    AND record_id IS NOT NULL