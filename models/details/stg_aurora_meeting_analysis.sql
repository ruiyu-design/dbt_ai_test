/*
 * @field is_record_screen
 * @description Whether to record the screen
 * @type integer
 * @value 0 Enabled
 * @value 1 Disabled
 */

WITH meeting_analysis AS (
    SELECT
        record_id,
        meeting_source,
        meeting_type,
        is_record_screen
    FROM
        {{ source('Aurora', 'mc_data_analysis_meeting_analysis') }}
    WHERE
        product = 'notta'
)

SELECT
    record_id,
    meeting_source,
    meeting_type,
    is_record_screen
FROM
    meeting_analysis
WHERE
    record_id IS NOT NULL
    AND meeting_source IS NOT NULL
    AND meeting_type IS NOT NULL