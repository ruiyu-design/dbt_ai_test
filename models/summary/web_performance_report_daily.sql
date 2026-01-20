SELECT
    event_date_dt,
    country,

    -- 1. Web Vitals 核心指标 (cp_web_vitals)
    -- 首页性能指标
    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_lcp' THEN 1 END) AS home_lcp_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_lcp' THEN value END, 100)[OFFSET(50)] AS p50_home_lcp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_lcp' THEN value END, 100)[OFFSET(80)] AS p80_home_lcp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_lcp' THEN value END, 100)[OFFSET(90)] AS p90_home_lcp_ms,

    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_cls' THEN 1 END) AS home_cls_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_cls' THEN value END, 100)[OFFSET(50)] AS p50_home_cls_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_cls' THEN value END, 100)[OFFSET(80)] AS p80_home_cls_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_cls' THEN value END, 100)[OFFSET(90)] AS p90_home_cls_ms,

    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_inp' THEN 1 END) AS home_inp_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_inp' THEN value END, 100)[OFFSET(50)] AS p50_home_inp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_inp' THEN value END, 100)[OFFSET(80)] AS p80_home_inp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'home_inp' THEN value END, 100)[OFFSET(90)] AS p90_home_inp_ms,

    -- 详情页性能指标
    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_lcp' THEN 1 END) AS detail_lcp_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_lcp' THEN value END, 100)[OFFSET(50)] AS p50_detail_lcp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_lcp' THEN value END, 100)[OFFSET(80)] AS p80_detail_lcp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_lcp' THEN value END, 100)[OFFSET(90)] AS p90_detail_lcp_ms,

    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_cls' THEN 1 END) AS detail_cls_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_cls' THEN value END, 100)[OFFSET(50)] AS p50_detail_cls_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_cls' THEN value END, 100)[OFFSET(80)] AS p80_detail_cls_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_cls' THEN value END, 100)[OFFSET(90)] AS p90_detail_cls_ms,

    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_inp' THEN 1 END) AS detail_inp_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_inp' THEN value END, 100)[OFFSET(50)] AS p50_detail_inp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_inp' THEN value END, 100)[OFFSET(80)] AS p80_detail_inp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'detail_inp' THEN value END, 100)[OFFSET(90)] AS p90_detail_inp_ms,

    -- 登录页性能指标
    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_lcp' THEN 1 END) AS login_lcp_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_lcp' THEN value END, 100)[OFFSET(50)] AS p50_login_lcp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_lcp' THEN value END, 100)[OFFSET(80)] AS p80_login_lcp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_lcp' THEN value END, 100)[OFFSET(90)] AS p90_login_lcp_ms,

    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_cls' THEN 1 END) AS login_cls_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_cls' THEN value END, 100)[OFFSET(50)] AS p50_login_cls_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_cls' THEN value END, 100)[OFFSET(80)] AS p80_login_cls_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_cls' THEN value END, 100)[OFFSET(90)] AS p90_login_cls_ms,

    COUNT(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_inp' THEN 1 END) AS login_inp_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_inp' THEN value END, 100)[OFFSET(50)] AS p50_login_inp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_inp' THEN value END, 100)[OFFSET(80)] AS p80_login_inp_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_web_vitals' AND metric_name = 'login_inp' THEN value END, 100)[OFFSET(90)] AS p90_login_inp_ms,

    -- 2. 记录列表加载 (cp_record_list_load)
    COUNT(CASE WHEN event_name = 'cp_record_list_load' THEN 1 END) AS cp_record_list_load_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_record_list_load' THEN time END, 100)[OFFSET(50)] AS p50_record_list_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_record_list_load' THEN time END, 100)[OFFSET(80)] AS p80_record_list_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_record_list_load' THEN time END, 100)[OFFSET(90)] AS p90_record_list_load_ms,

    -- 3. 转录编辑器加载 (cp_transcription_load)
    COUNT(CASE WHEN event_name = 'cp_transcription_load' THEN 1 END) AS cp_transcription_load_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_transcription_load' THEN time END, 100)[OFFSET(50)] AS p50_transcription_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_transcription_load' THEN time END, 100)[OFFSET(80)] AS p80_transcription_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_transcription_load' THEN time END, 100)[OFFSET(90)] AS p90_transcription_load_ms,

    -- 4. AI Notes 编辑器加载 (cp_ai_notes_load)
    COUNT(CASE WHEN event_name = 'cp_ai_notes_load' THEN 1 END) AS cp_ai_notes_load_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_ai_notes_load' THEN time END, 100)[OFFSET(50)] AS p50_ai_notes_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_ai_notes_load' THEN time END, 100)[OFFSET(80)] AS p80_ai_notes_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_ai_notes_load' THEN time END, 100)[OFFSET(90)] AS p90_ai_notes_load_ms,

    -- 5. 实时转写进入详情页 (cp_realtime_transcribe_enter_detail)
    COUNT(CASE WHEN event_name = 'cp_realtime_transcribe_enter_detail' THEN 1 END) AS cp_realtime_transcribe_enter_detail_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_enter_detail' THEN time END, 100)[OFFSET(50)] AS p50_realtime_transcribe_enter_detail_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_enter_detail' THEN time END, 100)[OFFSET(80)] AS p80_realtime_transcribe_enter_detail_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_enter_detail' THEN time END, 100)[OFFSET(90)] AS p90_realtime_transcribe_enter_detail_ms,
    COUNT(DISTINCT CASE WHEN event_name = 'cp_realtime_transcribe_enter_detail' THEN record_id END) AS realtime_transcribe_unique_records,

    -- 6. 实时转写首字 (cp_realtime_transcribe_first_word)
    COUNT(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' THEN 1 END) AS cp_realtime_transcribe_first_word_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' THEN delay END, 100)[OFFSET(50)] AS p50_realtime_transcribe_first_word_delay_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' THEN delay END, 100)[OFFSET(80)] AS p80_realtime_transcribe_first_word_delay_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' THEN delay END, 100)[OFFSET(90)] AS p90_realtime_transcribe_first_word_delay_ms,
    MIN(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' THEN first_word_time END) AS first_word_earliest_time,
    MAX(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' THEN first_word_time END) AS first_word_latest_time,

    -- 7. 视频/音频加载 (cp_video_load)
    COUNT(CASE WHEN event_name = 'cp_video_load' THEN 1 END) AS cp_video_load_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_video_load' THEN time END, 100)[OFFSET(50)] AS p50_video_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_video_load' THEN time END, 100)[OFFSET(80)] AS p80_video_load_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_video_load' THEN time END, 100)[OFFSET(90)] AS p90_video_load_ms,

    -- 8. 视频缓冲 (cp_video_buffer)
    COUNT(CASE WHEN event_name = 'cp_video_buffer' THEN 1 END) AS cp_video_buffer_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_video_buffer' THEN time END, 100)[OFFSET(50)] AS p50_video_buffer_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_video_buffer' THEN time END, 100)[OFFSET(80)] AS p80_video_buffer_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_video_buffer' THEN time END, 100)[OFFSET(90)] AS p90_video_buffer_ms,

    -- 9. 实时转写首字延迟 - kinesis (effective_first_word_latency)
    COUNT(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type != 'websocket' THEN 1 END) AS effective_first_word_latency_kinesis_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type != 'websocket' THEN effective_first_word_latency END, 100)[OFFSET(50)] AS p50_kinesis_effective_first_word_latency_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type != 'websocket' THEN effective_first_word_latency END, 100)[OFFSET(80)] AS p80_kinesis_effective_first_word_latency_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type != 'websocket' THEN effective_first_word_latency END, 100)[OFFSET(90)] AS p90_kinesis_effective_first_word_latency_ms,
    MIN(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type != 'websocket' THEN first_word_time END) AS kinesis_first_word_earliest_time,
    MAX(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type != 'websocket' THEN first_word_time END) AS kinesis_first_word_latest_time,

    -- 10. 实时转写首字延迟 - websocket (effective_first_word_latency)
    COUNT(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type = 'websocket' THEN 1 END) AS effective_first_word_latency_websocket_count,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type = 'websocket' THEN effective_first_word_latency END, 100)[OFFSET(50)] AS p50_websocket_effective_first_word_latency_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type = 'websocket' THEN effective_first_word_latency END, 100)[OFFSET(80)] AS p80_websocket_effective_first_word_latency_ms,
    APPROX_QUANTILES(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type = 'websocket' THEN effective_first_word_latency END, 100)[OFFSET(90)] AS p90_websocket_effective_first_word_latency_ms,
    MIN(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type = 'websocket' THEN first_word_time END) AS websocket_first_word_earliest_time,
    MAX(CASE WHEN event_name = 'cp_realtime_transcribe_first_word' AND transport_type = 'websocket' THEN first_word_time END) AS websocket_first_word_latest_time
FROM
    `dbt_models_details.stg_web_cp_event`
WHERE
    event_date_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
GROUP BY event_date_dt, country
