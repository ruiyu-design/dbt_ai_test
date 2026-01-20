{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        }
    )
}}

-- CTE 1: 配对事件处理（时长和完整率）
-- 将所有 _start 和 _end 事件通过 transaction_id 进行配对
WITH duration_events AS (
    SELECT
        -- 使用 COALESCE 确保即使只有 start 或 end 事件，维度也能正确填充
        COALESCE(s.event_date_dt, e.event_date_dt) AS event_date_dt,
        COALESCE(s.platform, e.platform) AS platform,
        COALESCE(s.app_version, e.app_version) AS app_version,

        -- 提取事件的基础名称，例如 'cp_coldstart'
        REPLACE(REPLACE(COALESCE(s.event_name, e.event_name), '_start', ''), '_end', '') AS event_base_name,
        COALESCE(s.transaction_id, e.transaction_id) AS transaction_id,
        e.transaction_id IS NOT NULL AS has_end, -- 标记是否存在 end 事件
        s.transaction_id IS NOT NULL AS has_start, -- 标记是否存在 start 事件
        TIMESTAMP_MILLIS(e.timestamp) AS end_timestamp,
        TIMESTAMP_MILLIS(s.timestamp) AS start_timestamp,
        (e.timestamp - s.timestamp) AS duration_ms
    FROM
        (
            SELECT
                event_date_dt,
                platform,
                app_version,
                event_name,
                timestamp,
                transaction_id
            FROM
                `dbt_models_details.stg_app_cp_event`
            WHERE
                event_name LIKE '%_start'
                AND transaction_id IS NOT NULL
                AND event_date_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
        ) AS s
    FULL OUTER JOIN
        (
            SELECT
                event_date_dt,
                platform,
                app_version,
                event_name,
                timestamp,
                transaction_id
            FROM
                `dbt_models_details.stg_app_cp_event`
            WHERE
                event_name LIKE '%_end'
                AND transaction_id IS NOT NULL
                 AND event_date_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
        ) AS e
        ON s.transaction_id = e.transaction_id
)
-- 主查询：聚合所有指标
SELECT
    base.event_date_dt,
    base.platform,
    COALESCE(base.app_version, 'All Versions') AS app_version,

    -- 1. 冷启动平均时长 (秒)
    COUNT(CASE WHEN base.event_name = 'cp_coldstart_end' THEN 1 ELSE NULL END) AS cp_coldstart_end_count,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_coldstart_end' THEN dur.duration_ms ELSE NULL END, 100)[OFFSET(50)] / 1000 AS p50_cold_start_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_coldstart_end' THEN dur.duration_ms ELSE NULL END, 100)[OFFSET(80)] / 1000 AS p80_cold_start_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_coldstart_end' THEN dur.duration_ms ELSE NULL END, 100)[OFFSET(90)] / 1000 AS p90_cold_start_duration_seconds,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_coldstart_end' THEN base.transaction_id ELSE NULL END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_coldstart_start', 'cp_coldstart_end') THEN base.transaction_id ELSE NULL END)) AS cold_start_completion_rate,

    -- 2. 实时转写
    COUNT(CASE WHEN base.event_name = 'cp_real_time_record_finish' THEN 1 ELSE NULL END) AS cp_real_time_record_finish_count,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_real_time_record_finish' AND base.drop_count > 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_real_time_record_finish' THEN 1 ELSE NULL END)) AS real_time_transcription_drop_rate,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_real_time_record_finish' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_real_time_record_finish' THEN 1 ELSE NULL END)) AS real_time_transcription_interrupt_rate,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_real_time_record_finish' THEN base.transaction_id END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_real_time_record_start', 'cp_real_time_record_finish') THEN base.transaction_id END)) AS real_time_transcription_completion_rate,

    -- 3. wifi传输
    COUNT(CASE WHEN base.event_name = 'cp_smart_device_wifi_trans_end' THEN 1 ELSE NULL END) AS cp_smart_device_wifi_trans_end_count,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_smart_device_wifi_trans_end' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_smart_device_wifi_trans_end' THEN 1 ELSE NULL END)) AS wifi_transfer_failure_rate,
    SAFE_DIVIDE(SUM(CASE WHEN base.event_name = 'cp_smart_device_wifi_trans_end' THEN base.trans_file_size ELSE 0 END), SUM(CASE WHEN base.event_name = 'cp_smart_device_wifi_trans_end' THEN base.trans_duration ELSE 0 END)/1000) AS avg_wifi_transfer_speed_bps,

    -- 4. 蓝牙传输
    COUNT(CASE WHEN base.event_name = 'cp_smart_device_ble_trans_end' THEN 1 ELSE NULL END) AS cp_smart_device_ble_trans_end_count,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_smart_device_ble_trans_end' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_smart_device_ble_trans_end' THEN 1 ELSE NULL END)) AS ble_transfer_failure_rate,
    SAFE_DIVIDE(SUM(CASE WHEN base.event_name = 'cp_smart_device_ble_trans_end' THEN base.trans_file_size ELSE 0 END), SUM(CASE WHEN base.event_name = 'cp_smart_device_ble_trans_end' THEN base.trans_duration ELSE 0 END)/1000) AS avg_ble_transfer_speed_bps,

    -- 5. 固件升级
    COUNT(CASE WHEN base.event_name = 'cp_smart_device_firmware_update_end' THEN 1 ELSE NULL END) AS cp_smart_device_firmware_update_end_count,
    AVG(CASE WHEN base.event_name = 'cp_smart_device_firmware_update_end' THEN dur.duration_ms ELSE NULL END) / 1000 AS avg_firmware_update_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_smart_device_firmware_update_end' AND dur.duration_ms >= 0 THEN dur.duration_ms ELSE NULL END, 100)[OFFSET(50)] / 1000 AS p50_firmware_update_duration_seconds,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_smart_device_firmware_update_end' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_smart_device_firmware_update_end' THEN 1 ELSE NULL END)) AS firmware_update_error_rate,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_smart_device_firmware_update_end' THEN base.transaction_id ELSE NULL END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_smart_device_firmware_update_start', 'cp_smart_device_firmware_update_end') THEN base.transaction_id ELSE NULL END)) AS firmware_update_completion_rate,

    -- 6. 硬件解绑与绑定
    COUNT(CASE WHEN base.event_name = 'cp_smart_device_unbind' THEN 1 ELSE NULL END) AS cp_smart_device_unbind_count,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_smart_device_unbind' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_smart_device_unbind' THEN 1 ELSE NULL END)) AS device_unbind_error_rate,
    COUNT(CASE WHEN base.event_name = 'cp_smart_device_bind' THEN 1 ELSE NULL END) AS cp_smart_device_bind_count,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_smart_device_bind' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_smart_device_bind' THEN 1 ELSE NULL END)) AS device_bind_error_rate,

    -- 7. Memo蓝牙连接
    COUNT(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 THEN 1 ELSE NULL END) AS cp_memo_ble_connect_end_count,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(50)] / 1000 AS p50_memo_ble_connect_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(80)] / 1000 AS p80_memo_ble_connect_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(90)] / 1000 AS p90_memo_ble_connect_duration_seconds,
    COUNT(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 AND dur.duration_ms < 0 THEN 1 END) AS negative_duration_count_memo_ble_connect,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 THEN 1 ELSE NULL END)) AS memo_ble_connect_error_rate,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_memo_ble_connect_end' AND is_manual=0 THEN base.transaction_id ELSE NULL END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_memo_ble_connect_start', 'cp_memo_ble_connect_end') AND is_manual=0 THEN base.transaction_id ELSE NULL END)) AS memo_ble_connect_completion_rate,

    -- 8. 实时转写首字延迟
    COUNT(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' THEN 1 ELSE NULL END) AS cp_real_time_record_first_word_end_count,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(50)] / 1000 AS p50_first_word_delay_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(80)] / 1000 AS p80_first_word_delay_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(90)] / 1000 AS p90_first_word_delay_duration_seconds,
    COUNT(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' AND dur.duration_ms < 0 THEN 1 END) AS negative_duration_count_first_word_delay,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' THEN 1 ELSE NULL END)) AS first_word_delay_error_rate,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_real_time_record_first_word_end' THEN base.transaction_id ELSE NULL END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_real_time_record_first_word_start', 'cp_real_time_record_first_word_end') THEN base.transaction_id ELSE NULL END)) AS first_word_delay_completion_rate,

    -- 9. 记录详情页加载
    COUNT(CASE WHEN base.event_name = 'cp_record_detail_end' THEN 1 ELSE NULL END) AS cp_record_detail_end_count,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_record_detail_end' AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(50)] / 1000 AS p50_record_detail_load_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_record_detail_end' AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(80)] / 1000 AS p80_record_detail_load_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_record_detail_end' AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(90)] / 1000 AS p90_record_detail_load_duration_seconds,
    COUNT(CASE WHEN base.event_name = 'cp_record_detail_end' AND dur.duration_ms < 0 THEN 1 END) AS negative_duration_count_record_detail_load,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_record_detail_end' AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_record_detail_end' THEN 1 ELSE NULL END)) AS record_detail_load_error_rate,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_record_detail_end' THEN base.transaction_id ELSE NULL END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_record_detail_start', 'cp_record_detail_end') THEN base.transaction_id ELSE NULL END)) AS record_detail_load_completion_rate,

    -- 10. 记录列表页加载
    COUNT(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 THEN 1 ELSE NULL END) AS cp_record_list_end_count,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(50)] / 1000 AS p50_record_list_load_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(80)] / 1000 AS p80_record_list_load_duration_seconds,
    APPROX_QUANTILES(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 AND dur.duration_ms >= 0 THEN dur.duration_ms END, 100)[OFFSET(90)] / 1000 AS p90_record_list_load_duration_seconds,
    COUNT(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 AND dur.duration_ms < 0 THEN 1 END) AS negative_duration_count_record_list_load,
    SAFE_DIVIDE(COUNT(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 AND base.error_type != 0 THEN 1 ELSE NULL END), COUNT(CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 THEN 1 ELSE NULL END)) AS record_list_load_error_rate,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN base.event_name = 'cp_record_list_end' AND is_folder=0 THEN base.transaction_id ELSE NULL END), COUNT(DISTINCT CASE WHEN base.event_name IN ('cp_record_list_start', 'cp_record_list_end') AND is_folder=0 THEN base.transaction_id ELSE NULL END)) AS record_list_load_completion_rate
FROM
    `dbt_models_details.stg_app_cp_event` AS base
LEFT JOIN
    duration_events AS dur ON base.transaction_id = dur.transaction_id
WHERE
    base.event_date_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
GROUP BY
    GROUPING SETS (
        (base.event_date_dt, base.platform),
        (base.event_date_dt, base.platform, base.app_version)
    )