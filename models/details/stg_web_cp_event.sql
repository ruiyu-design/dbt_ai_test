WITH source AS (
    SELECT
        event_name,
        timestamp_micros(event_timestamp) AS event_timestamp,
        parse_date('%Y%m%d',event_date) AS event_date_dt,
        geo.country AS country,
        user_id,
        event_params
    FROM  `notta-data-analytics.analytics_342007657.events_*`
    WHERE
        event_name LIKE 'cp_%'
        AND _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -8 day))
        AND date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -8 day)
)


    SELECT
        event_name,
        event_timestamp,
        event_date_dt, -- 用于分区
        country,
        COALESCE(user_id,CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'user_id') AS STRING)) AS user_id,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'timestamp') AS INT64) AS timestamp,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'time') AS INT64) AS time,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'record_id') AS STRING) AS record_id,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_type') AS STRING) AS page_type,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') AS INT64) AS value,
        CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_name') AS STRING) AS metric_name,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'delay') AS INT64) AS delay,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'first_word_time') AS INT64) AS first_word_time,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'transport_type') AS transport_type,
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'effective_first_word_latency') AS INT64) AS effective_first_word_latency
    FROM source
