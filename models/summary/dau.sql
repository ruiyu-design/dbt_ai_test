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

WITH all_days AS (
    SELECT
        android.event_date_dt,
        COALESCE(web.dau_web, 0) as dau_web,
        android.dau_android,
        ios.dau_ios
    FROM
        (SELECT
            parse_date('%Y%m%d',event_date) as event_date_dt,
            COUNT(DISTINCT user_pseudo_id) as dau_web
        FROM
            `notta-data-analytics.analytics_345009513.events_*`,
             UNNEST(event_params) as params
        WHERE
            device.web_info.hostname = 'app.notta.ai'
            AND (event_name = 'first_visit' OR (params.key = 'engagement_time_msec' AND params.value.int_value > 0))
            AND _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -3 day))
            AND date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -3 day)
        GROUP BY event_date_dt) web,
        (SELECT
            parse_date('%Y%m%d',event_date) as event_date_dt,
            COUNT(DISTINCT user_pseudo_id) as dau_android
        FROM
            `notta-data-analytics.analytics_234597866.events_*`,
             UNNEST(event_params) as params
        WHERE
            device.operating_system = 'Android'
            AND (event_name = 'first_open' OR (params.key = 'engagement_time_msec' AND params.value.int_value > 0))
            AND _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -3 day))
	        AND date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -3 day)
        GROUP BY event_date_dt) android,
        (SELECT
            parse_date('%Y%m%d',event_date) as event_date_dt,
            COUNT(DISTINCT user_pseudo_id) as dau_ios
        FROM
            `notta-data-analytics.analytics_234597866.events_*`
        WHERE
            device.operating_system = 'iOS'
            AND (event_name = 'first_open' OR event_name = 'user_engagement')
            AND _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -3 day))
            AND date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -3 day)
        GROUP BY event_date_dt) ios
    WHERE
        android.event_date_dt = web.event_date_dt
        AND android.event_date_dt = ios.event_date_dt
)
SELECT * FROM all_days
