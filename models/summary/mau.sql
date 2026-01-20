-- Web ios android monthly active user statistics, time partition table, update the data of the previous month at the beginning of each month

{% set partitions_to_replace = ['date_sub(current_date, interval 1 month)'] %}

{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions = partitions_to_replace,
    )
}}

with
    web_active_user as (
        SELECT
            parse_date('%Y%m', FORMAT_DATE('%Y%m', parse_date('%Y%m%d',event_date))) as event_date_dt,
            COUNT(DISTINCT user_pseudo_id) as mau_web
        FROM
            `notta-data-analytics.analytics_345009513.events_{{ get_last_month() }}*`,
             UNNEST(event_params) as params
        WHERE
            device.web_info.hostname = 'app.notta.ai'
            AND (event_name = 'first_visit' OR (params.key = 'engagement_time_msec' AND params.value.int_value > 0)) 
        GROUP BY event_date_dt
    ),
    android_active_user as (
        SELECT
            parse_date('%Y%m', FORMAT_DATE('%Y%m', parse_date('%Y%m%d',event_date))) as event_date_dt,
            COUNT(DISTINCT user_pseudo_id) as mau_android
        FROM
            `notta-data-analytics.analytics_234597866.events_{{ get_last_month() }}*`,
             UNNEST(event_params) as params
        WHERE
            device.operating_system = 'Android'
            AND (event_name = 'first_open' OR (params.key = 'engagement_time_msec' AND params.value.int_value > 0))
        GROUP BY event_date_dt
    ),
    ios_active_user as (
        SELECT
            parse_date('%Y%m', FORMAT_DATE('%Y%m', parse_date('%Y%m%d',event_date))) as event_date_dt,
            COUNT(DISTINCT user_pseudo_id) as mau_ios
        FROM
            `notta-data-analytics.analytics_234597866.events_{{ get_last_month() }}*`
        WHERE
            device.operating_system = 'iOS'
            AND (event_name = 'first_open' OR event_name = 'user_engagement')
        GROUP BY event_date_dt
    )
SELECT
    android.event_date_dt,
    COALESCE(web.mau_web, 0) as mau_web,
    android.mau_android,
    ios.mau_ios
FROM
    android_active_user android
LEFT JOIN
    web_active_user web
ON
    android.event_date_dt = web.event_date_dt
LEFT JOIN
    ios_active_user ios
ON
    android.event_date_dt = ios.event_date_dt
