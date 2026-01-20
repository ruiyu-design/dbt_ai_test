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

with combined_data as (
    SELECT
        user_id AS uid,
        user_pseudo_id,
        COALESCE(NULLIF(geo.country, ''), 'unknown') AS country,
        COALESCE(NULLIF(geo.city, ''), 'unknown') AS city,
        CASE
            WHEN device.operating_system = 'Android' THEN 1
            WHEN device.operating_system = 'iOS' THEN 2
            ELSE 99
        END AS device,
        device.category as device_category,
        app_info.install_source as install_source,
        traffic_source.name as user_campaign,
        traffic_source.medium as user_medium,
        traffic_source.source as user_source,
        parse_date('%Y%m%d',event_date) as event_date_dt
    FROM
    `analytics_234597866.events_*`
    WHERE
        event_name = 'sign_up_flutter_success_new'
        and REGEXP_CONTAINS(user_id, r'^[0-9]+$')
        and user_id IS NOT NULL
        and _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -3 day))
        and date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -3 day)
),

numbered_data as (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_date_dt) AS row_num
    FROM combined_data 
)

SELECT 
    uid,
    user_pseudo_id,
    country,
    city,
    device,
    device_category,
    install_source,
    user_campaign,
    user_medium,
    user_source,
    event_date_dt
FROM numbered_data 
WHERE row_num = 1