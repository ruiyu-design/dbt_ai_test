/*
 * @field device
 * @description Operating system platform annotations
 * @type integer
 * @value 0 web
 */

WITH web_sign_ups AS (
    SELECT
        CASE
            WHEN user_property.key = 'uid' THEN user_property.value.int_value
            WHEN user_property.key = 'user_id' THEN user_property.value.int_value
        END AS uid,
        COALESCE(NULLIF(geo_country, ''), 'unknown') AS country,
        COALESCE(NULLIF(geo_city, ''), 'unknown') AS city,
        CASE
            WHEN device_operating_system IS NOT NULL THEN 0
            ELSE 99
        END AS device,
        device_category as device_category,
        event_params.value.string_value as first_user_landing_page,
        user_source,
        user_medium,
        user_campaign,
        event_date_dt,
        ROW_NUMBER() OVER (PARTITION BY CASE
            WHEN user_property.key = 'uid' THEN user_property.value.int_value
            WHEN user_property.key = 'user_id' THEN user_property.value.int_value
        END ORDER BY event_date_dt) AS row_num
    FROM
      {{ ref('stg_ga4__event_sign_up') }},
      UNNEST(user_properties) AS user_property,
      UNNEST(event_params) AS event_params

    WHERE
      user_property.key IN ('uid', 'user_id')
      and event_params.key='first_user_landing_page'
)

SELECT 
    uid,
    country,
    city,
    device,
    device_category,
    first_user_landing_page,
    user_source,
    user_medium,
    user_campaign,
    event_date_dt
FROM
    web_sign_ups
WHERE
    uid IS NOT NULL AND row_num = 1