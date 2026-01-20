-- It is used to extract user uid and occupation information from GA questionnaire events, and filter dirty data
WITH profession_info AS (
    SELECT 
        uid_property.value.int_value as uid,
        CASE 
            WHEN user_property.value.string_value LIKE 'Other: %' THEN REGEXP_REPLACE(user_property.value.string_value, r'Other: ', '')
            ELSE user_property.value.string_value
        END as profession,
        ROW_NUMBER() OVER(PARTITION BY uid_property.value.int_value ORDER BY event_timestamp DESC) as row_num
    FROM 
        {{ ref('stg_ga4_onboarding_submit_survey') }},
        UNNEST(user_properties) AS user_property,
        UNNEST(user_properties) AS uid_property
    WHERE 
        user_property.key = 'member_profession'
        AND uid_property.key IN ('uid','user_id')
        AND uid_property.value.int_value IS NOT NULL
        AND user_property.value.string_value NOT LIKE '%login_onboarding_num1_answer%'
)

SELECT
    profession, uid
FROM
    profession_info
WHERE
    row_num = 1