
-- 中间表：用户统计
{{ config(materialized='table') }}
WITH

uid AS (
-- 目标注册用户筛选
SELECT
    uid
    ,signup_time
    ,is_paid
    ,first_paid_plan_start_time
    ,CASE WHEN first_paid_plan_start_time IS NULL THEN current_timestamp() ELSE first_paid_plan_start_time END AS paid_compare_time
    ,first_paid_plan_type
    ,signup_platform
    ,device_category
    ,source
    ,signup_date
    ,is_trial
    ,is_create_record
    ,first_transcription_language -- 增加语言维度
FROM `notta-data-analytics.dbt_models_details.user_details`
WHERE pt=date_add(date(current_timestamp()),interval -1 DAY)
AND is_create_ws=1 -- 选择创建ws的用户
AND signup_date>='2025-11-20' -- 更改：测试的注册用户时间
-- AND signup_date<='2025-11-12' -- 更改：测试的注册用户时间
AND first_transcription_type='Real Time'-- 首个转写是文件转写
-- 更改：测试改为下面多个语言了
AND first_transcription_language IN ('en-US')
),


record AS (

    SELECT
       found_uid AS uid
       ,signup_date
       ,signup_time
       ,is_paid
       ,is_create_record
       ,first_paid_plan_start_time
       ,is_trial
       ,signup_platform
       ,device_category
       ,source
       ,first_transcription_language -- 增加语言维度
       -- 更改：不匹配测试条件的记录
       ,count(CASE WHEN transcribe_language NOT IN ('en-US')
                --    OR transcription_type!=2
                   OR engine='medical'
                   OR audio_duration>16200 THEN 1 ELSE NULL END) AS match_other_records
       -- 更改：测试组 (11labs) 00-24 或 75-99
       ,count(CASE WHEN transcribe_language IN ('en-US')
                --    AND transcription_type=2
                   AND engine!='medical'
                   AND audio_duration<=16200
                   AND (right(cast(workspace_id AS string),2) BETWEEN '50' AND '99')
                   THEN 1 ELSE NULL END) AS `test_group_11labs_records`
       -- 更改：对照组 (ami)
       ,count(CASE WHEN transcribe_language IN ('en-US')
                --    AND transcription_type=2
                   AND engine!='medical'
                   AND audio_duration<=16200
                   AND right(cast(workspace_id AS string),2) BETWEEN '00' AND '49'
                   THEN 1 ELSE NULL END) AS `control_group_ami_records`
    FROM `dbt_models_details.stg_aurora_record` a
    INNER JOIN uid b ON a.found_uid=cast(b.uid AS INT)
    WHERE
       create_date<=paid_compare_time -- 取付费前的转写记录
       AND workspace_id!=390255-- 排除小工具
    GROUP BY
       found_uid
       ,signup_date
       ,signup_time
       ,is_paid
       ,is_create_record
       ,is_trial
       ,first_paid_plan_start_time
       ,signup_platform
       ,device_category
       ,source
       ,first_transcription_language -- 增加语言维度
)

SELECT
 signup_date
 ,signup_platform
 ,device_category
 ,source
  ,first_transcription_language -- 增加语言维度
  -- 更改：更新分组名称
 ,CASE WHEN match_other_records>0 THEN 'mixed records'
     WHEN test_group_11labs_records > 0 THEN 'assemblyai  only (Test)'
     WHEN control_group_ami_records > 0 THEN 'old only (Control)'
     ELSE 'else'
     END AS is_canary
 ,count(1) AS user_count
 ,count(distinct uid) AS user_check
 ,count(distinct CASE WHEN is_create_record=1 THEN uid ELSE NULL END) AS create_record_users
 ,count(distinct CASE WHEN is_trial=1 THEN uid ELSE NULL END) AS trial_users
 ,count(distinct CASE WHEN is_paid=1 AND datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=72 THEN uid ELSE NULL END) AS paid_users_in72hours
 ,count(distinct CASE WHEN is_paid=1 AND datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=96 THEN uid ELSE NULL END) AS paid_users_in96hours
 ,count(distinct CASE WHEN is_paid=1 AND datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=24 THEN uid ELSE NULL END) AS paid_users_in24hours
FROM record
WHERE match_other_records=0 -- 筛选出纯粹的测试用户
GROUP BY
 signup_date
 ,signup_platform
 ,device_category
 ,source
 ,first_transcription_language -- 增加语言维度
  -- 更改：更新分组名称
 ,CASE WHEN match_other_records>0 THEN 'mixed records'
     WHEN test_group_11labs_records > 0 THEN 'assemblyai  only (Test)'
     WHEN control_group_ami_records > 0 THEN 'old only (Control)'
     ELSE 'else'
     END
