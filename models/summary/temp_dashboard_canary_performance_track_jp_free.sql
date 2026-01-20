WITH uid AS (
-- CTE 1: 筛选目标注册用户
SELECT
    uid
    ,signup_time
    ,is_paid
    ,first_paid_plan_start_time
    -- 如果用户未付费，则使用当前时间作为比较时间点
    ,CASE WHEN first_paid_plan_start_time IS NULL THEN current_timestamp() ELSE first_paid_plan_start_time END AS paid_compare_time
    ,first_paid_plan_type
    ,signup_platform
    ,device_category
    ,source
    ,signup_date
    ,is_trial
    ,is_create_record
    ,first_transcription_language
FROM `notta-data-analytics.dbt_models_details.user_details`
WHERE pt = date_add(date(current_timestamp()),interval -1 DAY)
AND is_create_ws=1 -- 选择创建了工作空间 (WS) 的用户
AND signup_date>='2025-11-20' -- 注册用户时间范围起点
and signup_country = 'Japan' -- 国家为日本
AND first_transcription_type='Real Time' -- 首个转写为实时转写
AND first_transcription_language IN ('ja-JP') -- 首个转写语言为日语
-- and current_plan_type = 'Free'
),


record AS (
-- CTE 2: 统计用户在付费前（或至今）的转写记录，用于分组和付费指标
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
        ,first_transcription_language

        -- 统计不符合测试条件的记录（用于排除混合用户）
        ,count(CASE WHEN transcribe_language NOT IN ('ja-JP') -- 非日语
                    OR transcription_type!=2 -- 非实时转写 (2=实时)
                    OR engine='medical'
                    OR audio_duration>16200 THEN 1 ELSE NULL END) AS match_other_records

        -- 统计实验组 (11labs) 的纯净实时日语转写记录
        ,count(CASE WHEN transcribe_language IN ('ja-JP')
                    AND transcription_type=2
                    AND engine!='medical'
                    AND audio_duration<=16200
                    AND (right(cast(workspace_id AS string),2) BETWEEN '80' AND '99') -- WS ID 尾号 >= 80
                    THEN 1 ELSE NULL END) AS `test_group_11labs_records`

        -- 统计对照组 (AMI/线上) 的纯净实时日语转写记录
        ,count(CASE WHEN transcribe_language IN ('ja-JP')
                    AND transcription_type=2
                    AND engine!='medical'
                    AND audio_duration<=16200
                    AND right(cast(workspace_id AS string),2) BETWEEN '00' AND '79' -- WS ID 尾号 < 80
                    THEN 1 ELSE NULL END) AS `control_group_ami_records`

    FROM `dbt_models_details.stg_aurora_record` a
    INNER JOIN uid b ON a.found_uid=cast(b.uid AS INT)
    WHERE
        create_date<=paid_compare_time -- 只取付费前的转写记录
        AND workspace_id!=390255
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

-- CTE 3: 统计用户在相互排斥的周活跃窗口内的留存行为
retention_record AS (
    SELECT
        a.found_uid AS uid,
        b.signup_date,

        -- 1. D+1 留存（注册后次日）
        MAX(CASE WHEN date_diff(DATE(a.create_date), b.signup_date, DAY) = 1
                    AND a.transcription_type = 2
                    AND a.transcribe_language IN ('ja-JP') THEN 1 ELSE 0 END) AS retained_day1,

        -- 2. W1 留存 (D+1 到 D+7，即下一周)
        MAX(CASE WHEN date_diff(DATE(a.create_date), b.signup_date, DAY) BETWEEN 1 AND 7
                    AND a.transcription_type = 2
                    AND a.transcribe_language IN ('ja-JP') THEN 1 ELSE 0 END) AS retained_w1_d1_d7,

        -- 3. W2 留存 (D+8 到 D+14，即下下一周/第三周)
        MAX(CASE WHEN date_diff(DATE(a.create_date), b.signup_date, DAY) BETWEEN 8 AND 14
                    AND a.transcription_type = 2
                    AND a.transcribe_language IN ('ja-JP') THEN 1 ELSE 0 END) AS retained_w2_d8_d14

    FROM `dbt_models_details.stg_aurora_record` a
    INNER JOIN uid b ON a.found_uid=cast(b.uid AS INT)
    WHERE
        -- 修正类型错误：将 a.create_date 转换为 DATE
        DATE(a.create_date) > b.signup_date
        AND a.workspace_id != 390255
        AND a.transcription_type = 2
        AND a.transcribe_language IN ('ja-JP')
        -- 检查范围扩大到 D+14
        AND date_diff(DATE(a.create_date), b.signup_date, DAY) <= 14
    GROUP BY 1, 2
)

-- 最终查询: 将分组、付费和留存数据聚合
SELECT
    r.signup_date
    ,r.signup_platform
    ,r.device_category
    ,r.source
    ,r.first_transcription_language

    -- 根据记录情况划分最终的 AB 测试组别
    ,CASE WHEN r.match_other_records>0 THEN 'mixed records'
        WHEN r.test_group_11labs_records > 0 THEN '11labs only (Test)' -- 实验组
        WHEN r.control_group_ami_records > 0 THEN 'AMI & dolphin (Control)' -- 对照组
        ELSE 'else'
        END AS is_canary

    ,count(1) AS user_count -- 总注册用户数
    ,count(distinct r.uid) AS user_check
    ,count(distinct CASE WHEN r.is_create_record=1 THEN r.uid ELSE NULL END) AS create_record_users -- 注册转写用户数
    ,count(distinct CASE WHEN r.is_trial=1 THEN r.uid ELSE NULL END) AS trial_users

    -- 注册付费用户数（分时间窗口）
    ,count(distinct CASE WHEN r.is_paid=1 AND datetime_diff(r.first_paid_plan_start_time,r.signup_time,HOUR)<=72 THEN r.uid ELSE NULL END) AS paid_users_in72hours
    ,count(distinct CASE WHEN r.is_paid=1 AND datetime_diff(r.first_paid_plan_start_time,r.signup_time,HOUR)<=96 THEN r.uid ELSE NULL END) AS paid_users_in96hours
    ,count(distinct CASE WHEN r.is_paid=1 AND datetime_diff(r.first_paid_plan_start_time,r.signup_time,HOUR)<=24 THEN r.uid ELSE NULL END) AS paid_users_in24hours

    -- 实时转写留存用户数 (相互排斥的周活跃)
    ,count(distinct CASE WHEN rr.retained_day1 = 1 THEN r.uid ELSE NULL END) AS retained_day1_users -- D+1 留存
    ,count(distinct CASE WHEN rr.retained_w1_d1_d7 = 1 THEN r.uid ELSE NULL END) AS retained_w1_d1_d7_users -- W1 留存 (D+1 到 D+7)
    ,count(distinct CASE WHEN rr.retained_w2_d8_d14 = 1 THEN r.uid ELSE NULL END) AS retained_w2_d8_d14_users -- W2 留存 (D+8 到 D+14)

FROM record r
LEFT JOIN retention_record rr ON r.uid = rr.uid -- 将留存记录与分组数据合并
WHERE r.match_other_records=0 -- 关键筛选：排除有非日语/非实时转写记录的用户，确保分组纯净
GROUP BY 1,2,3,4,5,6