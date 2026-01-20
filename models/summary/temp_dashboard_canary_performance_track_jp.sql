-- 日语实时转写，引擎分配
-- 策略为: 【免费用户】+【日语实时转写】 + 【无词汇表】 + 【非医疗引擎】+【 ws_id 后两位 >= 50】

-- 1. 满足：免费用户,付费前音频均为实时转写,付费前音频均不需要医疗引擎,进入测试
-- 2. ws_id>=50, dolphin;
-- 2. ws_id<50, ami;

with

uid as (-- 目标注册用户筛选
select
	uid
	,signup_time
	,is_paid
	,first_paid_plan_start_time
	,case when first_paid_plan_start_time is null then current_timestamp() else first_paid_plan_start_time end as paid_compare_time
	,first_paid_plan_type
	,signup_platform
	,device_category
	,source
	,signup_date
	,is_trial
	,is_create_record
FROM `notta-data-analytics.dbt_models_details.user_details`
where pt=date_add(date(current_timestamp()),interval -1 day)
and is_create_ws=1 -- 选择创建ws的用户
and signup_date>='2025-07-25'
and first_transcription_type='Real Time'-- 首个转写是实时转写
and first_transcription_language='ja-JP'-- 首个转写语言是日语
),


record as (

	select
		found_uid as uid
		,signup_date
		,signup_time
		,is_paid
		,is_create_record
		,first_paid_plan_start_time
		,is_trial
		,signup_platform
		,device_category
		,source
		,count(case when transcribe_language!='ja-JP' or transcription_type!=2 or engine='medical' then 1 else null end) as match_other_records
		,count(case when transcribe_language='ja-JP' and transcription_type=2 and right(cast(workspace_id as string),2)>='50' then 1 else null end) as dolphin_records
		,count(case when transcribe_language='ja-JP' and transcription_type=2 and right(cast(workspace_id as string),2)<'50' then 1 else null end) as ami_records
	from `dbt_models_details.stg_aurora_record` a
	inner join uid b on a.found_uid=cast(b.uid as int)
	where
		create_date<=paid_compare_time -- 取付费前的转写记录
		and workspace_id!=390255-- 排除小工具
	group by
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
)

select
  signup_date
  ,signup_platform
  ,device_category
  ,source
  ,case when match_other_records>0 then 'mixed records'
	 when dolphin_records>0 then 'dolphin only'
	 when ami_records>0 then 'ami only'
	 else 'else'
	 end as is_canary
  ,count(1) as user_count
  ,count(distinct uid) as user_check
  ,count(distinct case when is_create_record=1 then uid else null end) as create_record_users
  ,count(distinct case when is_trial=1 then uid else null end) as trial_users
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=72 then uid else null end) as paid_users_in72hours
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=96 then uid else null end) as paid_users_in96hours
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=24 then uid else null end) as paid_users_in24hours
from record
where match_other_records=0
group by
  signup_date
  ,signup_platform
  ,device_category
  ,source
  ,case when match_other_records>0 then 'mixed records'
	 when dolphin_records>0 then 'dolphin only'
	 when ami_records>0 then 'ami only'
	 else 'else'
	 end






