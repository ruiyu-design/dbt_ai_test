with user as(
select
	uid
	,signup_date
	,case when signup_country in ('Japan','United States','unknown','United Kingdom','France','Canada') then signup_country else 'Other' end as signup_country
from dbt_models_details.user_details
where pt=date_add(current_date(),interval -1 day)

),

transcription_case as (
	select *
	from
		(
		select
			record_id
			,type
			,row_number() over (partition by record_id order by create_time) as rn
		from `notta-data-analytics.dev_mc_data_statistics.record_title_type_v2`
		where
			type in ('Team Meeting','Video','Course','Consultation','Interview','Podcast','Conference','Phone Records')
		)
	where rn=1
),

transcription_translation as (
select
	record_id
	,min(timestamp_seconds(create_time)) as create_time
from `notta-data-analytics.notta_aurora.langogo_user_space_translate_results`
group by
	record_id
),

transcription_ai_summary as (
select
	record_id
	,min(created_at) as create_time
from `notta-data-analytics.dbt_models_details.stg_aurora_ai_records`
group by
	record_id
),

ws_owner as(
select
    workspace_id
	,owner_uid
from `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
where
	owner_uid is not null --过滤脏数据,目前不存在
	and workspace_id is not null --过滤脏数据,目前不存在
	and status!=2 --排除已删除
)

select
	date(a.create_date) as record_date
	,date(timestamp_trunc(a.create_date,ISOWEEK)) as record_week
	,date(timestamp_trunc(a.create_date,MONTH)) as record_month
	,a.found_uid as uid
	,a.workspace_id
	,ifnull(c.signup_country,'unknown') as signup_country
	,case when c.uid is null then 'Guest/Deleted/Internal User'
	    when b.goods_plan_type is null or b.is_trial=1 then 'Free/Trial'
		when b.goods_plan_type=0 then 'Starter'
		when b.goods_plan_type=1 then 'Pro'
		when b.goods_plan_type=2 then 'Biz'
		when b.goods_plan_type=3 then 'Enterprise'
		else 'else'
	end as plan_type
	,case when audio_duration<=600 then '1 0-10 mins'
		when audio_duration<=1800 then '2 10-30 mins'
		when audio_duration<=3600 then '3 30-60 mins'
		when audio_duration<=7200 then '4 1-2 hours'
		when audio_duration<=10800 then '5 2-3 hours'
		when audio_duration<=14400 then '6 3-4 hours'
		when audio_duration<=18000 then '7 4-5 hours'
		else '8 5+ hours'
	end as duration_category
	,case
		when a.transcription_type is null then 'Unknown'
		when a.transcription_type=1 then 'File'
		when a.transcription_type=2 then 'Real Time'
		when a.transcription_type=3 then 'Multilingual Meeting'
		when a.transcription_type=4 then 'Meeting'
		when a.transcription_type=5 then 'Accurate'
		when a.transcription_type=6 then 'Screen'
		when a.transcription_type=7 then 'Media Download'
		when a.transcription_type=8 then 'Multilingual File Transcribe'
		when a.transcription_type=9 then 'Subtitle'
		when a.transcription_type=10 then 'Multilingual RealTime Transcribe'
		when a.transcription_type=11 then 'Calendar Events Auto Join Meeting'
		when a.transcription_type=12 then 'Youtube'
		else 'unknown'
	end as transcription_type
	,case when media_source=2 then 'Web'
		when media_source in (3,4) then 'App'
		when media_source=5 then 'IWatch'
		when media_source=6 then 'Extension'
		when media_source=7 then 'Website'
		when media_source=8 then 'Smart Device'
		else 'else'
	end as transcribe_source
	,case when transcribe_language in ('ja-JP', 'en-US', 'es-ES', 'fr-FR', 'pt-PT', 'de-DE') then transcribe_language else 'Other' end as transcribe_language
	,ifnull(d.type,'Unknown') as transcribe_case
	,case when transcribe_speaker_num in (0,1) then 'No speaker identification'
		when transcribe_speaker_num=2 then 'Two-person meeting (2)'
		when transcribe_speaker_num=3 then 'Small-scale meeting (3-5)'
		when transcribe_speaker_num=4 then 'Large-scale meeting (6 or more)'
		when transcribe_speaker_num=-1 then 'Not Sure'
		else 'else'
	end as transcribe_speaker_num
	,count(a.record_id) as record_count
	,count(case when transcription_type in (3,4,11) then a.record_id when transcription_type not in (7,9,12) and audio_duration>1800 then a.record_id else null end) as meeting_record_count
	,count(case when a.share_status=1 then a.record_id else null end) as share_record_count
	,count(e.record_id) as translate_record_count
	,count(f.record_id) as summary_record_count
	,sum(a.audio_duration) as record_duration
from dbt_models_details.stg_aurora_record a
    inner join ws_owner wo on a.workspace_id=wo.workspace_id
	left join user c on wo.owner_uid=c.uid
	left join dbt_models_details.workspace_daily_paid_plan b on a.workspace_id=b.workspace_id and date(a.create_date)=b.calendar_date
	left join transcription_case d on a.record_id=d.record_id
	left join transcription_translation e on e.record_id=a.record_id
	left join transcription_ai_summary f on f.record_id=a.record_id
where
	a.workspace_id!=390255-- 排除小工具
	and media_source!=1 -- 排除langogo硬件
group by
	1,2,3,4,5,6,7,8,9,10,11,12,13