
with interest_consume_record as ( -- member 维度数据
select
	workspace_id
	,uid
	,business_type
	,measure_unit
	,number
	,a.create_time
from `notta-data-analytics.notta_aurora.notta_mall_interest_user_package_interest_consume_record` a
inner join `notta-data-analytics.notta_aurora.notta_mall_interest_goods` b on a.package_id=b.goods_id
where
	workspace_id is not null
	and workspace_id!=''
	and uid is not null
	and a.create_time!=0
	and b.plan_type in (2,3) -- biz和enterprise
	and b.period_unit in (1,2) -- 排除试用
	and date(TIMESTAMP_SECONDS(a.create_time))>='2024-10-01'
),


interest_record as ( -- ws 维度数据

select
  usage.workspace_id,
  start_valid_time,
  flush_time,
  CASE usage.goods_plan_type
  WHEN 2 THEN 'Biz'
  WHEN 3 THEN 'Enterprise'
  ELSE 'Others'
  END AS plan_type,
  -- notta_interest_import_audio
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_import_audio.total') AS INT64),0) AS `import_file_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_import_audio.used') AS INT64),0) AS `import_file_used`,

  -- notta_interest_ai_summary
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_ai_summary.total') AS INT64),0) AS `ai_summary_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_ai_summary.used') AS INT64),0) AS `ai_summary_used`,

  -- notta_interest_chat_bot
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_new_chat_bot.total') AS INT64),0) AS `ai_chat_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.notta_interest_new_chat_bot.used') AS INT64),0) AS `ai_chat_used`,

  -- duration
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.duration.total') AS INT64),0) AS `duration_limit`,
  COALESCE(CAST(JSON_VALUE(usage.consume_interest, '$.duration.used') AS INT64),0) AS `duration_used`,

  COALESCE(CAST(JSON_VALUE(usage.common_interest, '$.seats') AS INT64),0) as seats_size,

  row_number() over (partition by concat(workspace_id,start_valid_time) order by create_time desc) as rn

FROM
	`notta-data-analytics.dbt_models_details.stg_aurora_interest` usage
WHERE
    usage.goods_plan_type IN (2,3) -- biz和enterprise
	and usage.goods_type not in (2,5,7,8,9) -- 排除add on
	and start_valid_time<flush_time --未回收数据
	and TIMESTAMP_SECONDS(start_valid_time)<current_timestamp() --排除未开始权益
)

select
	a.workspace_id
	,a.uid as member_uid
	,TIMESTAMP_SECONDS(b.start_valid_time) as start_t
	,b.plan_type as ws_plan_type
	,b.import_file_limit as ws_import_file_limit
	,b.import_file_used as ws_import_file_used
	,b.ai_summary_limit as ws_ai_summary_limit
	,b.ai_summary_used as ws_ai_summary_used
	,b.ai_chat_limit as ws_ai_chat_limit
	,b.ai_chat_used as ws_ai_chat_used
	,b.duration_limit as ws_duration_limit
	,b.duration_used as ws_duration_used
	,b.seats_size as ws_seats_size
	,sum(case when a.business_type='duration' then a.number else 0 end) as member_duration_used
	,count(case when a.business_type='notta_interest_ai_summary' then 1 else null end) as member_ai_summary_used
	,count(case when a.business_type='notta_interest_new_chat_bot' then 1 else null end) as memeber_ai_chat_used
	,count(case when a.business_type='notta_interest_import_audio' then 1 else null end) as member_import_audio_used
	,count(case when a.business_type='notta_interest_calendar_events_meeting_robot' then 1 else null end) as member_calendar_events_meeting_robot_used
	,count(case when a.business_type='notta_interest_meeting_robot' then 1 else null end) as member_meeting_robot_used
	,count(case when a.business_type='notta_interest_multilingual_file_transcribe' then 1 else null end) as member_multilingual_file_transcribe_used
	,count(case when a.business_type='notta_interest_multilingual_transcribe' then 1 else null end) as member_multilingual_transcribe_used
	,count(case when a.business_type='notta_interest_real_time_multilingual' then 1 else null end) as real_time_multilingual_used
	,count(case when a.business_type='notta_interest_realtime_transcription' then 1 else null end) as realtime_transcription_used
	,count(case when a.business_type='notta_interest_realtime_translate' then 1 else null end) as realtime_translate_used
	,count(case when a.business_type='notta_interest_speaker_insight' then 1 else null end) as speaker_insight_used
	,count(case when a.business_type='notta_interest_translate' then 1 else null end) as translate_used
from interest_consume_record a
inner join interest_record b on a.workspace_id=b.workspace_id
where
	b.rn=1 -- 保证同一开始时间只有一条数据
	and b.seats_size>1 --筛选大于1席位的ws
	and a.create_time>=b.start_valid_time
	and a.create_time<=b.flush_time
group by
	a.workspace_id
	,a.uid
	,TIMESTAMP_SECONDS(b.start_valid_time)
	,b.plan_type
	,b.import_file_limit
	,b.import_file_used
	,b.ai_summary_limit
	,b.ai_summary_used
	,b.ai_chat_limit
	,b.ai_chat_used
	,b.duration_limit
	,b.duration_used
	,b.seats_size




