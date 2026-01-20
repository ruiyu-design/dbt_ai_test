

select
	'DAU' as metric_name
	,date(a.create_date) as start_date
	,case
    	when c.goods_plan_type is null then 'Starter'
    	when c.goods_plan_type=0 then 'Starter'
    	when c.goods_plan_type=1 then 'Pro'
    	when c.goods_plan_type=2 then 'Biz'
    	when c.goods_plan_type=3 then 'Enterprise'
    	else 'else' end as plan_type
	,count(distinct a.workspace_id) as record_ws
	,count(distinct a.found_uid) as record_user
	,count(a.record_id) as record_count
	,round(sum(audio_duration/60),2) as record_duration_sum
from `dbt_models_details.stg_aurora_record` a
inner join `notta-data-analytics.dbt_models_details.smart_device_ws_details` b on b.workspace_id=a.workspace_id
left join`notta-data-analytics.dbt_models_details.workspace_daily_paid_plan` c on c.workspace_id=a.workspace_id and date(a.create_date)=c.calendar_date
left join `notta-data-analytics.notta_aurora.langogo_user_space_records_device_info` d on a.record_id=d.record_id
where a.media_source=8
and (d.device_brand='Memo' or d.device_brand is null )
group by
	date(a.create_date)
	,case
    	when c.goods_plan_type is null then 'Starter'
    	when c.goods_plan_type=0 then 'Starter'
    	when c.goods_plan_type=1 then 'Pro'
    	when c.goods_plan_type=2 then 'Biz'
    	when c.goods_plan_type=3 then 'Enterprise'
    	else 'else' end

union all

select
	'WAU' as metric_name
	,date(date_trunc(a.create_date,ISOWEEK)) as start_date
	,case
    	when c.goods_plan_type is null then 'Starter'
    	when c.goods_plan_type=0 then 'Starter'
    	when c.goods_plan_type=1 then 'Pro'
    	when c.goods_plan_type=2 then 'Biz'
    	when c.goods_plan_type=3 then 'Enterprise'
    	else 'else' end as plan_type
	,count(distinct a.workspace_id) as record_ws
	,count(distinct a.found_uid) as record_user
	,count(a.record_id) as record_count
	,round(sum(audio_duration/60),2) as record_duration_sum
from `dbt_models_details.stg_aurora_record` a
inner join `notta-data-analytics.dbt_models_details.smart_device_ws_details` b on b.workspace_id=a.workspace_id
left join`notta-data-analytics.dbt_models_details.workspace_daily_paid_plan` c on c.workspace_id=a.workspace_id and date(a.create_date)=c.calendar_date
left join `notta-data-analytics.notta_aurora.langogo_user_space_records_device_info` d on a.record_id=d.record_id
where a.media_source=8
and (d.device_brand='Memo' or d.device_brand is null )
group by
	date(date_trunc(a.create_date,ISOWEEK))
	,case
    	when c.goods_plan_type is null then 'Starter'
    	when c.goods_plan_type=0 then 'Starter'
    	when c.goods_plan_type=1 then 'Pro'
    	when c.goods_plan_type=2 then 'Biz'
    	when c.goods_plan_type=3 then 'Enterprise'
    	else 'else' end

union all
select
	'MAU' as metric_name
	,date(date_trunc(a.create_date,MONTH)) as start_date
	,case
    	when c.goods_plan_type is null then 'Starter'
    	when c.goods_plan_type=0 then 'Starter'
    	when c.goods_plan_type=1 then 'Pro'
    	when c.goods_plan_type=2 then 'Biz'
    	when c.goods_plan_type=3 then 'Enterprise'
    	else 'else' end as plan_type
	,count(distinct a.workspace_id) as record_ws
	,count(distinct a.found_uid) as record_user
	,count(a.record_id) as record_count
	,round(sum(audio_duration/60),2) as record_duration_sum
from `dbt_models_details.stg_aurora_record` a
inner join `notta-data-analytics.dbt_models_details.smart_device_ws_details` b on b.workspace_id=a.workspace_id
left join`notta-data-analytics.dbt_models_details.workspace_daily_paid_plan` c on c.workspace_id=a.workspace_id and date(a.create_date)=c.calendar_date
left join `notta-data-analytics.notta_aurora.langogo_user_space_records_device_info` d on a.record_id=d.record_id
where a.media_source=8
and (d.device_brand='Memo' or d.device_brand is null )
group by
	date(date_trunc(a.create_date,MONTH))
	,case
    	when c.goods_plan_type is null then 'Starter'
    	when c.goods_plan_type=0 then 'Starter'
    	when c.goods_plan_type=1 then 'Pro'
    	when c.goods_plan_type=2 then 'Biz'
    	when c.goods_plan_type=3 then 'Enterprise'
    	else 'else' end




