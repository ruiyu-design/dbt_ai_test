
with offline_trial_order as (
-- 筛选线下试用套餐
select distinct
order_sn
from `notta-data-analytics.notta_aurora.notta_oms_manager_present_send`
where order_sn!=''
and present_type in (1,2,3) --1、免费赠送；2、补偿赠送；3、试用
),


-- 推算用户每天是什么权益（只考虑24年后starter及以上的权益）
pro_interest as (
select
	a.*
	,c.period_unit
	,c.period_count
	,case when a.start_valid_time!=0 and a.start_valid_time!=a.flush_time then a.start_valid_time -- 正常情况
		when a.start_valid_time=0 and b.create_time is not null then b.create_time -- 回收但有订单的权益取订单开始时间
		else a.create_time -- 回收但没有订单的权益（取创建时间）
	end as order_fix_start_time
	,case when a.flush_time=0 or a.flush_time=a.start_valid_time then a.update_time else a.flush_time end as fix_flush_time -- 如果权益回收，则用更新时间作为权益结束时间，风险为更新时间有可能大于权益回收时间
	,coalesce(b.origin_order_sn,a.order_sn) as origin_order_sn
	,case when d.order_sn is not null then 1 -- 线下试用
	    when b.is_trial is null then 1 -- 线上试用
		when c.period_unit not in (1,2) or b.is_trial=1 then 1 -- 过滤非年月单位的均为试用
		when b.is_trial=0 then 0
		else 1 end as is_trial
	,COALESCE(CAST(JSON_VALUE(a.common_interest, '$.seats') AS INT64),0) as seats_size
from `notta-data-analytics.dbt_models_details.stg_aurora_interest` a
left join offline_trial_order d on a.order_sn=d.order_sn
left join `notta-data-analytics.notta_aurora.payment_center_order_table` b on a.order_sn=b.order_sn
inner join `notta-data-analytics.notta_aurora.notta_mall_interest_goods` c on b.goods_id = c.goods_id
where
	a.goods_plan_type!=0 -- 排除free
	and a.goods_type not in (2,5,7,8,9) -- 排除addon
	and (b.status is null or b.status in (4,8,13)) -- 排除退费订单
),


fixed_interest as (
select
	a.id
	,a.uid
	,a.workspace_id
	,a.order_sn
	,a.order_source
	,a.goods_id
	,a.goods_type
	,a.goods_plan_type
	,a.start_valid_time
	,a.flush_time
	,a.update_time
	,a.extension_period
	,a.create_time
	,a.order_fix_start_time
	,a.fix_flush_time
	,a.period_count
	,a.period_unit
	,date(timestamp_seconds(a.fix_flush_time)) as flush_date
	,date(timestamp_seconds(a.order_fix_start_time)) as start_date
	,is_trial
	,seats_size
from pro_interest a
where
	order_fix_start_time!=0
	and fix_flush_time!=0
	and date(timestamp_seconds(a.order_fix_start_time))<=date(timestamp_seconds(a.fix_flush_time))
),

smart_device_bind_ws as (
	select
	    starter_workspace_id as workspace_id
	    ,0 as goods_plan_type
	    ,date(timestamp_seconds(bind_time)) as bind_date
	    ,case when unbind_time=0 then date_add(date(current_timestamp()),INTERVAL 1 MONTH) else date(timestamp_seconds(unbind_time)) end as unbind_date
	from `notta-data-analytics.notta_aurora.langogo_user_space_user_ai_device` a
),

interest_union as (
	select
		cast(workspace_id as int) as workspace_id
		,start_date
		,flush_date as end_date
		,goods_plan_type
		,is_trial
		,seats_size
		,period_count
		,period_unit
		,1 as tag
	from fixed_interest
union all
	select
		workspace_id
		,bind_date as start_date
		,unbind_date as end_date
		,goods_plan_type
		,0 as is_trial
		,1 as seats_size
		,1 as period_count
		,2 as period_unit
		,1 as tag
	from smart_device_bind_ws
),

calendar_table as (
select
	date as calendar_date
	,1 as tag
from UNNEST(GENERATE_DATE_ARRAY('2024-01-01', date_add(date(current_timestamp()),INTERVAL 3 MONTH), INTERVAL 1 DAY)) AS date
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
	a.workspace_id
	,c.owner_uid
	,b.calendar_date
	,max(goods_plan_type) as goods_plan_type
	,max_by(is_trial,goods_plan_type) as is_trial
	,max_by(seats_size,goods_plan_type) as seats_size
	,max_by(period_unit,goods_plan_type) as period_unit
	,max_by(period_count,goods_plan_type) as period_count
from interest_union a
inner join ws_owner c on c.workspace_id=a.workspace_id
inner join calendar_table b on a.tag=b.tag
where
	a.start_date<=b.calendar_date
	and a.end_date>=b.calendar_date
group by
	a.workspace_id
	,c.owner_uid
	,b.calendar_date
order by workspace_id,calendar_date asc


