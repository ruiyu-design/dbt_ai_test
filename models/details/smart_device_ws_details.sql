-- 硬件的数据
-- 1.新增绑定硬件的 ws 数
-- 1.硬件用户 转写 ws数 转写用户数 转写时长 dau
-- 2.新增starter plan 的 ws数
-- 3.发过starter plan 的 ws 付费率

-- 日/美
-- 新/老

with user as ( -- 过滤内部用户取用户数据
    select
        uid
        ,email
        ,signup_date
        ,signup_platform
        ,case when signup_country in ('Japan','United States','unknown') then signup_country else 'Other' end as signup_country
        ,date(first_paid_plan_start_time) as first_paid_date
        ,is_paid
    from dbt_models_details.user_details
    where pt=date_add(date(current_timestamp()),interval -1 day)

),

user_ws as(-- 取过滤后的ws数据
	select
		owner_uid
		,cast(workspace_id as int) as workspace_id
		,date(timestamp_millis(cast(create_time as int))) as create_ws_date
		,date(b.signup_date) as signup_date
		,b.signup_platform
		,b.signup_country
		,b.email
		,b.first_paid_date
		,b.is_paid
	from `notta-data-analytics.notta_aurora.langogo_user_space_workspace` a
	inner join user b on a.owner_uid=b.uid -- 过滤内部用户和临时邮箱
	where
		owner_uid is not null -- 过滤脏数据,目前不存在
		and workspace_id is not null -- 过滤脏数据,目前不存在
		and status!=2 -- 排除已删除
),

smart_device_bind_ws as ( -- 取绑定过的workspace 首次绑定时间，当前绑定状态
	select
	    starter_workspace_id as workspace_id
	    ,b.owner_uid
	    ,b.email as owner_email
	    ,b.signup_platform
	    ,b.signup_country
	    ,b.signup_date
	    ,b.first_paid_date
	    ,b.is_paid
	    ,date(timestamp_seconds(min(bind_time))) as first_bind_date
	    ,count(distinct a.uid) as workspace_bind_users
	    ,count(distinct case when status=1 then a.uid else null end) as workspace_binding_users
	from `notta-data-analytics.notta_aurora.langogo_user_space_user_ai_device` a
	inner join `notta-data-analytics.notta_aurora.langogo_user_space_ai_device_info` c on a.device_id=c.id and c.device_type=2 -- memo
	inner join user_ws b on a.starter_workspace_id=b.workspace_id
	group by
		starter_workspace_id
		,b.owner_uid
		,b.email
		,b.signup_platform
	    ,b.signup_country
	    ,b.signup_date
	    ,b.first_paid_date
	    ,b.is_paid
),

upgrade_date as (
	select
		workspace_id
		,min(calendar_date) as first_upgrade_from_starter_date
	from(
		select
		 workspace_id
		 ,calendar_date
		 ,goods_plan_type
		 ,is_trial
		 ,lag(goods_plan_type) over(partition by workspace_id order by calendar_date asc) as previous_goods_plan_type
		from `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan`
		where calendar_date<=date(current_timestamp()) -- 取截止昨天的数据
			and is_trial=0-- 排除试用
		)
	where goods_plan_type!=0 and previous_goods_plan_type=0 --取升级
	group by
		workspace_id


),

ws_detail as (

	select
		b.workspace_id
		,b.owner_uid
		,b.owner_email
		,b.signup_date
		,b.first_bind_date
		,b.workspace_bind_users
		,b.workspace_binding_users
		,b.signup_platform
	    ,b.signup_country
	    ,b.is_paid
	    ,b.first_paid_date
	    ,c.first_upgrade_from_starter_date
	    ,min(case when b.first_bind_date<=a.calendar_date and a.goods_plan_type=0 then a.calendar_date else null end) as first_starter_plan_date
	    ,max(case when b.first_bind_date=a.calendar_date then a.goods_plan_type else null end) as bind_date_plan_type
	    ,max_by(a.is_trial,case when b.first_bind_date=a.calendar_date then a.goods_plan_type else null end) as bind_date_plan_is_trial
	    ,max_by(a.goods_plan_type,calendar_date) as yesterday_plan_type
	from smart_device_bind_ws b
	left join `notta-data-analytics.dbt_models_details.workspace_daily_paid_plan` a on cast(a.workspace_id as int)=b.workspace_id
	left join upgrade_date c on cast(c.workspace_id as int)=b.workspace_id
	where
		calendar_date<date(current_timestamp()) -- 取截止昨天的数据
	group by
		b.workspace_id
		,b.owner_uid
		,b.owner_email
		,b.signup_date
		,b.first_bind_date
		,b.workspace_bind_users
		,b.workspace_binding_users
		,b.signup_platform
	    ,b.signup_country
	    ,b.is_paid
	    ,b.first_paid_date
	    ,c.first_upgrade_from_starter_date
)

select
    workspace_id
    ,owner_uid
    ,owner_email
    ,signup_date
    ,signup_platform
    ,signup_country
    ,first_bind_date
    ,workspace_bind_users
    ,workspace_binding_users
    ,case
    	when bind_date_plan_type is null then null
    	when bind_date_plan_type=0 then 'Starter'
    	when bind_date_plan_type=1 then 'Pro'
    	when bind_date_plan_type=2 then 'Biz'
    	when bind_date_plan_type=3 then 'Enterprise'
    	else 'unknown'
    	end as bind_date_plan_type
    ,bind_date_plan_is_trial
    ,case when date_diff(first_bind_date,signup_date,DAY)<=3 then 'New User'
		when date_diff(first_bind_date,signup_date,DAY)>3 and (is_paid=0 or first_paid_date=first_upgrade_from_starter_date) then 'Existing Free User'
		when date_diff(first_bind_date,signup_date,DAY)>3 and is_paid=1 and bind_date_plan_type!=0 and bind_date_plan_is_trial=0 then 'Existing Paying User'
		when date_diff(first_bind_date,signup_date,DAY)>3 and is_paid=1 and first_paid_date<first_upgrade_from_starter_date  then 'Churned User'
	else 'Old' end as bind_user_type
	,first_starter_plan_date
    ,is_paid
    ,first_paid_date
    ,first_upgrade_from_starter_date
    ,case
    	when yesterday_plan_type is null then null
    	when yesterday_plan_type=0 then 'Starter'
    	when yesterday_plan_type=1 then 'Pro'
    	when yesterday_plan_type=2 then 'Biz'
    	when yesterday_plan_type=3 then 'Enterprise'
    	else 'unknown'
    	end as yesterday_plan_type
from ws_detail
