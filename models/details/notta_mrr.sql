{{ config(
    materialized='table',
    full_refresh=True
) }}

with interest as(
--order_sn 对应的权益起止时间，排除已回收
select
	a.order_sn
	,min(timestamp_seconds(a.start_valid_time)) as interest_start_time
	,max(timestamp_seconds(a.flush_time)) as interest_end_time
from `notta-data-analytics.dbt_models_details.stg_aurora_interest` a
where
	a.flush_time>a.start_valid_time -- 排除已经回收的权益
group by
	a.order_sn
),

uid as (
--排除测试和临时邮箱账户
    select
        u.uid
        ,min_by(signup_platform,signup_time) as signup_platform
        ,min_by(signup_country,signup_time) as signup_country
        ,min_by(profession,signup_time) as profession
        ,min_by(role,signup_time) as role
    from `dbt_models_details.user_details` u
    where
    	pt=(select max(pt) from `dbt_models_details.user_details`)
    group by uid
),

order_table_pre as (
--过滤付费成功的订单,处理数据,增加partition字段方便后续sql可读
select
	a.workspace_id
	,a.uid
	,a.order_sn
	,case when a.origin_order_sn is null or a.origin_order_sn='' then a.order_sn else a.origin_order_sn end as origin_order_sn
    ,a.create_time
    ,a.update_time
    ,a.entry_time
    ,a.failure_time
    ,a.pay_currency
    ,a.status
    ,case when b.type=2 then 'addon'
    	when b.plan_type=0 then 'free'
    	when b.plan_type=1 then 'pro'
    	when b.plan_type=2 then 'biz'
    	when b.plan_type=3 then 'enterprise'
    	else 'unknown'
    end as plan_type_name
    ,case when b.period_unit=1 then 'annually'
    	when b.period_unit=2 then 'monthly'
    	when b.period_unit=3 then 'days'
    	when b.period_unit=4 then 'hours'
    	when b.period_unit=5 then 'mins'
    	else cast(period_unit as string) end as period_unit
    ,b.period_count
    ,case when a.pay_channel in (5,8) then 'Stripe'
	    when a.pay_channel=6 then 'App Store'
	    when a.pay_channel=7 then 'Google Play'
	    end as pay_channel
	,case when a.goods_id in (1040,1057) then 'basic'
    	when b.type=1 then 'basic'
	    when b.type=2 then 'addon'
	    when b.type=3 then 'discount'
	    when b.type=4 then 'edu'
	    when b.type=5 then 'add-seats'
	    when b.type=6 then 'trial'
	    when b.type=7 then 'retention-discount'
	    when b.type=8 then 'ai-addon'
	    when b.type=9 then 'translate-addon'
	    else cast(type as string) end as discount_type
    ,a.pay_amount
    ,a.goods_price
    ,b.goods_id
    ,a.seats_size
    ,ifnull(c.rate,1) as exchange_rate
	,concat(case when a.origin_order_sn is null or a.origin_order_sn='' then a.order_sn else a.origin_order_sn end,'-',workspace_id) as `partition`
from `notta-data-analytics.notta_aurora.payment_center_order_table` a
inner join `notta-data-analytics.notta_aurora.notta_mall_interest_goods` b on b.goods_id = a.goods_id
left join `notta-data-analytics.dbt_models_summary.exchange_rates` c on c.currency=a.pay_currency
where
	a.status in (4,8,13) --支付/升级成功
	and a.uid is not null
	and a.order_sn is not null
	and a.pay_currency is not null and a.pay_currency!=''
	and a.is_trial=0
	and a.pay_channel in (5,6,7,8)--stripe,google play,app store
	and b.type!=2--排除早期addon数据
),

order_table as(
--订单过滤,数据处理订单起止日期处理
select
	a.workspace_id
	,a.uid
	,a.order_sn
	,a.origin_order_sn
	,a.partition
	,a.pay_channel
	,a.pay_currency
	,a.status
	,case when period_unit ='monthly' and period_count=12 then concat(plan_type_name,'-annually')
		when period_unit ='annually' and period_count=1 then concat(plan_type_name,'-annually')
		else concat(plan_type_name,'-',period_unit,'-',period_count)
		end as plan_type_name
	,a.period_unit
	,a.period_count
	,a.discount_type
	,a.pay_amount
	,a.goods_price
	,a.goods_id
	,a.seats_size
	,a.exchange_rate
    ,timestamp_seconds(a.create_time) as create_time
    ,timestamp_seconds(a.update_time) as update_time
    ,case when a.entry_time=0 then timestamp_seconds(a.create_time) else timestamp_seconds(a.entry_time) end as entry_time
    ,case
    when a.failure_time=0 and a.period_unit='monthly' then cast(datetime_add(cast(timestamp_seconds(a.create_time) as datetime),interval a.period_count month) as timestamp)
		when a.failure_time=0 and a.period_unit='annually' then cast(datetime_add(cast(timestamp_seconds(a.create_time) as datetime),interval a.period_count year) as timestamp)
    when lead(a.entry_time) over (partition by a.partition order by a.create_time) is null
		or a.entry_time=lead(a.entry_time) over (partition by a.partition order by a.create_time)
			then timestamp_seconds(a.failure_time)
		when a.entry_time<lead(a.entry_time) over (partition by a.partition order by a.create_time)
			then lead(timestamp_seconds(a.entry_time)) over (partition by a.partition order by a.create_time)
		else timestamp_seconds(a.failure_time)
		end as failure_time
	,timestamp_seconds(a.entry_time) as entry_time_ori
    ,timestamp_seconds(a.failure_time) as failure_time_ori
    ,case when d.interest_start_time is not null then d.interest_start_time
    	when lag(d.interest_end_time) over (partition by a.partition order by a.create_time) is not null
    		and lead(d.interest_start_time) over (partition by a.partition order by a.create_time) is not null
			and lag(d.interest_end_time) over (partition by a.partition order by a.create_time)<lead(d.interest_start_time) over (partition by a.partition order by a.create_time)
		then lag(d.interest_end_time) over (partition by a.partition order by a.create_time)
    	else null end as interest_start_time
    ,case when d.interest_end_time is not null then d.interest_end_time
    	when lag(d.interest_end_time) over (partition by a.partition order by a.create_time) is not null
    		and lead(d.interest_start_time) over (partition by a.partition order by a.create_time) is not null
			and lag(d.interest_end_time) over (partition by a.partition order by a.create_time)<lead(d.interest_start_time) over (partition by a.partition order by a.create_time)
    		then lead(d.interest_start_time) over (partition by a.partition order by a.create_time)
    	else null end as interest_end_time
    ,d.interest_start_time as interest_start_time_ori
    ,d.interest_end_time as interest_end_time_ori
    ,row_number() over(partition by a.partition order by a.create_time) as rn
from order_table_pre a
left join interest d on a.order_sn=d.order_sn
where a.period_unit!='days'
),

origin_order_payment_agg as (
	--payment聚合到原始订单维度
select
	a.workspace_id
	,a.origin_order_sn
	,min_by(a.uid,a.rn) as uid
	,min_by(a.pay_channel,a.rn) as pay_channel
	,min_by(a.pay_currency,a.rn) as pay_currency
	,min_by(a.create_time,a.rn) as create_time
	,min_by(a.entry_time,a.rn) as start_time
	,max_by(a.failure_time,a.rn) as end_time
	,min(a.entry_time) as start_time_fix
	,max(a.failure_time) as end_time_fix
	,min_by(a.interest_start_time,a.rn) as interest_start_time
	,max_by(a.interest_end_time,a.rn) as interest_end_time
	,min(a.interest_start_time) as interest_start_time_fix
	,max(a.interest_end_time) as interest_end_time_fix
    ,sum(a.pay_amount/100/a.exchange_rate) as payment_amount
from order_table a
inner join uid as b on a.uid=b.uid
group by
	a.workspace_id
	,a.origin_order_sn
),

origin_order_payment_fix_interest as (
	--24年原始订单起止日期用权益数据校准
select
	a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.pay_channel
	,a.pay_currency
	,case when date(a.create_time)>='2024-01-01' then coalesce(interest_start_time,start_time)
		else start_time end as start_time
	,case when date(a.create_time)>='2024-01-01' then coalesce(interest_end_time,end_time)
		else end_time end as end_time
	,case when date(a.create_time)>='2024-01-01' then coalesce(interest_start_time_fix,start_time_fix)
		else start_time_fix end as start_time_fix
	,case when date(a.create_time)>='2024-01-01' then coalesce(interest_end_time_fix,end_time_fix)
		else end_time_fix end as end_time_fix
	,payment_amount
from origin_order_payment_agg a
),

origin_order_payment as (
	--订单起止日期校准权益数据时间顺序和订单创建时间顺序不匹配
select
	a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.pay_channel
	,a.pay_currency
	,case when date_diff(date(end_time),date(start_time),day)<=0 then date(start_time_fix)
	else date(start_time) end as start_date_final
	,case when date_diff(date(end_time),date(start_time),day)<=0 then date(end_time_fix)
	else date(end_time) end as end_date_final
	,case when date_diff(date(end_time),date(start_time),day)<=0 then date_diff(date(end_time_fix),date(start_time_fix),day)
	else date_diff(date(end_time),date(start_time),day)
	end as payment_datediff_final
	,payment_amount/
	case when date_diff(date(end_time),date(start_time),day)<=0 then date_diff(date(end_time_fix),date(start_time_fix),day)
	else date_diff(date(end_time),date(start_time),day)
	end as daily_payment_amount
	,payment_amount
	,1 as tag
from origin_order_payment_fix_interest a
where case when date_diff(date(end_time),date(start_time),day)<=0 then date_diff(date(end_time_fix),date(start_time_fix),day)
	else date_diff(date(end_time),date(start_time),day)
	end>0
),

calendar_table as (
select
	date as calendar_date
	,1 as tag
from UNNEST(GENERATE_DATE_ARRAY('2019-01-01', '2045-12-31', INTERVAL 1 DAY)) AS date
),

order_payment_daily as (
--订单付费金额拆分到天起止日期,包含开始日期,不包含结尾日期
select
	a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.pay_channel
	,a.pay_currency
	,a.start_date_final
	,a.end_date_final
	,a.payment_datediff_final
	,a.daily_payment_amount
	,a.payment_amount
	,b.calendar_date
from origin_order_payment a
inner join calendar_table b on a.tag=b.tag
where
	a.payment_amount>0
	and a.payment_datediff_final>0
	and a.start_date_final<=b.calendar_date
	and a.end_date_final>b.calendar_date
),

order_table_plan_change_detail as (
--取用户更改plan和seat的时间
select
	a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.order_sn
	,a.plan_type_name
	,a.seats_size
	,a.create_time
	,a.partition
	,lag(a.plan_type_name) over(partition by a.`partition` order by a.rn) as preceding_plan_type_name
	,lag(a.seats_size) over(partition by a.`partition` order by a.rn) as preceding_seats_size
	,case when rn=1 then timestamp(b.start_date_final)
		when lag(plan_type_name) over(partition by `partition` order by rn)!=plan_type_name
		or lag(seats_size) over(partition by `partition` order by rn)!=seats_size
		then create_time else null end as plan_change_create_time
from order_table a
inner join origin_order_payment b on a.workspace_id=b.workspace_id and a.origin_order_sn=b.origin_order_sn
),

order_table_plan_change_ori as (
--取用户更改plan和seat的时间处理,聚合到原始订单升级日期当天最后一个plan信息
select
	workspace_id
	,uid
	,origin_order_sn
	,a.partition
	,date(plan_change_create_time) as plan_change_create_date
	,max_by(plan_type_name,plan_change_create_time) as plan_type_name
	,max_by(seats_size,plan_change_create_time) as seats_size
from order_table_plan_change_detail a
where plan_change_create_time is not null
group by
	workspace_id
	,uid
	,origin_order_sn
	,a.partition
	,date(plan_change_create_time)
),

order_table_plan_change_range as(
--取原始订单更改plan起止日期范围
select
	a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.start_date_final
	,a.end_date_final
	,b.plan_type_name
	,b.seats_size
	,b.plan_change_create_date
	,case when lag(b.plan_type_name) over(partition by b.partition order by b.plan_change_create_date)='pro' and b.plan_type_name='biz'
		or lag(b.seats_size) over(partition by b.partition order by b.plan_change_create_date)<b.seats_size then 'Expansion'
		else null end as is_expansion
	,row_number() over(partition by a.workspace_id order by b.plan_change_create_date) as ws_rn
	,row_number() over(partition by a.workspace_id order by a.end_date_final desc,b.plan_change_create_date desc) as ws_rn_desc
	,COALESCE(lead(b.plan_change_create_date) over(partition by b.partition order by b.plan_change_create_date),a.end_date_final) as next_change_date
	,1 as tag
from origin_order_payment a
inner join order_table_plan_change_ori b on a.workspace_id=b.workspace_id and a.origin_order_sn=b.origin_order_sn
where
	a.payment_amount>0
	and a.payment_datediff_final>0
	and b.plan_change_create_date>=a.start_date_final
	and b.plan_change_create_date<a.end_date_final
),

order_table_plan_change_daily as (
--取原始订单有效期每天的plan type和mrr type,ws 第一个订单的前30天为new, 权益升级订单的前30天为expansion, 其他为正常续费
select
	a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.start_date_final
	,a.end_date_final
	,a.plan_type_name
	,a.seats_size
	,a.plan_change_create_date
	,a.next_change_date
	,case when a.ws_rn=1 and date_diff(calendar_date,plan_change_create_date,day)<30 then 'New'
		when a.is_expansion='Expansion' and date_diff(calendar_date,plan_change_create_date,day)<30 then 'Expansion'
		else 'Recurring' end as mrr_type
	,b.calendar_date
from order_table_plan_change_range a
inner join calendar_table b on a.tag=b.tag
where
	plan_change_create_date<=b.calendar_date
	and next_change_date>b.calendar_date
),

order_table_churn_daily as (
--取ws最后一个订单plan type, 结束后30天为churn mrr
select
  a.workspace_id
	,a.uid
	,a.origin_order_sn
	,a.start_date_final
	,a.end_date_final
	,a.plan_type_name
	,a.seats_size
	,a.plan_change_create_date
	,a.next_change_date
	,'Churned' as mrr_type
	,b.calendar_date
from order_table_plan_change_range a
inner join calendar_table b on a.tag=b.tag
where
	a.ws_rn_desc=1 and a.end_date_final=a.next_change_date
	and a.end_date_final<=b.calendar_date
	and date_add(end_date_final,interval 30 day)>b.calendar_date
),

final_table_daily as (
--分天数据合并
	select
		a.workspace_id
		,a.uid
		,COALESCE(c.signup_platform,'unknown') signup_platform
		,COALESCE(c.signup_country,'unknown') signup_country
		,COALESCE(c.profession,'unknown') profession
		,COALESCE(c.role,'unknown') role
		,a.origin_order_sn
		,a.calendar_date
		,a.pay_channel
		,a.pay_currency
		,a.start_date_final
		,a.end_date_final
		,a.payment_datediff_final
		,b.plan_change_create_date as plan_change_start_date
		,b.next_change_date as plan_change_end_date
		,b.plan_type_name
		,b.seats_size
		,case when b.seats_size=1 then '1 seat' when b.seats_size>1 then '2+ seats' else 'else' end as seat_type
		,b.mrr_type as mrr_type_detail
		,case when b.mrr_type!='New' then 'Recurring' else b.mrr_type end as mrr_type
		,a.daily_payment_amount
		,a.payment_amount
	from order_payment_daily a
	inner join order_table_plan_change_daily b on a.workspace_id=b.workspace_id and a.origin_order_sn=b.origin_order_sn and a.calendar_date=b.calendar_date
	inner join uid c on c.uid=a.uid
	-- where end_time_final<='2030-12-31'
	union all
	select
		a.workspace_id
		,a.uid
		,COALESCE(c.signup_platform,'unknown') signup_platform
		,COALESCE(c.signup_country,'unknown') signup_country
		,COALESCE(c.profession,'unknown') profession
		,COALESCE(c.role,'unknown') role
		,a.origin_order_sn
		,b.calendar_date
		,a.pay_channel
		,a.pay_currency
		,a.start_date_final
		,a.end_date_final
		,a.payment_datediff_final
		,b.plan_change_create_date as plan_change_start_date
		,b.next_change_date as plan_change_end_date
		,b.plan_type_name
		,b.seats_size
		,case when b.seats_size=1 then '1 seat' when b.seats_size>1 then '2+ seats' else 'else' end as seat_type
		,b.mrr_type as mrr_type_detail
		,b.mrr_type as mrr_type
		,a.daily_payment_amount
		,a.payment_amount
	from
		(
		select
			a.workspace_id
			,a.uid
			,a.origin_order_sn
			,a.calendar_date
			,a.pay_channel
			,a.pay_currency
			,a.start_date_final
			,a.end_date_final
			,a.payment_datediff_final
			,a.daily_payment_amount
			,a.payment_amount
		from order_payment_daily a
		where a.calendar_date=a.start_date_final
		) a
	inner join order_table_churn_daily b on a.workspace_id=b.workspace_id and a.origin_order_sn=b.origin_order_sn
	inner join uid c on c.uid=a.uid

)

select *
from final_table_daily