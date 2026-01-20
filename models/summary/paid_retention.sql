{{ config(
    materialized='table',
    full_refresh=True
) }}

-- 付费留存率 基于mrr

with ws_min_order as(
--取ws的首个付费套餐开始时间及标签
select
	workspace_id
	,min(calendar_date) as min_start_date
	,min_by(pay_channel,calendar_date) as min_pay_channel
	,min_by(seat_type,calendar_date) as min_seat_type
	,min_by(plan_type_name,calendar_date) as min_plan_type_name
	,min_by(case when signup_country in ('United States','Japan') then signup_country
            else 'Others' end ,calendar_date) as min_signup_country
	,min_by(profession,calendar_date) as min_profession
from {{ ref('notta_mrr') }}
where
	mrr_type!='Churned'
group by
	workspace_id
),

--
--order_table as (
----取ws的首个plan类型
--select
--	a.workspace_id
--    ,min(a.create_time) as min_create_time
--    ,min_by(case when b.type=2 then 'addon'
--    	when b.plan_type=0 then 'free'
--    	when b.plan_type=1 then 'pro'
--    	when b.plan_type=2 then 'biz'
--    	when b.plan_type=3 then 'enterprise'
--    	else 'unknown'
--    end,a.create_time) as plan_type_name
--    ,min_by(case when b.period_unit=1 then 'annually'
--    	when b.period_unit=2 then 'monthly'
--    	when b.period_unit=3 then 'days'
--    	when b.period_unit=4 then 'hours'
--    	when b.period_unit=5 then 'mins'
--    	else cast(period_unit as string) end,a.create_time) as period_unit
--    ,min_by(b.period_count,a.create_time) as period_count
--	,min_by(case when a.goods_id in (1040,1057) then 'basic'
--    	when b.type=1 then 'basic'
--	    when b.type=2 then 'addon'
--	    when b.type=3 then 'discount'
--	    when b.type=4 then 'edu'
--	    when b.type=5 then 'add-seats'
--	    when b.type=6 then 'trial'
--	    when b.type=7 then 'retention-discount'
--	    when b.type=8 then 'ai-addon'
--	    when b.type=9 then 'translate-addon'
--	    else cast(type as string) end,a.create_time) as discount_type
--from `notta-data-analytics.notta_aurora.payment_center_order_table` a
--inner join `notta-data-analytics.notta_aurora.notta_mall_interest_goods` b on b.goods_id = a.goods_id
--where
--	a.status in (4,8,13) --支付/升级成功
--	and a.uid is not null
--	and a.order_sn is not null
--	and a.order_sn is not null
--	and b.period_unit in (1,2) --只留月付年付
--	and a.pay_currency is not null and a.pay_currency!=''
--	and a.is_trial=0
--	and a.pay_channel in (5,6,7,8)--stripe,google play,app store
--	and a.create_time > UNIX_SECONDS(TIMESTAMP('2021-01-01'))
--	and b.type!=2--排除早期addon数据
--group by
--	a.workspace_id
--),

paid_retention as (
select
	date_trunc(b.min_start_date,MONTH) as first_paid_month
	,date_trunc(a.calendar_date,MONTH) as paid_retained_month
	,date_diff(a.calendar_date,b.min_start_date,MONTH) as month_diff
	,min_plan_type_name as first_paid_plan
	,min_seat_type as first_seat_type
	,min_pay_channel as first_paid_channel
	,min_signup_country as signup_country
	,count(distinct a.workspace_id) as ws_count
from {{ ref('notta_mrr') }} a
inner join ws_min_order b on b.workspace_id=a.workspace_id
--left join order_table c on c.workspace_id=a.workspace_id
where
	mrr_type!='Churned'
	and
	(
		a.calendar_date=b.min_start_date
		or
		(--只取首次付费后每个月的同一天看是否留存
			format_date('%d',b.min_start_date)<=format_date('%d',last_day(a.calendar_date,MONTH))
			and
			format_date('%d',a.calendar_date)=format_date('%d',b.min_start_date)
		)
		or
		(--如果首次付费时间的天大于当月的最后一天的天值，则取最后一天
			format_date('%d',b.min_start_date)>format_date('%d',last_day(a.calendar_date,MONTH))
			and
			format_date('%d',a.calendar_date)=format_date('%d',last_day(a.calendar_date,MONTH))
		)
	)
	and date_trunc(a.calendar_date,MONTH)<date_trunc(date(current_timestamp()),MONTH)--取完整月数据
group by
	date_trunc(b.min_start_date,MONTH)
	,date_trunc(a.calendar_date,MONTH)
	,date_diff(a.calendar_date,b.min_start_date,MONTH)
	,min_plan_type_name
	,min_seat_type
	,min_pay_channel
	,min_signup_country
)

select
    first_paid_month
    ,paid_retained_month
    ,month_diff
    ,first_paid_plan
    ,first_seat_type
    ,first_paid_channel
    ,signup_country
    ,ws_count
    ,first_value(ws_count) over (partition by first_paid_month,first_paid_plan,first_seat_type,first_paid_channel,signup_country order by month_diff asc) as first_ws_count
from paid_retention