{{ config(
    materialized='table',
    full_refresh=True
) }}

with new_user as (
--取注册用户注册时间
select
	user_id
	,min(event_date_dt) as min_date
	,FORMAT_DATE('%Y-W%V', min(event_date_dt)) as min_week
from {{ ref('stg_ga4_core_features') }}
where
    FORMAT_DATE('%Y-W%V', event_date_dt)<FORMAT_DATE('%Y-W%V', date(current_timestamp()))
group by
	user_id
),

active_user as (
select
	b.min_week
	,a.event_week
	,date_diff(event_date_dt,min_date,week(monday)) as week_diff
	,'WEB' as platform
	,count(distinct a.user_id) as active_users
from {{ ref('stg_ga4_core_features') }} a
inner join new_user b on b.user_id=a.user_id
where
	a.platform='WEB'
	and event_date_dt>=min_date
	and min_week>='2024-W01'
group by
	b.min_week
	,a.event_week
	,date_diff(event_date_dt,min_date,week(monday))
union all
select
	b.min_week
	,a.event_week
	,date_diff(event_date_dt,min_date,week(monday)) as week_diff
	,'App' as platform
	,count(distinct a.user_id) as active_users
from {{ ref('stg_ga4_core_features') }} a
inner join new_user b on b.user_id=a.user_id
where
	a.platform in ('iOS','Android')
	and event_date_dt>=min_date
	and min_week>='2024-W01'
group by
	b.min_week
	,a.event_week
	,date_diff(event_date_dt,min_date,week(monday))
union all
select
	b.min_week
	,a.event_week
	,date_diff(event_date_dt,min_date,week(monday)) as week_diff
	,'All' as platform
	,count(distinct a.user_id) as active_users
from {{ ref('stg_ga4_core_features') }} a
inner join new_user b on b.user_id=a.user_id
where
    event_date_dt>=min_date
	and min_week>='2024-W01'
group by
	b.min_week
	,a.event_week
	,date_diff(event_date_dt,min_date,week(monday))
)

select
    min_week
	,event_week
	,week_diff
	,platform
	,active_users
	,first_value(active_users) over (partition by platform,min_week order by week_diff asc) as first_active_users
from active_user





