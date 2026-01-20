with combined_date as (
 -- 首次绑定ws数
select
	a.first_bind_date as date
	,a.signup_country
	,a.bind_user_type
	,count(a.workspace_id) as first_bind_ws
	,0 as first_starter_plan_ws
	,0 as first_upgrade_ws
from `notta-data-analytics.dbt_models_details.smart_device_ws_details` a
group by
	a.first_bind_date
	,a.signup_country
	,a.bind_user_type

union all
 -- 首次发放starter ws数
select
	a.first_starter_plan_date as date
	,a.signup_country
	,a.bind_user_type
	,0 as first_bind_ws
	,count(a.workspace_id) as first_starter_plan_ws
	,0 as first_upgrade_ws
from `notta-data-analytics.dbt_models_details.smart_device_ws_details` a
where first_starter_plan_date is not null -- 过滤没有发starter plan的数据
group by
	a.first_starter_plan_date
	,a.signup_country
	,a.bind_user_type

union all
 -- 首次从starter plan 升级到 pro及以上的用户
select
	a.first_upgrade_from_starter_date as date
	,a.signup_country
	,a.bind_user_type
	,0 as first_bind_ws
	,0 as first_starter_plan_ws
	,count(a.workspace_id) as first_upgrade_from_starter_ws
from `notta-data-analytics.dbt_models_details.smart_device_ws_details` a
where first_upgrade_from_starter_date is not null -- 过滤没有升级的数据
group by
	a.first_upgrade_from_starter_date
	,a.signup_country
	,a.bind_user_type
)

select
	date
	,signup_country
	,bind_user_type
	,sum(first_bind_ws) as first_bind_ws
	,sum(first_starter_plan_ws) as first_starter_plan_ws
	,sum(first_upgrade_ws) as upgrate_ws
from combined_date
group by
	date
	,signup_country
	,bind_user_type