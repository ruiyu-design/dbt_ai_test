-- OKR 进度看板

with fact as (
	select
		'Week' as segment
		,date_trunc(date,WEEK(MONDAY)) as date
		,case when platform='WEB' then 'WEB' else 'APP' end as platform
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end signup_country
		,'Completed' as status
		,sum(signup_users) as signup_users
		,sum(create_record_users) as create_record_users
		,sum(new_paid_users) as new_paid_users
		,round(sum(first_payment_rev),2) as new_revenue
		,round(sum(cumulative_rev),2) as total_revenue
		,round(sum(case when source in ('GoogleAds','BingAds','YahooAds','MetaAds','ASA', 'MolocoAds', 'AppierAds', 'TiktokAds', 'MintegralAds') then first_payment_rev else 0 end),2) as ads_first_payment
	from `dbt_models_details.marketing_report_detail`
	where date>='2024-10-01' and date<date(current_timestamp)
	group by
		date_trunc(date,WEEK(MONDAY))
		,case when platform='WEB' then 'WEB' else 'APP' end
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end


	union all
	select
		'Month' as segment
		,date_trunc(date,MONTH) as date
		,case when platform='WEB' then 'WEB' else 'APP' end as platform
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end as signup_country
		,'Completed' as status
		,sum(signup_users) as signup_users
		,sum(create_record_users) as create_record_users
		,sum(new_paid_users) as new_paid_users
		,round(sum(first_payment_rev),2) as new_revenue
		,round(sum(cumulative_rev),2) as total_revenue
		,round(sum(case when source in ('GoogleAds','BingAds','YahooAds','MetaAds','ASA', 'MolocoAds', 'AppierAds', 'TiktokAds', 'MintegralAds') then first_payment_rev else 0 end),2) as ads_first_payment
	from `dbt_models_details.marketing_report_detail`
	where date>='2024-10-01' and date<date(current_timestamp)
	group by
		date_trunc(date,MONTH)
		,case when platform='WEB' then 'WEB' else 'APP' end
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end


	union all
	select
		'Quarter' as segment
		,date_trunc(date,QUARTER) as date
		,case when platform='WEB' then 'WEB' else 'APP' end as platform
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end as signup_country
		,'Completed' as status
		,sum(signup_users) as signup_users
		,sum(create_record_users) as create_record_users
		,sum(new_paid_users) as new_paid_users
		,round(sum(first_payment_rev),2) as new_revenue
		,round(sum(cumulative_rev),2) as total_revenue
		,round(sum(case when source in ('GoogleAds','BingAds','YahooAds','MetaAds','ASA', 'MolocoAds', 'AppierAds', 'TiktokAds', 'MintegralAds') then first_payment_rev else 0 end),2) as ads_first_payment
	from `dbt_models_details.marketing_report_detail`
	where date>='2024-10-01' and date<date(current_timestamp)
	group by
		date_trunc(date,QUARTER)
		,case when platform='WEB' then 'WEB' else 'APP' end
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end

	union all
	select
		'Week' as segment
		,date_trunc(date,WEEK(MONDAY)) as date
		,case when platform='WEB' then 'WEB' else 'APP' end as platform
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end as signup_country
		,'Not Completed' as status
		,sum(signup_users) as signup_users
		,sum(create_record_users) as create_record_users
		,sum(new_paid_users) as new_paid_users
		,round(sum(first_payment_rev),2) as new_revenue
		,round(sum(cumulative_rev),2) as total_revenue
		,round(sum(case when source in ('GoogleAds','BingAds','YahooAds','MetaAds','ASA', 'MolocoAds', 'AppierAds', 'TiktokAds', 'MintegralAds') then first_payment_rev else 0 end),2) as ads_first_payment
	from `dbt_models_details.marketing_report_detail`
	where date>='2024-10-01' and date<date(current_timestamp)
	group by
		date_trunc(date,WEEK(MONDAY))
		,case when platform='WEB' then 'WEB' else 'APP' end
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end


	union all
	select
		'Month' as segment
		,date_trunc(date,MONTH) as date
		,case when platform='WEB' then 'WEB' else 'APP' end as platform
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end as signup_country
		,'Not Completed' as status
		,sum(signup_users) as signup_users
		,sum(create_record_users) as create_record_users
		,sum(new_paid_users) as new_paid_users
		,round(sum(first_payment_rev),2) as new_revenue
		,round(sum(cumulative_rev),2) as total_revenue
		,round(sum(case when source in ('GoogleAds','BingAds','YahooAds','MetaAds','ASA', 'MolocoAds', 'AppierAds', 'TiktokAds', 'MintegralAds') then first_payment_rev else 0 end),2) as ads_first_payment
	from `dbt_models_details.marketing_report_detail`
	where date>='2024-10-01' and date<date(current_timestamp)
	group by
		date_trunc(date,MONTH)
		,case when platform='WEB' then 'WEB' else 'APP' end
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end


	union all
	select
		'Quarter' as segment
		,date_trunc(date,QUARTER) as date
		,case when platform='WEB' then 'WEB' else 'APP' end as platform
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end as signup_country
		,'Not Completed' as status
		,sum(signup_users) as signup_users
		,sum(create_record_users) as create_record_users
		,sum(new_paid_users) as new_paid_users
		,round(sum(first_payment_rev),2) as new_revenue
		,round(sum(cumulative_rev),2) as total_revenue
		,round(sum(case when source in ('GoogleAds','BingAds','YahooAds','MetaAds','ASA', 'MolocoAds', 'AppierAds', 'TiktokAds', 'MintegralAds') then first_payment_rev else 0 end),2) as ads_first_payment
	from `dbt_models_details.marketing_report_detail`
	where date>='2024-10-01' and date<date(current_timestamp)
	group by
		date_trunc(date,QUARTER)
		,case when platform='WEB' then 'WEB' else 'APP' end
		,case when signup_country in ('Japan','United States') then signup_country else 'Other' end
)


select
	a.segment
	,a.date
	,a.platform
	,a.signup_country
	,a.status
	,signup_users
	,create_record_users
	,new_paid_users
	,new_revenue
	,total_revenue
	,ads_first_payment
	,signup_user_target
	,new_revenue_target
	,ads_first_payment_target
from fact a
left join `notta-data-analytics.mc_data_statistics.2025_okr_target` b
on a.segment=b.segment and a.date=b.date and a.platform=b.platform and a.signup_country=b.signup_country






