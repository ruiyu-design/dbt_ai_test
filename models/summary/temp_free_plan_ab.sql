-- free 用户转写三分钟和十五分钟 AB实验观测数据指标

-- **统计时间：**2025年7月17号开始-8月12日

-- **用户范围和分组：**注册时间为2025年7月17日后，且非广告渠道用户，且成功创建了WS的新用户，基于以上筛选用户后，拆分对照组和实验组；对照组：发放了 原有 free plan,  商品 ID为 11100；实验组：发放了新的 free plan，商品ID为 11102；

-- **筛选国家：**Japan,United States,United Kingdom,Canada,German,Brazil,Mexico,Netherlands，Belgium,Singapore,Poland,France,Italy,Australia,Spain,Switzerland,Colombia,Iran,Vietnam，Argentina,Thailand,Turkey,Ukraine,Malaysia,Pakistan,Russia,Bolivia,Morocco,Portugal,Israel, Korea, South,Austria,Ireland,Iceland,Denmark,Finland,Norway,Sweden,Hungary

-- **观察指标：24小时**付费率，96小时付费率，7天付费率，14天付费率，

-- User journey ，
-- 24小时转写留存率，3天转写留存率，7天转写留存率，14天转写留存率。

-- **维度：**国家，洲，


with uid as (-- 目标注册且创建ws的用户,筛选web端注册，目标注册国家，非广告用户
select
	uid
	,signup_time
	,is_paid
	,first_paid_plan_start_time
	,case when first_paid_plan_start_time is null then current_timestamp() else first_paid_plan_start_time end as paid_compare_time
	,first_paid_plan_type
	,signup_platform
	,device_category
	,signup_country
	,source
	,signup_date
	,is_trial
	,is_create_record
FROM `notta-data-analytics.dbt_models_details.user_details`
where pt=date_add(date(current_timestamp()),interval -1 day)
and is_create_ws=1 -- 选择创建ws的用户
and signup_date>='2025-07-17'
and signup_date<='2025-09-12'
and medium!='cpc'
and signup_platform='WEB'
and signup_country in ('Japan','United States','United Kingdom','Canada','German','Brazil','Mexico','Netherlands','Belgium','Singapore','Poland','France','Italy','Australia','Spain','Switzerland','Colombia','Iran','Vietnam','Argentina','Thailand','Turkey','Ukraine','Malaysia','Pakistan','Russia','Bolivia','Morocco','Portugal','Israel','South Korea','Austria','Ireland','Iceland','Denmark','Finland','Norway','Sweden','Hungary')
and source not in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825','BingAds','YahooAds','MetaAds')
),

mall_veriation as (

select distinct uid from `notta-data-analytics.notta_aurora.notta_mall_interest_ab_test_user_type`

),



record as (

	select
		found_uid
		,min(create_date) as min_create_record_date
		,min(case when audio_duration>=900 then create_date else null end) as min_15min_create_record_date
	from `dbt_models_details.stg_aurora_record` a
	group by
		found_uid
),


user_record_retention as (

	select
		a.found_uid
		,b.min_create_record_date
		,b.min_15min_create_record_date
		,count(distinct case when datetime_diff(create_date,min_create_record_date,DAY)=1 then 1 else null end) as record_in_1day
		,count(distinct case when datetime_diff(create_date,min_create_record_date,DAY)>=7 and datetime_diff(create_date,min_create_record_date,DAY)<14 then 1 else null end) as record_in_1week
		,count(distinct case when datetime_diff(create_date,min_create_record_date,DAY)>=14 and datetime_diff(create_date,min_create_record_date,DAY)<21 then 1 else null end) as record_in_2week
	from `dbt_models_details.stg_aurora_record` a
	inner join record b on a.found_uid=b.found_uid
	group by
		a.found_uid
		,b.min_create_record_date
		,b.min_15min_create_record_date


)

select
  signup_date
  ,signup_platform
  ,device_category
  ,case when signup_country in ('Japan','United States') then signup_country else 'Others' end as signup_country
  -- ,case when (source in ('google','GoogleAds') and medium='cpc') or source in ('googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
-- 		when source in ('bing','BingAds') and medium='cpc' then 'BingAds'
-- 		when source in ('yahoo','yahoo2','YahooAds') and medium='cpc' then 'YahooAds'
-- 		when (source in ('MetaAds','facebook') and medium='cpc') or source='Facebook Ads' then 'MetaAds'
-- 		when (source='Apple' and medium='search') or source='Apple Search Ads' then 'ASA'
-- 		when source='moloco_int' then 'MolocoAds'
-- 		when source='appier_int' then 'AppierAds'
-- 		when source='mintegral_int' then 'MintegralAds'
-- 		when source='tiktokglobal_int' then 'TiktokAds'
-- 		when source='impact' then 'Affiliate'
-- 		when source like '%_int' then 'Affiliate'
-- 		when campaign like '%notta_kol_%' and medium='youtube' then 'KOL'
-- 		when source in ('%kol%','%KOL%','%KOC%','TT') then 'KOL'
-- 		when medium='referral' then 'Referral'
-- 		else 'Other Organic'
-- 		end as source
  ,case when b.uid is null then '3min' else '15min' end as test_plan
  ,count(1) as user_count
  ,count(distinct a.uid) as user_check
  ,count(distinct case when is_trial=1 then a.uid else null end) as trial_users
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=24 then a.uid else null end) as paid_users_in24hours
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=96 then a.uid else null end) as paid_users_in96hours
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=168 then a.uid else null end) as paid_users_in7days
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=336 then a.uid else null end) as paid_users_in14days
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=720 then a.uid else null end) as paid_users_in30days
  ,count(distinct case when is_paid=1 then a.uid else null end) as paid_users
  ,count(distinct case when c.min_create_record_date is not null then a.uid else null end) as create_record_users
  ,count(distinct case when c.min_15min_create_record_date is not null then a.uid else null end) as create_15min_record_users
  ,count(distinct case when record_in_1day>0 then a.uid else null end) as create_record_in_1day_users
  ,count(distinct case when record_in_1week>0 then a.uid else null end) as create_record_in_1week_users
  ,count(distinct case when record_in_2week>0 then a.uid else null end) as create_record_in_2week_users
from uid a
left join mall_veriation b on cast(a.uid as string)=b.uid
left join user_record_retention c on a.uid=c.found_uid
group by
  signup_date
  ,signup_platform
  ,device_category
  ,case when signup_country in ('Japan','United States') then signup_country else 'Others' end
  -- ,case when (source in ('google','GoogleAds') and medium='cpc') or source in ('googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
-- 		when source in ('bing','BingAds') and medium='cpc' then 'BingAds'
-- 		when source in ('yahoo','yahoo2','YahooAds') and medium='cpc' then 'YahooAds'
-- 		when (source in ('MetaAds','facebook') and medium='cpc') or source='Facebook Ads' then 'MetaAds'
-- 		when (source='Apple' and medium='search') or source='Apple Search Ads' then 'ASA'
-- 		when source='moloco_int' then 'MolocoAds'
-- 		when source='appier_int' then 'AppierAds'
-- 		when source='mintegral_int' then 'MintegralAds'
-- 		when source='tiktokglobal_int' then 'TiktokAds'
-- 		when source='impact' then 'Affiliate'
-- 		when source like '%_int' then 'Affiliate'
-- 		when campaign like '%notta_kol_%' and medium='youtube' then 'KOL'
-- 		when source in ('%kol%','%KOL%','%KOC%','TT') then 'KOL'
-- 		when medium='referral' then 'Referral'
-- 		else 'Other Organic'
-- 		end as source
  ,case when b.uid is null then '3min' else '15min' end






