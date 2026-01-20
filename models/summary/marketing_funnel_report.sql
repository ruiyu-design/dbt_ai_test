
with order_table as
(
select
	a.workspace_id
	,a.uid
	,a.order_sn
	,a.origin_order_sn
	,a.create_time
    ,timestamp_seconds(a.create_time) as payment_date
    ,a.pay_currency
    ,b.plan_type
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
    ,a.pay_amount
    ,b.goods_id
    ,b.tag
    ,ifnull(c.rate,1) as exchange_rate
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
    ,lag(case when b.type=2 then 'addon'
    	when b.plan_type=0 then 'free'
    	when b.plan_type=1 then 'pro'
    	when b.plan_type=2 then 'biz'
    	when b.plan_type=3 then 'enterprise'
    	else 'unknown' end ) over (partition by case when b.type=2 then 'addon' else workspace_id end order by a.create_time) as preceding_plan_type
    ,lag(b.period_count) over (partition by case when b.type=2 then 'addon' else workspace_id end order by a.create_time) as preceding_period_count
    ,lag(case when b.period_unit=1 then 'annually'
    	when b.period_unit=2 then 'monthly'
    	when b.period_unit=3 then 'days'
    	when b.period_unit=4 then 'hours'
    	when b.period_unit=5 then 'mins'
    	else cast(period_unit as string) end) over (partition by case when b.type=2 then 'addon' else workspace_id end order by a.create_time) as preceding_period_unit
    ,row_number() over(partition by a.uid order by a.create_time) as rn
from `notta_aurora.payment_center_order_table` a
inner join `notta_aurora.notta_mall_interest_goods` b on b.goods_id = a.goods_id
left join `dbt_models_summary.exchange_rates` c on c.currency=a.pay_currency
where
	a.status in (4,8,13) --支付/升级成功
	and a.uid is not null
	and a.order_sn is not null
	and a.pay_amount>0 --筛选有付费金额
	and a.pay_channel in (5,6,7,8)
),

user_payment as (
select
	a.uid
	,signup_date
	,signup_platform
	,signup_country
	,source
	,medium
	,campaign
	,first_user_landing_page
	,signup_time
	,is_create_record
	,is_trial
	,first_trial_start_time
	,min(b.payment_date) as first_payment_time
	,sum(case when datetime_diff(b.payment_date,a.signup_time,HOUR)<=24 then b.pay_amount/b.exchange_rate/100 else 0 end) as `24hours_revenue`
	,sum(case when datetime_diff(b.payment_date,a.signup_time,HOUR)<=72 then b.pay_amount/b.exchange_rate/100 else 0 end) as `72hours_revenue`
	,sum(case when datetime_diff(b.payment_date,a.signup_time,HOUR)<=96 then b.pay_amount/b.exchange_rate/100 else 0 end) as `96hours_revenue`
	,sum(case when datetime_diff(b.payment_date,a.signup_time,DAY)<=7 then b.pay_amount/b.exchange_rate/100 else 0 end) as `7days_revenue`
	,sum(case when datetime_diff(b.payment_date,a.signup_time,DAY)<=30 then b.pay_amount/b.exchange_rate/100 else 0 end) as `30days_revenue`
	,sum(b.pay_amount/b.exchange_rate/100) as cumulative_revenue
from `dbt_models_details.user_details` a
left join order_table b on a.uid=b.uid
where
	a.pt=date_add(date(current_timestamp()),interval -1 day)
group by
	a.uid
	,signup_date
	,signup_platform
	,signup_country
	,source
	,medium
	,campaign
	,first_user_landing_page
	,signup_time
	,is_create_record
	,is_trial
	,first_trial_start_time
),

campaign_revenue as (
select
	signup_date
	,case when (source in ('google') and medium='cpc') or source in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
		when source in ('bing','BingAds') and medium='cpc' then 'BingAds'
		when source in ('yahoo','yahoo2','YahooAds') and medium='cpc' then 'YahooAds'
		when (source in ('MetaAds','facebook') and medium='cpc') or source='Facebook Ads' then 'MetaAds'
		when (source='Apple' and medium='search') or source='Apple Search Ads' then 'ASA'
		when source='moloco_int' then 'MolocoAds'
		when source='appier_int' then 'AppierAds'
		when source='mintegral_int' then 'MintegralAds'
		when source='tiktokglobal_int' then 'TiktokAds'
		when source='impact' then 'Affiliate'
		when source like '%_int' then 'Affiliate'
		when campaign like 'notta_kol_' and medium='youtube' then 'KOL'
		when source in ('kol','KOL','KOC','TT') then 'KOL'
		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end as source
	,case when campaign in ('None','unknown') then null else campaign end as campaign
	,signup_platform
	,signup_country
	,first_user_landing_page
	,count(distinct uid) as signup_users
	,count(distinct case when is_create_record=1 then uid else null end) as create_record_users
	,count(distinct case when first_trial_start_time is not null and datetime_diff(first_trial_start_time,signup_time,HOUR)<=24 then uid else null end) as `24hours_trial_users`
	,count(distinct case when first_payment_time is not null and datetime_diff(first_payment_time,signup_time,HOUR)<=24 then uid else null end) as `24hours_paid_users`
	,count(distinct case when first_payment_time is not null and datetime_diff(first_payment_time,signup_time,HOUR)<=72 then uid else null end) as `72hours_paid_users`
	,count(distinct case when first_payment_time is not null and datetime_diff(first_payment_time,signup_time,HOUR)<=96 then uid else null end) as `96hours_paid_users`
	,count(distinct case when first_payment_time is not null and datetime_diff(first_payment_time,signup_time,DAY)<=7 then uid else null end) as `7days_paid_users`
	,count(distinct case when first_payment_time is not null and datetime_diff(first_payment_time,signup_time,DAY)<=30 then uid else null end) as `30days_paid_users`
	,count(distinct case when first_payment_time is not null then uid else null end) as cumulative_paid_users
	,sum(`24hours_revenue`) as `24hours_revenue`
	,sum(`72hours_revenue`) as `72hours_revenue`
	,sum(`96hours_revenue`) as `96hours_revenue`
	,sum(`7days_revenue`) as `7days_revenue`
	,sum(`30days_revenue`) as `30days_revenue`
	,sum(cumulative_revenue) as cumulative_revenue
from user_payment a
group by
	signup_date
	,case when (source in ('google') and medium='cpc') or source in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
		when source in ('bing','BingAds') and medium='cpc' then 'BingAds'
		when source in ('yahoo','yahoo2','YahooAds') and medium='cpc' then 'YahooAds'
		when (source in ('MetaAds','facebook') and medium='cpc') or source='Facebook Ads' then 'MetaAds'
		when (source='Apple' and medium='search') or source='Apple Search Ads' then 'ASA'
		when source='moloco_int' then 'MolocoAds'
		when source='appier_int' then 'AppierAds'
		when source='mintegral_int' then 'MintegralAds'
		when source='tiktokglobal_int' then 'TiktokAds'
		when source='impact' then 'Affiliate'
		when source like '%_int' then 'Affiliate'
		when campaign like 'notta_kol_' and medium='youtube' then 'KOL'
		when source in ('kol','KOL','KOC','TT') then 'KOL'
		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end
	,case when campaign in ('None','unknown') then null else campaign end
	,signup_platform
	,signup_country
	,first_user_landing_page
),

campaign_revenue_agg as (
select
	signup_date
	,source
	,campaign
	,max_by(signup_platform,signup_users) as max_user_signup_platform
	,max_by(signup_country,signup_users) as max_user_signup_country
	,sum(signup_users) as signup_users
	,sum(create_record_users) as create_record_users
	,sum(`24hours_trial_users`) as `24hours_trial_users`
	,sum(`24hours_paid_users`) as `24hours_paid_users`
	,sum(`72hours_paid_users`) as `72hours_paid_users`
	,sum(`96hours_paid_users`) as `96hours_paid_users`
	,sum(`7days_paid_users`) as `7days_paid_users`
	,sum(`30days_paid_users`) as `30days_paid_users`
	,sum(cumulative_paid_users) as cumulative_paid_users
	,round(sum(`24hours_revenue`),2) as `24hours_revenue`
	,round(sum(`72hours_revenue`),2) as `72hours_revenue`
	,round(sum(`96hours_revenue`),2) as `96hours_revenue`
	,round(sum(`7days_revenue`),2) as `7days_revenue`
	,round(sum(`30days_revenue`),2) as `30days_revenue`
	,round(sum(cumulative_revenue),2) as cumulative_revenue
from campaign_revenue
group by
	signup_date
	,source
	,campaign
),

campaign_cost as (
--ggads web
	select
	  a.segments_date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,'WEB' as platform
	  ,case when UPPER(b.campaign_name) like '%_JP_%' or UPPER(b.campaign_name) like '% JP %' or UPPER(b.campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(b.campaign_name) like '%_US_%' or UPPER(b.campaign_name) like '% US %' or UPPER(b.campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,0 as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_2936848719` a
	left join
	(
		select
			campaign_id
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_2936848719`
		group by campaign_id
	)
	b on a.campaign_id=b.campaign_id
	group by
	   a.segments_date
	  ,b.campaign_name
union all
--ggads ios
	select
	  a.segments_date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,'IOS' as platform
	  ,case when UPPER(b.campaign_name) like '%_JP_%' or UPPER(b.campaign_name) like '% JP %' or UPPER(b.campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(b.campaign_name) like '%_US_%' or UPPER(b.campaign_name) like '% US %' or UPPER(b.campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,sum(metrics_conversions) as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_1789624174` a
	left join
	(
		select
			campaign_id
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_1789624174`
		group by campaign_id
	)
	b on a.campaign_id=b.campaign_id
	group by
	   a.segments_date
	  ,b.campaign_name
union all
--ggads android
	select
	  a.segments_date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,'ANDROID' as platform
	  ,case when UPPER(b.campaign_name) like '%_JP_%' or UPPER(b.campaign_name) like '% JP %' or UPPER(b.campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(b.campaign_name) like '%_US_%' or UPPER(b.campaign_name) like '% US %' or UPPER(b.campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,sum(metrics_conversions) as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_4257710495` a
	left join
	(
		select
			campaign_id
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_4257710495`
		group by campaign_id
	)
	b on a.campaign_id=b.campaign_id
	group by
	   a.segments_date
	  ,b.campaign_name
union all
--ggads other app
	select
	  a.segments_date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,case when UPPER(b.campaign_name) like '%IOS%'then 'IOS' when (UPPER(b.campaign_name) like '%ANDROID%' or UPPER(b.campaign_name) like '%_AND_%') then 'ANDROID' else 'WEB' end as platform
	  ,case when UPPER(b.campaign_name) like '%_JP_%' or UPPER(b.campaign_name) like '% JP %' or UPPER(b.campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(b.campaign_name) like '%_US_%' or UPPER(b.campaign_name) like '% US %' or UPPER(b.campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,sum(metrics_conversions) as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_4969130739` a
	left join
	(
		select
			campaign_id
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_4969130739`
		group by campaign_id
	)
	b on a.campaign_id=b.campaign_id
	group by
	   a.segments_date
	  ,b.campaign_name
	  ,case when UPPER(b.campaign_name) like '%IOS%'then 'IOS' when (UPPER(b.campaign_name) like '%ANDROID%' or UPPER(b.campaign_name) like '%_AND_%') then 'ANDROID' else 'WEB' end
union all
--web retargeting
	select
	  a.segments_date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,'WEB' as platform
	  ,case when UPPER(b.campaign_name) like '%_JP_%' or UPPER(b.campaign_name) like '% JP %' or UPPER(b.campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(b.campaign_name) like '%_US_%' or UPPER(b.campaign_name) like '% US %' or UPPER(b.campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,0 as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_2279574195` a
	left join
	(
		select
			campaign_id
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_2279574195`
		group by campaign_id
	)
	b on a.campaign_id=b.campaign_id
	group by
	   a.segments_date
	  ,b.campaign_name
union all
--ASA 分天分campaign花费
	select
	  date
	  ,'ASA' as source
	  ,campaign_name as campaign
	  ,'IOS' as platform
	  ,case when UPPER(campaign_name) like '%_JP_%' or UPPER(campaign_name) like '% JP %' or UPPER(campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(campaign_name) like '%_US_%' or UPPER(campaign_name) like '% US %' or UPPER(campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(local_spend_amount) as cost
	  ,sum(taps) as clicks
	  ,sum(impressions) as impressions
	  ,sum(tap_installs) as installs
	from `notta-data-analytics.notta_apple_search_ads.apple_search_ads_campaign_report`
	group by
	   date
	  ,campaign_name
union all
--MolocoAds 分天分campaign分国家花费
	select
	  date
	  ,'MolocoAds' as source
	  ,campaign_title as campaign
	  ,case when UPPER(campaign_title) like '%IOS%'then 'IOS' when (UPPER(campaign_title) like '%ANDROID%' or UPPER(campaign_title) like '%_AND_%') then 'ANDROID' else 'WEB' end as platform
	  ,case when UPPER(campaign_title) like '%_JP_%' or UPPER(campaign_title) like '% JP %' or UPPER(campaign_title) like '%-JP-%' then 'Japan'
	  when UPPER(campaign_title) like '%_US_%' or UPPER(campaign_title) like '% US %' or UPPER(campaign_title) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(spend) as cost
	  ,sum(clicks) as clicks
	  ,sum(impressions) as impressions
	  ,sum(installs) as installs
	from `notta-data-analytics.notta_moloco_ads.campaign_report`
	group by
	   date
	  ,campaign_title
union all
--AppierAds 分天分campaign分国家花费
	select
	  date
	  ,'AppierAds' as source
	  ,campaign_c as campaign
	  ,'IOS' as platform
	  ,case when UPPER(campaign_c) like '%_JP_%' or UPPER(campaign_c) like '% JP %' or UPPER(campaign_c) like '%-JP-%' then 'Japan'
	  when UPPER(campaign_c) like '%_US_%' or UPPER(campaign_c) like '% US %' or UPPER(campaign_c) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(total_cost) as cost
	  ,sum(clicks) as clicks
	  ,sum(impressions) as impressions
	  ,sum(installs) as installs
	from `notta-data-analytics.notta_appsflyer.campaign_report_by_geo_n_date`
	where media_source_pid='appier_int'
	group by
	   date
	  ,campaign_c
union all
--BingAds 分天分campaign分国家花费
	select
	  time_period as date
	  ,'BingAds' as source
	  ,campaign_name as campaign
	  ,'WEB' as platform
	  ,case when UPPER(campaign_name) like '%_JP_%' or UPPER(campaign_name) like '% JP %' or UPPER(campaign_name) like '%-JP-%' then 'Japan'
	  when UPPER(campaign_name) like '%_US_%' or UPPER(campaign_name) like '% US %' or UPPER(campaign_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(spend) as cost
	  ,sum(clicks) as clicks
	  ,sum(impressions) as impressions
	  ,0 as installs
	from `notta-data-analytics.notta_bing_ads.bingads_campaign_report`
	group by
	   time_period
	  ,campaign_name
union all
--MintegralAds 分天分campaign分国家花费
	select
	  date
	  ,'MintegralAds' as source
	  ,offer_name as campaign
	  ,case when offer_name='JPand0609' or upper(offer_name) like '%_AND_%' then 'ANDROID' else 'IOS' end as platform
	  ,case when UPPER(offer_name) like '%_JP_%' or UPPER(offer_name) like '% JP %' or UPPER(offer_name) like '%-JP-%' then 'Japan'
	  when UPPER(offer_name) like '%_US_%' or UPPER(offer_name) like '% US %' or UPPER(offer_name) like '%-US-%' then 'United States'
	  else null end as country
	  ,sum(spend) as cost
	  ,sum(click) as clicks
	  ,sum(impression) as impressions
	  ,sum(conversion) as installs
	from `notta-data-analytics.notta_mintegral_ads.mintegral_ads_campaign_report`
	group by
	   date
	  ,offer_name
	  ,case when offer_name='JPand0609' or upper(offer_name) like '%_AND_%' then 'ANDROID' else 'IOS' end
)


select
	COALESCE(a.signup_date,b.segments_date) as date
	,COALESCE(a.source,b.source) as source
	,COALESCE(a.campaign,b.campaign,'unknown') as campaign
	,COALESCE(b.platform,a.max_user_signup_platform,'unknown') as platform
	,COALESCE(a.max_user_signup_country,b.country, 'unknown') as country
	,COALESCE(signup_users,0) as signup_users
	,COALESCE(create_record_users,0) as create_record_users
	,COALESCE(`24hours_trial_users`,0) as `24hours_trial_users`
	,COALESCE(`24hours_paid_users`,0) as `24hours_paid_users`
	,COALESCE(`72hours_paid_users`,0) as `72hours_paid_users`
	,COALESCE(`96hours_paid_users`,0) as `96hours_paid_users`
	,COALESCE(`7days_paid_users`,0) as `7days_paid_users`
	,COALESCE(`30days_paid_users`,0) as `30days_paid_users`
	,COALESCE(cumulative_paid_users,0) as cumulative_paid_users
	,COALESCE(`24hours_revenue`,0) as `24hours_revenue`
	,COALESCE(`72hours_revenue`,0) as `72hours_revenue`
	,COALESCE(`96hours_revenue`,0) as `96hours_revenue`
	,COALESCE(`7days_revenue`,0) as `7days_revenue`
	,COALESCE(`30days_revenue`,0) as `30days_revenue`
	,COALESCE(cumulative_revenue,0) as cumulative_revenue
	,round(COALESCE(cost,0),2) as cost
	,COALESCE(clicks,0) as clicks
	,COALESCE(impressions,0) as impressions
	,COALESCE(installs,0) as installs
from campaign_revenue_agg a
full join campaign_cost b on a.signup_date=b.segments_date and a.source=b.source and a.campaign=b.campaign













