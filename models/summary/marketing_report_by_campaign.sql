with campaign_country as (
select
	format_date('%Y-%m',date) as month
	,source
	,campaign
	,max_by(platform,signup_users) as max_paid_platform
	,max_by(signup_country,signup_users) as max_paid_country
from `notta-data-analytics.dbt_models_details.marketing_report_detail`
where source not in ('Other Organic','Referral','Unknown','Direct')
group by
	format_date('%Y-%m',date)
	,source
	,campaign
),

campaign_performance_agg as (
select
	date
	,a.source
	,a.campaign
	,b.max_paid_platform as max_paid_platform
	,b.max_paid_country as max_paid_country
	,sum(signup_users) as signup_users
	,sum(create_record_users) as create_record_users
	,sum(new_paid_users) as new_paid_users
	,round(sum(first_payment_rev),2) as first_payment_rev
	,round(sum(cumulative_rev),2) as cumulative_rev
from `notta-data-analytics.dbt_models_details.marketing_report_detail` a
inner join campaign_country b on format_date('%Y-%m',a.date)=b.month and a.source=b.source and a.campaign=b.campaign
where a.source not in ('Other Organic','Referral','Unknown','Direct')
group by
	a.date
	,a.source
	,a.campaign
	,b.max_paid_platform
	,b.max_paid_country
union all
select
	date
	,source
	,campaign
	,platform as max_paid_platform
	,signup_country as max_paid_country
	,sum(signup_users) as signup_users
	,sum(create_record_users) as create_record_users
	,sum(new_paid_users) as new_paid_users
	,round(sum(first_payment_rev),2) as first_payment_rev
	,round(sum(cumulative_rev),2) as cumulative_rev
from `notta-data-analytics.dbt_models_details.marketing_report_detail`
where source in ('Other Organic','Referral','Unknown','Direct')
group by
	date
	,source
	,campaign
	,platform
	,signup_country
),

campaign_cost as (
--ggads web
	select
	  a.segments_date as date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,'WEB' as platform
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,0 as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_2936848719` a
	left join
	(
		select
			campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) as segments_date
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_2936848719`
		group by
		    campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY))
	)
	b on a.campaign_id=b.campaign_id and a.segments_date=b.segments_date
	group by
	   a.segments_date
	  ,b.campaign_name
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end
union all
--ggads ios
	select
	  a.segments_date as date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,'IOS' as platform
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,sum(metrics_conversions) as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_1789624174` a
	left join
	(
		select
			campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) as segments_date
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_1789624174`
		group by
		    campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY))
	)
	b on a.campaign_id=b.campaign_id and a.segments_date=b.segments_date
	group by
	   a.segments_date
	  ,b.campaign_name
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end
union all
--ggads android
	select
	  a.segments_date as date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,'ANDROID' as platform
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,sum(metrics_conversions) as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_4257710495` a
	left join
	(
		select
			campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) as segments_date
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_4257710495`
		group by
		    campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY))
	)
	b on a.campaign_id=b.campaign_id and a.segments_date=b.segments_date
	group by
	   a.segments_date
	  ,b.campaign_name
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end
union all
--ggads other app
	select
	  a.segments_date as date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,case when UPPER(b.campaign_name) like '%IOS%'then 'IOS' when (UPPER(b.campaign_name) like '%ANDROID%' or UPPER(b.campaign_name) like '%_AND_%') then 'ANDROID' else 'WEB' end as platform
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,sum(metrics_conversions) as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_4969130739` a
	left join
	(
		select
			campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) as segments_date
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_4969130739`
		group by
		    campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY))
	)
	b on a.campaign_id=b.campaign_id and a.segments_date=b.segments_date
	group by
	   a.segments_date
	  ,b.campaign_name
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end
	  ,case when UPPER(b.campaign_name) like '%IOS%'then 'IOS' when (UPPER(b.campaign_name) like '%ANDROID%' or UPPER(b.campaign_name) like '%_AND_%') then 'ANDROID' else 'WEB' end
union all
--web retargeting
	select
	  a.segments_date as date
	  ,'GoogleAds' as source
	  ,b.campaign_name as campaign
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,'WEB' as platform
	  ,sum(metrics_cost_micros/1000000) as cost
	  ,sum(metrics_clicks) as clicks
	  ,sum(metrics_impressions) as impressions
	  ,0 as installs
	from `notta-data-analytics.notta_google_ads.p_ads_CampaignStats_2279574195` a
	left join
	(
		select
			campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) as segments_date
			,max_by(campaign_name,campaign_start_date) as campaign_name
		from `notta-data-analytics.notta_google_ads.p_ads_Campaign_2279574195`
		group by
		    campaign_id
			,date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY))
	)
	b on a.campaign_id=b.campaign_id and a.segments_date=b.segments_date
	group by
	   a.segments_date
	  ,b.campaign_name
	  ,case when upper(b.campaign_name) like '%_JP_%' or upper(b.campaign_name) like '%_JP' or upper(b.campaign_name) like '%-JP-%' or upper(b.campaign_name) like '%-JP' or upper(b.campaign_name) like 'JP_%' then 'Japan'
	        when upper(b.campaign_name) like '%_US_%' or upper(b.campaign_name) like '%_US' or upper(b.campaign_name) like '%-US-%' or upper(b.campaign_name) like '%-US' or upper(b.campaign_name) like 'US_%' then 'United States'
	        else null
	  end
union all
--ASA 分天分campaign花费
	select
	  date
	  ,'ASA' as source
	  ,campaign_name as campaign
	  ,case when upper(campaign_name) like '%_JP_%' or upper(campaign_name) like '%_JP' or upper(campaign_name) like '%-JP-%' or upper(campaign_name) like '%-JP' or upper(campaign_name) like 'JP_%' then 'Japan'
	        when upper(campaign_name) like '%_US_%' or upper(campaign_name) like '%_US' or upper(campaign_name) like '%-US-%' or upper(campaign_name) like '%-US' or upper(campaign_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,'IOS' as platform
	  ,sum(local_spend_amount) as cost
	  ,sum(taps) as clicks
	  ,sum(impressions) as impressions
	  ,sum(tap_installs) as installs
	from `notta-data-analytics.notta_apple_search_ads.apple_search_ads_campaign_report`
	group by
	   date
	  ,campaign_name
	  ,case when upper(campaign_name) like '%_JP_%' or upper(campaign_name) like '%_JP' or upper(campaign_name) like '%-JP-%' or upper(campaign_name) like '%-JP' or upper(campaign_name) like 'JP_%' then 'Japan'
	        when upper(campaign_name) like '%_US_%' or upper(campaign_name) like '%_US' or upper(campaign_name) like '%-US-%' or upper(campaign_name) like '%-US' or upper(campaign_name) like 'US_%' then 'United States'
	        else null
	  end
union all
--MolocoAds 分天分campaign分国家花费
	select
	  date
	  ,'MolocoAds' as source
	  ,campaign_title as campaign
	  ,case when upper(campaign_title) like '%_JP_%' or upper(campaign_title) like '%_JP' or upper(campaign_title) like '%-JP-%' or upper(campaign_title) like '%-JP' or upper(campaign_title) like '%_JPN_%' or upper(campaign_title) like 'JP_%' then 'Japan'
	        when upper(campaign_title) like '%_US_%' or upper(campaign_title) like '%_US' or upper(campaign_title) like '%-US-%' or upper(campaign_title) like '%-US' or upper(campaign_title) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,case when UPPER(campaign_title) like '%IOS%'then 'IOS' when (UPPER(campaign_title) like '%ANDROID%' or UPPER(campaign_title) like '%_AND_%') then 'ANDROID' else 'WEB' end as platform
	  ,sum(spend) as cost
	  ,sum(clicks) as clicks
	  ,sum(impressions) as impressions
	  ,sum(installs) as installs
	from `notta-data-analytics.notta_moloco_ads.campaign_report`
	group by
	   date
	  ,campaign_title
	  ,case when upper(campaign_title) like '%_JP_%' or upper(campaign_title) like '%_JP' or upper(campaign_title) like '%-JP-%' or upper(campaign_title) like '%-JP' or upper(campaign_title) like '%_JPN_%' or upper(campaign_title) like 'JP_%' then 'Japan'
	        when upper(campaign_title) like '%_US_%' or upper(campaign_title) like '%_US' or upper(campaign_title) like '%-US-%' or upper(campaign_title) like '%-US' or upper(campaign_title) like 'US_%' then 'United States'
	        else null
	  end
union all
--AppierAds 分天分campaign分国家花费
	select
	  date
	  ,'AppierAds' as source
	  ,campaign_c as campaign
	  ,case when upper(campaign_c) like '%_JP_%' or upper(campaign_c) like '%_JP' or upper(campaign_c) like '%-JP-%' or upper(campaign_c) like '%-JP' or upper(campaign_c) like '% JP %' or upper(campaign_c) like 'JP_%' then 'Japan'
	        when upper(campaign_c) like '%_US_%' or upper(campaign_c) like '%_US' or upper(campaign_c) like '%-US-%' or upper(campaign_c) like '%-US' or upper(campaign_c) like '% US %' or upper(campaign_c) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,'IOS' as platform
	  ,sum(total_cost) as cost
	  ,sum(clicks) as clicks
	  ,sum(impressions) as impressions
	  ,sum(installs) as installs
	from `notta-data-analytics.notta_appsflyer.campaign_report_by_geo_n_date`
	where media_source_pid='appier_int'
	group by
	   date
	  ,campaign_c
	  ,case when upper(campaign_c) like '%_JP_%' or upper(campaign_c) like '%_JP' or upper(campaign_c) like '%-JP-%' or upper(campaign_c) like '%-JP' or upper(campaign_c) like '% JP %' or upper(campaign_c) like 'JP_%' then 'Japan'
	        when upper(campaign_c) like '%_US_%' or upper(campaign_c) like '%_US' or upper(campaign_c) like '%-US-%' or upper(campaign_c) like '%-US' or upper(campaign_c) like '% US %' or upper(campaign_c) like 'US_%' then 'United States'
	        else null
	  end
union all
--MintegralAds 分天分campaign分国家花费
	select
	  date
	  ,'MintegralAds' as source
	  ,offer_name as campaign
	  ,case when upper(offer_name) like '%_JP_%' or upper(offer_name) like '%_JP' or upper(offer_name) like 'JP_%' or upper(offer_name) like '%-JP-%' or upper(offer_name) like '%-JP' or upper(offer_name) like '% JP %' or upper(offer_name) like 'JP_%' then 'Japan'
	        when upper(offer_name) like '%_US_%' or upper(offer_name) like '%_US' or upper(offer_name) like 'US_%' or upper(offer_name) like '%-US-%' or upper(offer_name) like '%-US' or upper(offer_name) like '% US %' or upper(offer_name) like 'US_%' then 'United States'
	        else null
	  end as country
	  ,case when offer_name='JPand0609' or upper(offer_name) like '%_AND_%' then 'ANDROID' else 'IOS' end as platform
	  ,sum(spend) as cost
	  ,sum(click) as clicks
	  ,sum(impression) as impressions
	  ,sum(conversion) as installs
	from `notta-data-analytics.notta_mintegral_ads.mintegral_ads_campaign_report`
	group by
	   date
	  ,offer_name
	  ,case when upper(offer_name) like '%_JP_%' or upper(offer_name) like '%_JP' or upper(offer_name) like 'JP_%' or upper(offer_name) like '%-JP-%' or upper(offer_name) like '%-JP' or upper(offer_name) like '% JP %' or upper(offer_name) like 'JP_%' then 'Japan'
	        when upper(offer_name) like '%_US_%' or upper(offer_name) like '%_US' or upper(offer_name) like 'US_%' or upper(offer_name) like '%-US-%' or upper(offer_name) like '%-US' or upper(offer_name) like '% US %' or upper(offer_name) like 'US_%' then 'United States'
	        else null
	  end
	  ,case when offer_name='JPand0609' or upper(offer_name) like '%_AND_%' then 'ANDROID' else 'IOS' end
),

bingads_campaign_cost as (
--BingAds 分天分campaign_name,campaign_id分国家花费
	select
        time_period as date
        ,'BingAds' as source
        ,campaign_name
        ,campaign_id
        ,case when upper(campaign_name) like '%_JP_%' or upper(campaign_name) like '%_JP' or upper(campaign_name) like '%-JP-%' or upper(campaign_name) like '%-JP' or upper(campaign_name) like 'JP_%' then 'Japan'
	        when upper(campaign_name) like '%_US_%' or upper(campaign_name) like '%_US' or upper(campaign_name) like '%-US-%' or upper(campaign_name) like '%-US' or upper(campaign_name) like 'US_%' then 'United States'
	        else null
	    end as country
        ,'WEB' as platform
        ,sum(spend) as cost
        ,sum(clicks) as clicks
        ,sum(impressions) as impressions
        ,0 as installs
	from `notta-data-analytics.notta_bing_ads.bingads_campaign_report` a
	group by
        time_period
        ,campaign_name
        ,campaign_id
        ,case when upper(campaign_name) like '%_JP_%' or upper(campaign_name) like '%_JP' or upper(campaign_name) like '%-JP-%' or upper(campaign_name) like '%-JP' or upper(campaign_name) like 'JP_%' then 'Japan'
	        when upper(campaign_name) like '%_US_%' or upper(campaign_name) like '%_US' or upper(campaign_name) like '%-US-%' or upper(campaign_name) like '%-US' or upper(campaign_name) like 'US_%' then 'United States'
	        else null
	    end
),

bingads_campaign_performance as (
select
	a.date
	,a.source
	,coalesce(b.campaign_name,c.campaign_name,a.campaign) as campaign
	,'WEB' as max_paid_platform
	,max_by(max_paid_country,signup_users) as max_paid_country
	,sum(signup_users) as signup_users
	,sum(create_record_users) as create_record_users
	,sum(new_paid_users) as new_paid_users
	,sum(first_payment_rev) as first_payment_rev
	,sum(cumulative_rev) as cumulative_rev
from campaign_performance_agg a
left join bingads_campaign_cost b on a.campaign=b.campaign_id and a.date=b.date
left join (
select
    campaign_id
    ,max_by(campaign_name,date) as campaign_name
from bingads_campaign_cost
group by
    campaign_id
) c on a.campaign=c.campaign_id
where a.source='BingAds'
group by
	a.date
	,a.source
	,coalesce(b.campaign_name,c.campaign_name,a.campaign)
)


select
	COALESCE(a.date,b.date) as date
	,DATE_TRUNC(COALESCE(a.date,b.date),WEEK(FRIDAY)) as weekdate_friday
	,COALESCE(a.source,b.source) as source
	,COALESCE(a.campaign,b.campaign,'unknown') as campaign
	,COALESCE(b.platform,a.max_paid_platform,'unknown') as max_paid_platform
	,case when COALESCE(b.country,a.max_paid_country,'unknown') in ('Japan','United States') then COALESCE(b.country,a.max_paid_country,'unknown')
	 else 'Other'
	 end as max_paid_country
	,COALESCE(signup_users,0) as signup_users
	,COALESCE(create_record_users,0) as create_record_users
	,COALESCE(new_paid_users,0) as new_paid_users
	,COALESCE(first_payment_rev,0) as first_payment_rev
	,COALESCE(cumulative_rev,0) as cumulative_rev
	,round(COALESCE(cost,0),2) as cost
	,COALESCE(clicks,0) as clicks
	,COALESCE(impressions,0) as impressions
	,COALESCE(installs,0) as installs
from campaign_performance_agg a
full join campaign_cost b on a.date=b.date and a.source=b.source and a.campaign=b.campaign
where
    a.source is null or a.source!='BingAds'
union all
select
	COALESCE(a.date,b.date) as date
	,DATE_TRUNC(COALESCE(a.date,b.date),WEEK(FRIDAY)) as weekdate_friday
	,COALESCE(a.source,b.source) as source
	,COALESCE(b.campaign_name,a.campaign,'unknown') as campaign
	,COALESCE(b.platform,a.max_paid_platform,'unknown') as max_paid_platform
	,case when COALESCE(b.country,a.max_paid_country,'unknown') in ('Japan','United States') then COALESCE(b.country,a.max_paid_country,'unknown')
	 else 'Other'
	 end as max_paid_country
	,COALESCE(signup_users,0) as signup_users
	,COALESCE(create_record_users,0) as create_record_users
	,COALESCE(new_paid_users,0) as new_paid_users
	,COALESCE(first_payment_rev,0) as first_payment_rev
	,COALESCE(cumulative_rev,0) as cumulative_rev
	,round(COALESCE(cost,0),2) as cost
	,COALESCE(clicks,0) as clicks
	,COALESCE(impressions,0) as impressions
	,0 as installs
from bingads_campaign_performance a
full join bingads_campaign_cost b on a.date=b.date and a.source=b.source and a.campaign=b.campaign_name













