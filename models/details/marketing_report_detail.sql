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
    ,case when a.pay_channel in (5,8) then 'WEB'
	    when a.pay_channel=6 then 'IOS'
	    when a.pay_channel=7 then 'ANDROID'
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
	and is_trial=0
	and a.pay_channel in (5,6,7,8)
),

campaign_revenue as (
select
	date(b.payment_date) as payment_date
	,pay_channel
	,case when signup_country in ('Japan','United States','unknown','Canada','Australia','Brazil','Mexico',
  'United Kingdom',
  'France',
  'Germany',
  'Singapore',
  'Switzerland',
  'Hong Kong',
  'Netherlands',
  'Malaysia',
  'South Africa',
  'United Arab Emirates',
  'Poland',
  'Spain',
  'Colombia',
  'Thailand',
  'Italy',
  'Indonesia',
  'Philippines',
  'South Korea'
  ) then signup_country
  when signup_country in (
  'Sweden',
  'Portugal',
  'Belgium',
  'Romania',
  'Finland',
  'Russia',
  'Serbia',
  'Luxembourg',
  'Malta',
  'Ukraine',
  'Norway',
  'Latvia',
  'Austria',
  'Greece',
  'Slovenia',
  'Czechia',
  'Bulgaria',
  'Hungary',
  'Moldova',
  'Ireland',
  'Bosnia & Herzegovina',
  'Albania',
  'Croatia',
  'Denmark',
  'Lithuania',
  'Slovakia',
  'Kosovo',
  'Montenegro',
  'North Macedonia',
  'Liechtenstein',
  'Belarus',
  'Iceland',
  'Estonia',
  'Andorra',
  'Gibraltar',
  'Guernsey',
  'Isle of Man',
  'Jersey',
  'San Marino',
  'Monaco',
  'Svalbard & Jan Mayen') then 'Other Europe'
   else 'Other' end as signup_country
	,case when source in ('unknown','None') then 'Unknown'
	    when source='(direct)' then 'Direct'
	    when (source in ('google') and medium='cpc') or source in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
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
		when campaign like '%notta_kol_%' and medium='youtube' then 'KOL'
		when source in ('%kol%','%KOL%','%KOC%','TT') then 'KOL'
--		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end as source
	,case when campaign in ('None','unknown') then null else campaign end as campaign
	,first_user_landing_page
	,count(distinct case when b.rn=1 then a.uid else null end) as new_paid_users
    ,round(sum(case when b.rn=1 then b.pay_amount/b.exchange_rate/100 else 0 end),2) as first_payment_rev 
	,round(sum(b.pay_amount/b.exchange_rate/100),2) as cumulative_rev 
from `dbt_models_details.user_details` a
inner join order_table b on a.uid=b.uid
where
	a.pt=date_add(date(current_timestamp()),interval -1 day)
group by
	date(b.payment_date)
	,pay_channel
    ,case when signup_country in ('Japan','United States','unknown','Canada','Australia','Brazil','Mexico',
  'United Kingdom',
  'France',
  'Germany',
  'Singapore',
  'Switzerland',
  'Hong Kong',
  'Netherlands',
  'Malaysia',
  'South Africa',
  'United Arab Emirates',
  'Poland',
  'Spain',
  'Colombia',
  'Thailand',
  'Italy',
  'Indonesia',
  'Philippines',
  'South Korea'
  ) then signup_country
  when signup_country in (
  'Sweden',
  'Portugal',
  'Belgium',
  'Romania',
  'Finland',
  'Russia',
  'Serbia',
  'Luxembourg',
  'Malta',
  'Ukraine',
  'Norway',
  'Latvia',
  'Austria',
  'Greece',
  'Slovenia',
  'Czechia',
  'Bulgaria',
  'Hungary',
  'Moldova',
  'Ireland',
  'Bosnia & Herzegovina',
  'Albania',
  'Croatia',
  'Denmark',
  'Lithuania',
  'Slovakia',
  'Kosovo',
  'Montenegro',
  'North Macedonia',
  'Liechtenstein',
  'Belarus',
  'Iceland',
  'Estonia',
  'Andorra',
  'Gibraltar',
  'Guernsey',
  'Isle of Man',
  'Jersey',
  'San Marino',
  'Monaco',
  'Svalbard & Jan Mayen') then 'Other Europe'
   else 'Other' end
	,case when source in ('unknown','None') then 'Unknown'
	    when source='(direct)' then 'Direct'
	    when (source in ('google') and medium='cpc') or source in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
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
		when campaign like '%notta_kol_%' and medium='youtube' then 'KOL'
		when source in ('%kol%','%KOL%','%KOC%','TT') then 'KOL'
--		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end
	,case when campaign in ('None','unknown') then null else campaign end
	,first_user_landing_page
),

campaign_signup as (
select
	signup_date
	,signup_platform
	,case when signup_country in ('Japan','United States','unknown','Canada','Australia','Brazil','Mexico',
  'United Kingdom',
  'France',
  'Germany',
  'Singapore',
  'Switzerland',
  'Hong Kong',
  'Netherlands',
  'Malaysia',
  'South Africa',
  'United Arab Emirates',
  'Poland',
  'Spain',
  'Colombia',
  'Thailand',
  'Italy',
  'Indonesia',
  'Philippines',
  'South Korea'
  ) then signup_country
  when signup_country in (
  'Sweden',
  'Portugal',
  'Belgium',
  'Romania',
  'Finland',
  'Russia',
  'Serbia',
  'Luxembourg',
  'Malta',
  'Ukraine',
  'Norway',
  'Latvia',
  'Austria',
  'Greece',
  'Slovenia',
  'Czechia',
  'Bulgaria',
  'Hungary',
  'Moldova',
  'Ireland',
  'Bosnia & Herzegovina',
  'Albania',
  'Croatia',
  'Denmark',
  'Lithuania',
  'Slovakia',
  'Kosovo',
  'Montenegro',
  'North Macedonia',
  'Liechtenstein',
  'Belarus',
  'Iceland',
  'Estonia',
  'Andorra',
  'Gibraltar',
  'Guernsey',
  'Isle of Man',
  'Jersey',
  'San Marino',
  'Monaco',
  'Svalbard & Jan Mayen') then 'Other Europe'
   else 'Other' end as signup_country
	,case when source in ('unknown','None') then 'Unknown'
	    when source='(direct)' then 'Direct'
	    when (source in ('google') and medium='cpc') or source in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
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
		when campaign like '%notta_kol_%' and medium='youtube' then 'KOL'
		when source in ('%kol%','%KOL%','%KOC%','TT') then 'KOL'
--		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end as source
	,case when campaign in ('None','unknown') then null else campaign end as campaign
	,first_user_landing_page
	,count(distinct uid) as signup_users
	,count(distinct case when is_create_record=1 then uid else null end) as create_record_users -- [TEST v3] 激活用户：注册当天完成核心动作（创建记录）的用户
from `dbt_models_details.user_details` a
where
	a.pt=date_add(date(current_timestamp()),interval -1 day)
group by
	signup_date
	,signup_platform
	,case when signup_country in ('Japan','United States','unknown','Canada','Australia','Brazil','Mexico',
  'United Kingdom',
  'France',
  'Germany',
  'Singapore',
  'Switzerland',
  'Hong Kong',
  'Netherlands',
  'Malaysia',
  'South Africa',
  'United Arab Emirates',
  'Poland',
  'Spain',
  'Colombia',
  'Thailand',
  'Italy',
  'Indonesia',
  'Philippines',
  'South Korea'
  ) then signup_country
  when signup_country in (
  'Sweden',
  'Portugal',
  'Belgium',
  'Romania',
  'Finland',
  'Russia',
  'Serbia',
  'Luxembourg',
  'Malta',
  'Ukraine',
  'Norway',
  'Latvia',
  'Austria',
  'Greece',
  'Slovenia',
  'Czechia',
  'Bulgaria',
  'Hungary',
  'Moldova',
  'Ireland',
  'Bosnia & Herzegovina',
  'Albania',
  'Croatia',
  'Denmark',
  'Lithuania',
  'Slovakia',
  'Kosovo',
  'Montenegro',
  'North Macedonia',
  'Liechtenstein',
  'Belarus',
  'Iceland',
  'Estonia',
  'Andorra',
  'Gibraltar',
  'Guernsey',
  'Isle of Man',
  'Jersey',
  'San Marino',
  'Monaco',
  'Svalbard & Jan Mayen') then 'Other Europe'
   else 'Other' end
	,case when source in ('unknown','None') then 'Unknown'
	    when source='(direct)' then 'Direct'
	    when (source in ('google') and medium='cpc') or source in ('GoogleAds','googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
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
		when campaign like '%notta_kol_%' and medium='youtube' then 'KOL'
		when source in ('%kol%','%KOL%','%KOC%','TT') then 'KOL'
--		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end
	,case when campaign in ('None','unknown') then null else campaign end
	,first_user_landing_page
)

select
	COALESCE(a.signup_date,b.payment_date) as date
	,COALESCE(a.source,b.source) as source
	,COALESCE(a.campaign,b.campaign,'unknown') as campaign
	,COALESCE(a.signup_platform,b.pay_channel,'unknown') as platform
	,COALESCE(a.signup_country,b.signup_country,'unknown') as signup_country
	,COALESCE(a.first_user_landing_page,b.first_user_landing_page) as first_user_landing_page
	,COALESCE(signup_users,0) as signup_users
	,COALESCE(create_record_users,0) as create_record_users
	,COALESCE(new_paid_users,0) as new_paid_users
	,COALESCE(first_payment_rev,0) as first_payment_rev
	,COALESCE(cumulative_rev,0) as cumulative_rev
from campaign_signup a
full join campaign_revenue b on a.signup_date=b.payment_date and a.source=b.source and a.campaign=b.campaign and a.signup_platform=b.pay_channel and a.signup_country=b.signup_country and a.first_user_landing_page=b.first_user_landing_page
