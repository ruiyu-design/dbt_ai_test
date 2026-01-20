select
  signup_date
  ,signup_platform
  ,case when (source in ('google','GoogleAds') and medium='cpc') or source in ('googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
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
		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end as source
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
  ,case when device_category in ('desktop','mobile','unknown','tablet') then device_category else 'other' end as device_category
  ,profession
  ,case when is_trial=1 then 'is trial' else 'not trail' end as is_trial
  ,count(distinct uid) as signup_users
  ,count(distinct case when is_create_ws=1 then uid else null end) as create_ws_users
  ,count(distinct case when is_create_record=1 and datetime_diff(first_transcription_create_time,signup_time,HOUR)<=24 then uid else null end) as create_record_users
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,DAY)<=7 then uid else null end) as paid_users
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=96 then uid else null end) as paid_users_in96hours
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=72 then uid else null end) as paid_users_in72hours
  ,count(distinct case when is_paid=1 and datetime_diff(first_paid_plan_start_time,signup_time,HOUR)<=24 then uid else null end) as paid_users_in24hours
from `dbt_models_details.user_details`
where pt=date_add(date(current_timestamp()),interval -1 day)
    and is_joined_other_ws=0
group by
  signup_date
  ,signup_platform
  ,case when (source in ('google','GoogleAds') and medium='cpc') or source in ('googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
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
		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end
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
  ,case when device_category in ('desktop','mobile','unknown','tablet') then device_category else 'other' end
  ,profession
  ,case when is_trial=1 then 'is trial' else 'not trail' end
