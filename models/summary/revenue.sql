
with order_table as(

--有付费金额的订单
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
    ,a.seats_size
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
from {{ source('Aurora', 'payment_center_order_table') }} a
inner join {{ source('Aurora', 'notta_mall_interest_goods') }} b on b.goods_id = a.goods_id
left join {{ source('Summary', 'exchange_rates') }} c on c.currency=a.pay_currency
where
	a.status in (4,8,13) --支付/升级成功
	and a.uid is not null
	and a.order_sn is not null
	and a.pay_amount>0 --筛选有付费金额
	and a.pay_channel in (5,6,7,8)
	and a.is_trial=0
--	and a.create_time > UNIX_SECONDS(TIMESTAMP('2021-01-01'))
),

uid as (
--排除测试和临时邮箱账户
    SELECT
        u.uid
        ,max_by(signup_platform,pt) as signup_platform
        ,max_by(profession,pt) as profession
        ,max_by(signup_country,pt) as signup_country
        ,max_by(medium,pt) as medium
        ,max_by(campaign,pt) as campaign
        ,max_by(source,pt) as source
    FROM {{ ref('user_details') }} u
    WHERE
        pt=date_add(date(current_timestamp()),interval -1 day)
    group by uid
)



select
	date(payment_date) as payment_date
	,case when date_sub(date(current_timestamp()),interval 1 day)=last_day(date_sub(date(current_timestamp()),interval 1 day),MONTH) then 'MoM' --如果昨天是每个月的最后一天则取完整月数据
	    when extract(day from date(payment_date))<=extract(day from date_sub(date(current_timestamp()),interval 1 day)) then 'MoM'
	    else 'Not MoM' end as is_mom
	,case when extract(month from date(payment_date))<extract(month from date_sub(date(current_timestamp()),interval 1 day)) then 'YoY'
	      when extract(month from date(payment_date))=extract(month from date_sub(date(current_timestamp()),interval 1 day)) and extract(day from date(payment_date))<=extract(day from date_sub(date(current_timestamp()),interval 1 day)) then 'YoY'
	      else 'Not YoY' end as is_yoy
--	,case when a.pay_currency in ('JPY','USD') then pay_currency else 'Other' end as
	,a.pay_currency
	,case when plan_type_name='addon' then 'addon'
		when period_unit ='monthly' and period_count=12 then concat(plan_type_name,'-annually')
		when period_unit ='annually' and period_count=1 then concat(plan_type_name,'-annually')
		else concat(plan_type_name,'-',period_unit,'-',period_count)
		end as plan_type
	,discount_type
	,case when seats_size=1 then '1 seat'
	    when seats_size<=20 then '2-20 seats'
	    when seats_size<=200 then '21-200 seats'
	    when seats_size<=500 then '201-500 seats'
	    else '500+ seats' end as seat_type
	,case when source='(direct)' then 'Direct'
	    when source in ('None','unknown') then 'Unknown'
	    when source='chatgpt.com' then 'Other Organic'
	    when (source in ('google','GoogleAds') and medium='cpc') or source in ('googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
		when source in ('bing','BingAds') and medium='cpc' then 'BingAds'
		when source in ('yahoo','yahoo2','YahooAds') and medium='cpc' then 'YahooAds'
		when (source in ('MetaAds','facebook') and medium='cpc') or source='Facebook Ads' then 'MetaAds'
		when (source='Apple' and medium='search') or source='Apple Search Ads' then 'ASA'
		when source='moloco_int' then 'MolocoAds'
		when source='appier_int' then 'AppierAds'
		when source='mintegral_int' then 'MintegralAds'
		when source='tiktokglobal_int' then 'TiktokAds'
		when source in ('impact','endorsely') then 'Affiliate'
		when source like '%_int' then 'Affiliate'
		when campaign like 'notta_kol_' and medium='youtube' then 'KOL'
		when medium like 'kol_%' then 'KOL'
		when source in ('kol','KOL','KOC','TT') then 'KOL'
--		when medium='referral' then 'Referral'
--		when source='Tools' then 'Tools'
--		when source in ('Blog','blog') then 'Blog'
		else 'Other Organic'
		end as Source
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
  'South Korea',
  'Taiwan'
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
    ,b.profession
	,b.signup_platform
	,pay_channel
	,case when rn=1 then 'New'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-1','pro-monthly-6') and concat(plan_type_name,'-',period_unit,'-',period_count)='pro-monthly-12' then 'Upsell: Pro M- Pro Y'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-1','pro-monthly-6') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-1' then 'Upsell: Pro M- Biz M'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-1','pro-monthly-6') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-12' then 'Upsell: Pro M- Biz Y'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('biz-monthly-1') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-12' then 'Upsell: Biz M- Biz Y'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-12') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-1' then 'Upsell: Pro Y- Biz M'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-12') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-12' then 'Upsell: Pro Y- Biz Y'
		else 'Recurring'
		end as revenue_type_detail
	,case when rn=1 then 'New'
		when preceding_plan_type='pro' and plan_type_name='biz' then 'Upsell'
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='annually' then 'Upsell'
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='monthly' and period_count=12 then 'Upsell'
		else 'Recurring'
		end as revenue_type
	,round(sum(pay_amount/exchange_rate/100),2) as revenue
	,round(sum(case when rn=1 then pay_amount else 0 end/exchange_rate/100),2) as new_revenue
	,round(sum(case when rn=1 then 0
		when preceding_plan_type='pro' and plan_type_name='biz' then pay_amount
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='annually' then pay_amount
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='monthly' and period_count=12 then pay_amount
		else 0 end/exchange_rate/100),2) as upsell_revenue
	,round(sum(case when rn=1 then 0
		when preceding_plan_type='pro' and plan_type_name='biz' then 0
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='annually' then 0
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='monthly' and period_count=12 then 0
		else pay_amount end/exchange_rate/100),2) as recurring_revenue
    ,count(distinct a.uid) as paid_user
    ,count(case when rn=1 then 1 else null end) as new_paid_user
    ,count(1) as paid_order
from order_table a
inner join uid b on b.uid=a.uid
group by
	date(payment_date)
	,case when date_sub(date(current_timestamp()),interval 1 day)=last_day(date_sub(date(current_timestamp()),interval 1 day),MONTH) then 'MoM' --如果昨天是每个月的最后一天则取完整月数据
	    when extract(day from date(payment_date))<=extract(day from date_sub(date(current_timestamp()),interval 1 day)) then 'MoM'
	    else 'Not MoM' end
	,case when extract(month from date(payment_date))<extract(month from date_sub(date(current_timestamp()),interval 1 day)) then 'YoY'
	      when extract(month from date(payment_date))=extract(month from date_sub(date(current_timestamp()),interval 1 day)) and extract(day from date(payment_date))<=extract(day from date_sub(date(current_timestamp()),interval 1 day)) then 'YoY'
	      else 'Not YoY' end
	,a.pay_currency
	,case when plan_type_name='addon' then 'addon'
		when period_unit ='monthly' and period_count=12 then concat(plan_type_name,'-annually')
		when period_unit ='annually' and period_count=1 then concat(plan_type_name,'-annually')
		else concat(plan_type_name,'-',period_unit,'-',period_count)
		end
	,discount_type
	,case when seats_size=1 then '1 seat'
	    when seats_size<=20 then '2-20 seats'
	    when seats_size<=200 then '21-200 seats'
	    when seats_size<=500 then '201-500 seats'
	    else '500+ seats' end
	,case when source='(direct)' then 'Direct'
	    when source in ('None','unknown') then 'Unknown'
	    when source='chatgpt.com' then 'Other Organic'
	    when (source in ('google','GoogleAds') and medium='cpc') or source in ('googleadwords_int','GG_Ads_PMax_EN_230825') then 'GoogleAds'
		when source in ('bing','BingAds') and medium='cpc' then 'BingAds'
		when source in ('yahoo','yahoo2','YahooAds') and medium='cpc' then 'YahooAds'
		when (source in ('MetaAds','facebook') and medium='cpc') or source='Facebook Ads' then 'MetaAds'
		when (source='Apple' and medium='search') or source='Apple Search Ads' then 'ASA'
		when source='moloco_int' then 'MolocoAds'
		when source='appier_int' then 'AppierAds'
		when source='mintegral_int' then 'MintegralAds'
		when source='tiktokglobal_int' then 'TiktokAds'
		when source in ('impact','endorsely') then 'Affiliate'
		when source like '%_int' then 'Affiliate'
		when campaign like 'notta_kol_' and medium='youtube' then 'KOL'
		when medium like 'kol_%' then 'KOL'
		when source in ('kol','KOL','KOC','TT') then 'KOL'
--		when medium='referral' then 'Referral'
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
  'South Korea',
  'Taiwan'
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
	,b.profession
	,b.signup_platform
	,pay_channel
	,case when rn=1 then 'New'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-1','pro-monthly-6') and concat(plan_type_name,'-',period_unit,'-',period_count)='pro-monthly-12' then 'Upsell: Pro M- Pro Y'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-1','pro-monthly-6') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-1' then 'Upsell: Pro M- Biz M'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-1','pro-monthly-6') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-12' then 'Upsell: Pro M- Biz Y'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('biz-monthly-1') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-12' then 'Upsell: Biz M- Biz Y'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-12') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-1' then 'Upsell: Pro Y- Biz M'
		when concat(preceding_plan_type,'-',preceding_period_unit,'-',preceding_period_count) in ('pro-monthly-12') and concat(plan_type_name,'-',period_unit,'-',period_count)='biz-monthly-12' then 'Upsell: Pro Y- Biz Y'
		else 'Recurring'
		end
	,case when rn=1 then 'New'
		when preceding_plan_type='pro' and plan_type_name='biz' then 'Upsell'
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='annually' then 'Upsell'
		when preceding_period_unit='monthly' and preceding_period_count<12 and period_unit='monthly' and period_count=12 then 'Upsell'
		else 'Recurring'
		end



