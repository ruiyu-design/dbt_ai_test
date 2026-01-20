
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        }
    )
}}

with event_detail as(
--web埋点
select
	 event_timestamp
	,event_name
	,user_properties
	,geo.continent as continent
	,geo.country as country
from `analytics_345009513.events_*`
where
	(geo.country in ('Japan','United States') or geo.continent='Europe')
	and event_name in ('open_detail','dashboard_newrecord_start','ai_notes_generate_succeed')
	and _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -3 day))
	and date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -3 day)
),

app_event_detail as(
--app埋点
select
	event_timestamp
	,event_name
	,user_id
	,case when device.operating_system in ('iOS','Android') then device.operating_system
	    else 'else' end as platform
	,geo.continent as continent
	,geo.country as country
from `analytics_234597866.events_*`
where
    (geo.country in ('Japan','United States') or geo.continent='Europe')
    and event_name in ('home_record_item_btn_click','recording_end_btn_click','recording_end_change_title','file_transcribe_media_type','ai_summary_generate_btn')
    and _TABLE_SUFFIX>=format_date('%Y%m%d',date_add(date(current_timestamp()),interval -3 day))
	and date(timestamp_micros(event_timestamp))>=date_add(date(current_timestamp()),interval -3 day)

),

user_country as(
 select uid,signup_country
  from {{ ref('user_details') }}
  where pt=(select max(pt) from {{ ref('user_details') }} )
  and signup_country in ('Japan','United States','Sweden',
    'United Kingdom',
    'Spain',
    'Germany',
    'Switzerland',
    'Portugal',
    'Italy',
    'France',
    'Belgium',
    'Romania',
    'Finland',
    'Russia',
    'Serbia',
    'Netherlands',
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
    'Poland',
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
    'Svalbard & Jan Mayen')
),

ai_notes_detail as(
--后端ai notes 记录
select
	timestamp_seconds(timestamp) as event_timestamp
	,case when platform='Web' and trigger_type=1 then 'web_ai_notes_manual'
		when platform in ('Android','IOS') and trigger_type=1 then 'app_ai_notes_manual'
		when trigger_type=2 then 'web_ai_notes_auto'
		else 'else' end as event_name
	,cast(a.uid as string) as user_id
	,case when platform in ('Web','Server') then 'WEB' else platform end as platform
	,case when b.signup_country='Japan' then 'Asia' when b.signup_country='United States' then 'Americas' else 'Europe' end as continent
	,b.signup_country as country
from `mc_data_statistics.notta_summary_ai_records` a
inner join user_country b on a.uid=b.uid
where
	a.uid is not null
	and platform in ('Server','Web','Android','IOS')
	and date(timestamp_seconds(timestamp))>=date_add(date(current_timestamp()),interval -3 day)
)

select
    date(timestamp_micros(event_timestamp)) as event_date_dt
	,FORMAT_DATE('%Y-W%W', date(timestamp_micros(event_timestamp))) as event_week
	,date_trunc(date(timestamp_micros(event_timestamp)),week(monday)) as event_week_first_date
	,timestamp_micros(event_timestamp) as event_timestamp
	,continent
	,country
    ,event_name
	,'WEB' as platform
	,cast(case
        when user_property.key = 'uid' THEN user_property.value.int_value
        when user_property.key = 'user_id' THEN user_property.value.int_value
        end as string) as user_id
from event_detail ,UNNEST(user_properties) AS user_property
where user_property.key IN ('uid', 'user_id') and user_property.value.int_value is not null
union all
select
    date(timestamp_micros(event_timestamp)) as event_date_dt
	,FORMAT_DATE('%Y-W%W', date(timestamp_micros(event_timestamp))) as event_week
	,date_trunc(date(timestamp_micros(event_timestamp)),week(monday)) as event_week_first_date
	,timestamp_micros(event_timestamp) as event_timestamp
	,continent
	,country
    ,event_name
	,platform
	,user_id
from app_event_detail
where user_id is not null and user_id!=''
and REGEXP_CONTAINS(user_id, r'^[0-9]+$')
union all
select
    date(event_timestamp) as event_date_dt
	,FORMAT_DATE('%Y-W%W', date(event_timestamp)) as event_week
	,date_trunc(date(event_timestamp),week(monday)) as event_week_first_date
	,event_timestamp
	,continent
	,country
    ,event_name
	,platform
	,user_id
from ai_notes_detail