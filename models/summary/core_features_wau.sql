{{ config(
    materialized='table',
    full_refresh=True
) }}


with new_user as (
--取注册用户注册时间
select
	user_id
	,min(event_date_dt) as min_date
	,FORMAT_DATE('%Y-W%W', min(event_date_dt)) as min_week
from {{ ref('stg_ga4_core_features') }}
group by
	user_id
)

select
	event_week
	,event_week_first_date
	,case when event_week=b.min_week then 'New' else 'Old' end as user_type
	,case when continent='Asia' then 'Japan' when continent='Americas' then 'United States'
		else continent end as event_country
	,count(distinct case when platform='WEB' then a.user_id else null end) as web_wau
	,count(distinct case when platform in ('iOS','Android') then a.user_id else null end) as app_wau
	,count(distinct a.user_id) as wau
	,count(distinct case when platform='WEB' and event_name in ('open_detail','home_record_item_btn_click') then a.user_id else null end) as web_open_detail_wau
	,count(distinct case when platform='WEB' and event_name in ('dashboard_newrecord_start','recording_end_btn_click','recording_end_change_title','file_transcribe_media_type') then a.user_id else null end) as web_record_wau
	,count(distinct case when platform='WEB' and event_name in ('ai_notes_generate_succeed','ai_summary_generate_btn','web_ai_notes_manual','web_ai_notes_auto') then a.user_id else null end) as web_summary_wau
	,count(distinct case when platform in ('iOS','Android') and event_name in ('open_detail','home_record_item_btn_click') then a.user_id else null end) as app_open_detail_wau
	,count(distinct case when platform in ('iOS','Android') and event_name in ('dashboard_newrecord_start','recording_end_btn_click','recording_end_change_title','file_transcribe_media_type') then a.user_id else null end) as app_record_wau
	,count(distinct case when platform in ('iOS','Android') and event_name in ('ai_notes_generate_succeed','ai_summary_generate_btn','app_ai_notes_manual') then a.user_id else null end) as app_summary_wau
	,count(distinct case when event_name in ('open_detail','home_record_item_btn_click') then a.user_id else null end) as total_open_detail_wau
	,count(distinct case when event_name in ('dashboard_newrecord_start','recording_end_btn_click','recording_end_change_title','file_transcribe_media_type') then a.user_id else null end) as total_record_wau
	,count(distinct case when  event_name in ('ai_notes_generate_succeed','ai_summary_generate_btn','web_ai_notes_manual','app_ai_notes_manual','web_ai_notes_auto') then a.user_id else null end) as total_summary_wau
from {{ ref('stg_ga4_core_features') }} a
inner join new_user b on b.user_id=a.user_id
group by
	event_week
	,event_week_first_date
	,case when event_week=b.min_week then 'New' else 'Old' end
	,case when continent='Asia' then 'Japan' when continent='Americas' then 'United States'
		else continent end








