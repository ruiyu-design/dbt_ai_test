{{ config(
    materialized='table',
    full_refresh=True
) }}

with user_details as (
select
uid
,email
,signup_time
,signup_platform
,signup_country
,device_category
,source
,campaign
,medium
,first_user_landing_page
,profession
,role
,domain
,same_domain_users
,is_create_ws
,create_first_ws_time
,first_ws_type
,create_ws_count
,is_joined_other_ws
,joined_ws_plan_type
,is_create_record
,first_transcription_create_time
,first_transcription_type
,first_transcription_duration
,first_transcription_language
,is_trial
,first_trial_start_time
,first_trial_plan_type
,is_pro_trial
,first_pro_trial_create_time
,is_biz_trial
,first_biz_trial_create_time
,is_paid
,first_paid_plan_start_time
,first_paid_plan_type
,first_pro_pay_create_time
,first_biz_pay_create_time
,is_upsell
,first_upsell_create_time
,pay_amount_usd
,pay_order_count
,total_record_count
,total_record_duration
,import_file_record_count
,realtime_record_count
,meeting_record_count
,last_transcription_create_time
,last_7days_record_count
,last_7days_weekend_record_count
,last_30days_record_count
,last_30days_ai_notes_count
,user_active_segment
,current_ws_id
,current_plan_type
,current_interest_seats
,current_interest_start_time
,current_interest_flush_time
,current_interest_import_total
,current_interest_import_used
,current_interest_duration_total
,current_interest_duration_used
,current_interest_ai_summary_used
,current_plan_end_time
,subscription_renewal_status
,is_offline_user
,pt
from {{ ref('user_details') }} a
where
    pt=date_add(date(current_timestamp()),interval -1 day)
    and (
    signup_country ='Japan'
    or(
        signup_country in('United States','Singapore','United Kingdom','Canada','Australia','Malaysia')
        and (
        subscription_renewal_status='Subscribed'
        or date(first_paid_plan_start_time)>='2024-05-01'
        or signup_date>='2024-08-01'
        )
    )
 )
)

select
    a.*
    ,b.first_bind_date as first_bind_memo_date
    ,b.first_bind_date_plan_type as first_bind_memo_date_plan_type
    ,b.first_bind_date_plan_is_trial as first_bind_memo_date_plan_is_trial
    ,b.current_bind_status as current_bind_memo_status
    ,b.current_bind_ws_plan_type as current_bind_memo_plan_type
    ,b.first_starter_plan_date
    ,b.first_upgrade_from_starter_date
    ,b.bind_user_type as first_bind_memo_user_type
from user_details a
left join {{ ref('notta_memo_user_details') }} b on a.uid=b.uid