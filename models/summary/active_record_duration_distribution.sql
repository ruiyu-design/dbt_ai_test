
select
	record_date
	,signup_country
	,plan_type
	,duration_category
	,transcription_type
	,transcribe_speaker_num
	,transcribe_source
	,transcribe_language
	,transcribe_case
	,sum(record_count) as record_count
	,sum(translate_record_count) as translate_record_count
	,sum(summary_record_count) as summary_record_count
	,sum(record_duration) as record_duration
	,count(distinct uid) as user_count
from dbt_models_details.stg_active_user_records_daily
group by
	record_date
	,signup_country
	,plan_type
	,duration_category
	,transcription_type
	,transcribe_speaker_num
	,transcribe_source
	,transcribe_language
	,transcribe_case











