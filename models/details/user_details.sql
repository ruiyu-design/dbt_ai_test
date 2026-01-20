
{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
        'field': 'pt',
        'data_type': 'date'
    }
) }}



WITH user_data AS (
    SELECT
        u.uid
        ,u.email
        ,SUBSTR(u.email, instr(u.email, '@') + 1) as domain
        ,timestamp_seconds(u.create_time) as signup_time
        ,u.os_type
    FROM
        `notta-data-analytics.notta_aurora.langogo_user_space_users` u
    WHERE
    	u.email!='' and u.email is not null
    	and u.status=1 --
        -- Company E-mail
        AND u.email NOT LIKE '%@airgram.io'
        AND u.email NOT LIKE '%@notta.ai'
        AND u.email NOT LIKE '%@langogo.test'
        AND u.email NOT LIKE '%@langogo.ai'
        -- Temporary E-mail
        AND u.email NOT LIKE '%@chacuo.net'
        AND u.email NOT LIKE '%@uuf.me'
        AND u.email NOT LIKE '%@nqmo.com'
        AND u.email NOT LIKE '%@linshiyouxiang.net'
        AND u.email NOT LIKE '%@besttempmail.com'
        AND u.email NOT LIKE '%@celebrityfull.com'
        AND u.email NOT LIKE '%@comparisions.net'
        AND u.email NOT LIKE '%@mediaholy.com'
        AND u.email NOT LIKE '%@maillazy.com'
        AND u.email NOT LIKE '%@justdefinition.com'
        AND u.email NOT LIKE '%@inctart.com'
        AND u.email NOT LIKE '%@deepyinc.com'
),

same_domain_users as (

    select
        substr(u.email, instr(u.email, '@') + 1) as domain
        ,count(distinct uid) as same_domain_users
    from
        `notta-data-analytics.notta_aurora.langogo_user_space_users` u
    WHERE
	     u.email!='' and u.email is not null
	     and u.email NOT LIKE '%@gmail.com'--不计算
	     and u.email NOT LIKE '%@outlook.com'--不计算
	     and u.email NOT LIKE '%@hotmail.com'--不计算
	     and u.email NOT LIKE '%@icloud.com'--不计算
	     and u.email NOT LIKE '%@privaterelay.appleid.com'--不计算

    group by
    	substr(u.email, instr(u.email, '@') + 1)
),

user_country as (
select
	uid
	,min_by(country,event_date_dt) as country
	,min_by(city,event_date_dt) as city
from
	(
		select
			cast(uid as string) as uid
		   ,country
		   ,city
		   ,event_date_dt
		from {{ ref('stg_ga4_web_sign_up') }}
		where country!='unknown'
		union all
		select
		    uid
		    ,country
		    ,city
		    ,event_date_dt
		from {{ ref('stg_ga4_app_sign_up') }}
		where country!='unknown'
		union all
		select
			COALESCE(uid,user_id) as uid
			,min_by(case when country='jp' then 'Japan'
			        when country='us' then 'United States'
			        when country='fr' then 'France'
			        WHEN country = 'de' THEN 'Germany'
                      WHEN country = 'gb' THEN 'United Kingdom'
                      WHEN country = 'ca' THEN 'Canada'
                      WHEN country = 'nl' THEN 'Netherlands'
                      WHEN country = 'au' THEN 'Australia'
                      WHEN country = 'sg' THEN 'Singapore'
                      WHEN country = 'hk' THEN 'Hong Kong'
                      WHEN country = 'ch' THEN 'Switzerland'
			else country end,timestamp) as country
            ,null as city
			,date(min(timestamp)) as event_date_dt
		FROM `notta-data-analytics.notta_web_prod.sign_up`
		where COALESCE(uid,user_id) is not null and COALESCE(uid,user_id)!=''
		and country!='' and country is not null
		group by COALESCE(uid,user_id)
	)
group by
	uid
),

ga4_web_sign_up AS (
    SELECT uid,device_category,user_source,user_campaign,user_medium,first_user_landing_page FROM {{ ref('stg_ga4_web_sign_up') }}
),

ga4_app_sign_up AS (
    SELECT uid,device_category,user_source,user_campaign,user_medium
    FROM
    (
        SELECT *,ROW_NUMBER() OVER (PARTITION BY uid ORDER BY event_date_dt) AS rn
        FROM
        {{ ref('stg_ga4_app_sign_up') }}
    )
    WHERE rn=1
),

android_appsflyer_attribute as (
select *
from (
	select
		customer_user_id as uid
		,attributed_touch_type
		,attributed_touch_time
		,media_source
		,channel
		,campaign
		,row_number() over(partition by customer_user_id order by attributed_touch_type,attributed_touch_time) as rn --如果有click按click的最早归因,如果没有click按impression的最早归因
	from `notta_appsflyer.android_in_app_events`
	where
	    customer_user_id is not null and customer_user_id!=''
		and media_source!='' and media_source is not null
		and (--筛选non-organic
			media_source like '%_int'
			or media_source in (
				'Apple Search Ads'
				,'Facebook Ads'
				,'GoogleAds'
				,'KOL'
				,'KOC'
				,'TT'
				,'Social_instagram'
				)
			)
	)
where rn=1
),

ios_appsflyer_attribute as (
select *
from (
	select
		customer_user_id as uid
		,attributed_touch_type
		,attributed_touch_time
		,media_source
		,channel
		,campaign
		,row_number() over(partition by customer_user_id order by attributed_touch_type,attributed_touch_time) as rn --如果有click按click的最早归因,如果没有click按impression的最早归因
	from `notta_appsflyer.ios_in_app_events`
	where
	    customer_user_id is not null and customer_user_id!=''
		and media_source!='' and media_source is not null
		and (--筛选non-organic且非GGAds
			media_source like '%_int'
			or media_source in (
				'Apple Search Ads'
				,'Facebook Ads'
				,'GoogleAds'
				,'KOL'
				,'KOC'
				,'TT'
				,'Social_instagram'
				)
			)
	)
where rn=1
),

user_source as(
	select
		u.uid
		,COALESCE(gaw.device_category, gaa.device_category) AS device_category
		,gaw.first_user_landing_page
		,case when u.os_type=1 then gaw.user_source
			when u.os_type=2 then COALESCE(aa.media_source, gaa.user_source)
			when u.os_type=3 then COALESCE(ia.media_source, gaa.user_source)
			else null
			end as source
		,case when u.os_type=1 then gaw.user_campaign
			when u.os_type=2 then COALESCE(aa.campaign, gaa.user_campaign)
			when u.os_type=3 then COALESCE(ia.campaign, gaa.user_campaign)
			else null
			end as campaign
		,case when u.os_type=1 then gaw.user_medium
			when u.os_type=2 then gaa.user_medium
			when u.os_type=3 then gaa.user_medium
			else null
			end as medium
	from user_data u
	left join ga4_web_sign_up gaw ON u.uid = gaw.uid
	left join ga4_app_sign_up gaa ON u.uid = CAST(gaa.uid AS INT64)
	left join android_appsflyer_attribute aa ON cast(u.uid as string) = aa.uid
	left join ios_appsflyer_attribute ia ON cast(u.uid as string) = ia.uid
),

user_extra AS (
	SELECT
		*
	FROM
		(
	    SELECT
	    	uid
	    	,country
	    	,city
	    	,utm_source
	    	,utm_campaign
	    	,utm_medium
	    	,device
	    	,row_number() over(partition by uid order by uid) as rn
	    FROM {{ source('Aurora', 'user_extra') }}
	    )
	WHERE rn=1
),

user_profession as(
select
	uid
	,profession
	,use_case
from
	(
	select
		CASE
	            WHEN uid_property.key = 'uid' THEN uid_property.value.int_value
	            WHEN uid_property.key = 'user_id' THEN uid_property.value.int_value
	        END AS uid
	    ,CASE
	            WHEN user_property.key='member_profession' and user_property.value.string_value LIKE 'Other: %' THEN REGEXP_REPLACE(user_property.value.string_value, r'Other: ', '')
	            WHEN user_property.key='member_profession' then user_property.value.string_value
	        END as profession
	    ,user_case.value.string_value as use_case
	    ,ROW_NUMBER() OVER (PARTITION BY CASE
	            WHEN uid_property.key = 'uid' THEN uid_property.value.int_value
	            WHEN uid_property.key = 'user_id' THEN uid_property.value.int_value
	        END ORDER BY event_timestamp) AS row_num
	from {{ ref('stg_ga4_onboarding_submit_survey') }}
		,unnest(user_properties) as user_property
		,unnest(user_properties) as user_case
		,unnest(user_properties) as uid_property
	where
		user_property.key='member_profession'
		and user_case.key='use_case'
		AND uid_property.key IN ('uid','user_id')
	  AND uid_property.value.int_value IS NOT NULL
	  AND user_property.value.string_value NOT LIKE '%login_onboarding_num1_answer%'
	)
where row_num=1
),

user_ws as(
select
	owner_uid as uid
	,count(distinct workspace_id) as ws_count
	,timestamp_seconds(min(cast(create_time/1000 as int64))) as create_first_ws_time
	,min_by(workspace_type,create_time) as first_ws_type --1: for self 2: for team
	,min_by(workspace_id,create_time) as first_ws_id --1: for self 2: for team
from `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
where
	owner_uid is not null --过滤脏数据,目前不存在
	and workspace_id is not null --过滤脏数据,目前不存在
	and status!=2 --排除已删除
group by
	owner_uid
),

paid_ws as (
-- 当前有biz,enterprise 付费权益的ws
select
	workspace_id
	,max(goods_plan_type) as max_goods_plan_type
from `notta-data-analytics.dbt_models_details.stg_aurora_interest` a
where timestamp_seconds(a.start_valid_time)< cast(date_trunc(CURRENT_DATETIME(), day) as timestamp) --开始时间小于今天
	and timestamp_seconds(a.flush_time)> cast(date_trunc(CURRENT_DATETIME(), day) as timestamp) -- 结束时间大于今天
	and goods_plan_type in (2,3) -- biz+enterprise
group by workspace_id
 ),

user_joined_ws as (
select
	uid
	,max(max_goods_plan_type) as joined_ws_plan_type
from `notta-data-analytics.notta_aurora.langogo_user_space_member` a
inner join paid_ws b on cast(a.workspace_id as string)=b.workspace_id
where role!='owner' --取member
group by uid

),


record_detail as (--全量记录,采用found_uid

SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records0` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records1` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records2` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records3` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records4` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records5` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records6` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records7` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records8` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records9` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records10` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records11` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records12` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records13` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records14` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records15` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records16` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records17` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records18` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records19` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records20` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records21` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records22` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records23` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records24` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records25` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records26` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records27` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records28` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records29` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records30` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records31` union all
SELECT record_id, workspace_id, create_time, audio_duration, transcribe_language, creator_uid, found_uid, transcription_status, media_source, transcribe_speaker_num, transcription_type,audio_s3_url_origin,audio_upload_status FROM `notta-data-analytics.notta_aurora.langogo_user_space_records32`
),



record as (

	select
		found_uid as uid
		,min(create_time) as first_transcription_create_time
		,min_by(transcription_type,create_time) as first_transcription_type
		,min_by(audio_duration,create_time) as first_transcription_duration
		,min_by(transcribe_language,create_time) as first_transcription_language
		,min_by(audio_s3_url_origin,create_time) as audio_s3_url_origin
		,count(record_id) as total_record_count
		,sum(audio_duration) as total_record_duration
		,count(case when transcription_type in (1,7,8) then record_id else null end) as import_file_record_count
		,count(case when transcription_type in (2,10) then record_id else null end) as realtime_record_count
		,count(case when transcription_type in (3,4,11) then record_id else null end) as meeting_record_count
		,max(create_time) as last_transcription_create_time
		,count(case when date(timestamp_seconds(cast(create_time as int64)))>=date_add(date(current_timestamp()),interval -30 day) then record_id else null end) as last_30days_record_count
		,count(distinct case when date(timestamp_seconds(cast(create_time as int64)))>=date_add(date(current_timestamp()),interval -30 day) then format_date("%Y-%V",date(timestamp_seconds(cast(create_time as int64)))) else null end) as last_30days_active_week_count
		,count(case when date(timestamp_seconds(cast(create_time as int64)))>=date_add(date(current_timestamp()),interval -7 day) then record_id else null end) as last_7days_record_count
		,count(case when date(timestamp_seconds(cast(create_time as int64)))>=date_add(date(current_timestamp()),interval -7 day) and extract(dayofweek from timestamp_seconds(cast(create_time as int64))) in (1,7) then record_id else null end) as last_7days_weekend_record_count
	from record_detail
	where
		audio_s3_url_origin not in
        (
        'https://s3.ap-northeast-1.amazonaws.com/langogo.rd.audio-corpus.lock/audio/ja-JP/f56dcabd-9270-4465-a592-1913cf0cc270_171_0.wav'--排除默认记录
        ,'https://s3.ap-northeast-1.amazonaws.com/langogo.rd.audio-corpus.lock/audio/en-US/1aa645ba-e302-4042-a1ef-9dab223ddc81_171_0.wav'--排除默认记录
        ,'https://s3.ap-northeast-1.amazonaws.com/langogo.rd.audio-corpus.lock/audio/en-US/demo.mp4'--排除默认记录
        ,'https://s3.ap-northeast-1.amazonaws.com/langogo.rd.audio-corpus.lock/audio/ja-JP/demo.mp4'--排除默认记录
        )
		and transcription_status = 2 --转写成功
		and record_id is not null
		and transcribe_language is not null
		and transcribe_language !=''
		and transcription_type is not null
		and (
			(transcription_type in (2,3,4,9,10,11) and audio_s3_url_origin is not null and audio_s3_url_origin!='')
			or
			(transcription_type in (1,5,6,7,8,12) and audio_upload_status = 100)
		) --实时文件地址不为空，非实时上传进度100%
	group by found_uid
),

ai_notes as (
	select
		uid
		,count(1) as last_30days_ai_notes_count
	from `mc_data_statistics.notta_summary_ai_records` a
	where
		a.uid is not null
		and platform in ('Server','Web','Android','IOS')
		and date(timestamp_seconds(timestamp))>=date_add(date(current_timestamp()),interval -30 day)
	group by
		uid
),

order_table as (
select
	a.workspace_id
	,a.uid
	,a.order_sn
	,a.origin_order_sn
	,a.goods_plan_type
	,a.failure_time
    ,a.create_time
    ,a.is_trial
    ,a.pay_amount
    ,a.pay_currency
    ,a.goods_price
    ,a.status
    ,a.seats_size
    ,b.goods_id
	,case when b.plan_type=0 then 'Free'
    	when b.plan_type=1 then 'Pro'
    	when b.plan_type=2 then 'Biz'
    	when b.plan_type=3 then 'Enterprise'
    	else 'unknown' end as plan_type
    ,case when b.period_unit=1 then 'annually'
    	when b.period_unit=2 then 'monthly'
    	when b.period_unit=3 then 'days'
    	when b.period_unit=4 then 'hours'
    	when b.period_unit=5 then 'mins'
    	else cast(period_unit as string) end as period_unit
    ,b.period_count
    ,case when a.goods_id in (1040,1057) then 'basic'
        when b.type=1 then 'basic'
	    when b.type=2 then 'addon'
	    when b.type=3 then 'discount'
	    when b.type=4 then 'edu'
	    when b.type=5 then 'addseats'
	    when b.type=6 then 'trial'
	    when b.type=7 then 'limit-discount'
	    when b.type=8 then 'ai addon'
	    when b.type=9 then 'translate addon'
	    else cast(type as string) end as type
    ,lag(case when b.plan_type=0 then 'Free'
    	when b.plan_type=1 then 'Pro'
    	when b.plan_type=2 then 'Biz'
    	when b.plan_type=3 then 'Enterprise'
    	else 'unknown' end) over (partition by workspace_id order by a.create_time asc) as preceding_plan_type
    ,lag(a.is_trial) over (partition by workspace_id order by a.create_time asc) as preceding_is_trial
    ,lag(b.period_count) over (partition by workspace_id order by a.create_time asc) as preceding_period_count
    ,lag(a.status) over (partition by workspace_id order by a.create_time asc) as preceding_status
    ,lag(case when b.period_unit=1 then 'annually'
    	when b.period_unit=2 then 'monthly'
    	when b.period_unit=3 then 'days'
    	when b.period_unit=4 then 'hours'
    	when b.period_unit=5 then 'mins'
    	else cast(period_unit as string) end) over (partition by workspace_id order by a.create_time asc) as preceding_period_unit
    ,lag(a.seats_size) over (partition by workspace_id order by a.create_time asc) as preceding_seats_size
from `notta-data-analytics.notta_aurora.payment_center_order_table` a
inner join `notta-data-analytics.notta_aurora.notta_mall_interest_goods` b on b.goods_id = a.goods_id
where
	a.status in (4,8,13) --支付/升级成功
	and a.uid is not null
	and a.order_sn is not null
--	and (a.addon_ids='' or a.addon_ids='[]')--筛除addon权益
	and a.goods_id not in (11001,12166,13133) --筛除邀请赠送3天,抽奖商品,workemail7天免费试用
	and a.pay_channel in (5,6,7,8,11,12,13) --stripe、app、google、线下支付
	and (b.type in (1,3,4,6,7) or a.goods_id in (1040,1057))  --筛除addon
	and b.tag!='add-on'--筛除addon权益
	and b.tag!=''--筛除addon权益
	and b.period_unit in (1,2) --筛选正式年月权益
),

is_upsell_details as (
select
	uid
	,timestamp_seconds(min(create_time)) as first_upsell_create_time
	,min_by(order_sn,create_time) as first_order_sn
	,min_by(concat(preceding_plan_type,'-',preceding_period_unit,preceding_period_count,'-to-',plan_type,'-',period_unit,period_count),create_time) as upsell_type
from order_table
where
	(
		preceding_is_trial=0 and preceding_plan_type='Pro' and plan_type='Pro'
		and preceding_period_unit='monthly' and preceding_period_count=1
		and ((period_unit='monthly' and period_count=12) or (period_unit='annually' and period_count=1))
		)--'Pro-monthly to Pro-annually'
	or
	(
		preceding_is_trial=0 and preceding_plan_type='Biz' and plan_type='Biz'
		and preceding_period_unit='monthly' and preceding_period_count=1
		and ((period_unit='monthly' and period_count=12) or (period_unit='annually' and period_count=1))
		)--'Biz-monthly to Biz-annually'
	or
	(
		preceding_is_trial=0 and preceding_plan_type='Pro' and plan_type='Biz'
		)--'Pro-Biz'
group by uid

),

order_table_user as (
--取ws维度,首次试用pro时间,首次试用biz时间,首次付费pro时间,首次付费biz时间, 首次付费pro时间
select
	uid
	,min(case when is_trial=1 then timestamp_seconds(create_time) else null end) as first_trial_start_time
	,min_by(a.goods_plan_type,case when is_trial=1 then timestamp_seconds(create_time) else null end) as first_trial_plan_type
	,min(case when is_trial=1 and goods_plan_type=1 then timestamp_seconds(create_time) else null end) as first_pro_trial_create_time
	,min(case when is_trial=1 and goods_plan_type=1 then timestamp_seconds(failure_time) else null end) as first_pro_trial_end_time
	,min(case when is_trial=1 and goods_plan_type=2 then timestamp_seconds(create_time) else null end) as first_biz_trial_create_time
	,min(case when is_trial=1 and goods_plan_type=2 then timestamp_seconds(failure_time) else null end) as first_biz_trial_end_time
	,min(case when is_trial=0 then timestamp_seconds(create_time) else null end) as first_paid_plan_start_time
	,min_by(concat(plan_type,'-',period_unit,period_count,'-',type),case when is_trial=0 then timestamp_seconds(create_time) else null end) as first_paid_plan_type
	,min(case when is_trial=0 and goods_plan_type=1 then timestamp_seconds(create_time) else null end) as first_pro_pay_create_time
	,min(case when is_trial=0 and goods_plan_type=1 then timestamp_seconds(failure_time) else null end) as first_pro_pay_end_time
	,min(case when is_trial=0 and goods_plan_type=2 then timestamp_seconds(create_time) else null end) as first_biz_pay_create_time
	,min(case when is_trial=0 and goods_plan_type=2 then timestamp_seconds(failure_time) else null end) as first_biz_pay_end_time
	,round(sum(case when is_trial=0 then a.pay_amount else 0 end/ifnull(b.rate,1))/100,2) as pay_amount_usd
	,count(case when is_trial=0 then order_sn else null end) as pay_order_count
from order_table a
left join {{ source('Summary', 'exchange_rates') }} b on upper(a.pay_currency)=b.currency
group by uid
),

user_interest_ws as (
--取当前权益ws
select
	uid
	,workspace_id
	,case when goods_id=11108 then 4 else plan_type end as plan_type
	,order_sn
	,current_interest_start_time
	,current_interest_flush_time
	,cast(current_interest_seats as int) as current_interest_seats
	,cast(current_interest_import_total as int) as current_interest_import_total
	,cast(current_interest_import_used as int) as current_interest_import_used
	,cast(current_interest_duration_total as int) as current_interest_duration_total
	,cast(current_interest_duration_used as int) as current_interest_duration_used
	,cast(current_interest_ai_summary_used as int) as current_interest_ai_summary_used
from
	(
	select
		a.uid
		,a.workspace_id
		,a.goods_plan_type as plan_type
		,a.goods_id
		,a.id
		,a.order_sn
		,timestamp_seconds(a.start_valid_time) as current_interest_start_time
		,timestamp_seconds(a.flush_time) as current_interest_flush_time
		,JSON_EXTRACT_SCALAR(a.common_interest,'$.seats') current_interest_seats
		,JSON_EXTRACT_SCALAR(a.consume_interest,'$.notta_interest_import_audio.total' ) as current_interest_import_total
		,JSON_EXTRACT_SCALAR(a.consume_interest,'$.notta_interest_import_audio.used') as current_interest_import_used
		,JSON_EXTRACT_SCALAR(a.consume_interest,'$.duration.total') as current_interest_duration_total
		,JSON_EXTRACT_SCALAR(a.consume_interest,'$.duration.used') as current_interest_duration_used
		,JSON_EXTRACT_SCALAR(a.consume_interest,'$.notta_interest_ai_summary.used') as current_interest_ai_summary_used
		,row_number()over(partition by uid order by goods_plan_type desc,id asc ) as row_num --如果多个ws,优先高级别ws,同级别有多个,按创建时间选第一个
	from `notta-data-analytics.dbt_models_details.stg_aurora_interest` a
	where timestamp_seconds(a.start_valid_time)< cast(date_trunc(CURRENT_DATETIME(), day) as timestamp) --开始时间小于今天
	and timestamp_seconds(a.flush_time)> cast(date_trunc(CURRENT_DATETIME(), day) as timestamp) -- 结束时间大于今天
  	and goods_type in (1,3,4,7) -- 正式权益包，取消折扣包,教育折扣包，退订挽留包
	)
where row_num=1
),

user_interest_ws_end_time as (
--用户当前权益ws套餐到期时间
select
	a.uid
	,a.workspace_id
	,max(timestamp_seconds(a.flush_time)) as current_plan_end_time
from `notta-data-analytics.dbt_models_details.stg_aurora_interest` a
inner join user_interest_ws b on a.workspace_id=b.workspace_id and a.uid=b.uid
where timestamp_seconds(a.flush_time)> cast(date_trunc(CURRENT_DATETIME(), day) as timestamp) -- 结束时间大于今天
and goods_type in (1,3,4,7) -- 正式权益包，取消折扣包,教育折扣包，退订挽留包
group by
	a.uid
	,a.workspace_id
),

user_renewal as (
--取用户当前权益ws套餐订阅状态
select
	a.workspace_id
	,max_by(renewal_status,id) as renewal_status
from user_interest_ws a
left join`notta-data-analytics.notta_aurora.payment_center_subscribe_table` b on a.workspace_id=b.workspace_id
where pay_channel in (5,6,7,8) --stripe、app、google --排除线下渠道订单的订阅状态
group by
    a.workspace_id
),

offline_user as (

select distinct
    a.uid
from `notta-data-analytics.notta_aurora.payment_center_order_table` a
inner join `notta-data-analytics.notta_aurora.notta_mall_interest_goods` b on b.goods_id = a.goods_id
where
	a.status in (4,8,13) --支付/升级成功
	and a.uid is not null
	and a.order_sn is not null
	and a.goods_id not in (11001,12166,13133) --筛除邀请赠送3天,抽奖商品,workemail7天免费试用
	and a.pay_channel in (11,12,13) --线下支付
	and (b.type in (1,3,4,6,7) or a.goods_id in (1040,1057))  --筛除addon
	and b.tag!='add-on'--筛除addon权益
	and b.tag!=''--筛除addon权益
	and b.period_unit in (1,2) --筛选正式年月权益
)


select
	u.uid
	,u.email
    ,u.signup_time
    ,date(u.signup_time) as signup_date
    ,case
    	when u.os_type=1 then 'WEB'
    	when u.os_type=2 then 'ANDROID'
    	when u.os_type=3 then 'IOS'
    	else 'unknown'
    end as signup_platform
    ,COALESCE(uc.country, ue.country, 'unknown') AS signup_country
    ,COALESCE(uc.city, ue.city, 'unknown') AS signup_city
    ,COALESCE(us.device_category,ue.device, 'unknown') AS device_category
    ,COALESCE(us.source,ue.utm_source,'unknown') AS source
    ,COALESCE(us.campaign,ue.utm_campaign, 'unknown') AS campaign
    ,COALESCE(us.medium,ue.utm_medium, 'unknown') AS medium
    ,COALESCE(us.first_user_landing_page, 'unknown') AS first_user_landing_page
    ,case when up.profession is null then 'unknown'
        when up.profession in ('Student','学生') then 'Student'
        when up.profession in ('Educator','教職員','Teacher/Educator') then 'Educator'
        when up.profession in ('Writer','ライター','ライター・編集者・作家') then 'Writer'
        when up.profession in ('Youtuber/Video Maker','ユーチューバー') then 'Youtuber'
        when up.profession in ('Sales','営業','営業職') then 'Sales'
        when up.profession in ('Customer Service','顧客サービス','カスタマーサポート') then 'CS'
        when up.profession in ('Product/Project Manager','プロダクト・プロジェクトマネージャー') then 'PM'
        when up.profession in ('PR/Marketing','企画・マーケティング','Marketing') then 'Marketing'
        when up.profession in ('HR & Legal','人事労務・法律関係','Law','人事総務') then 'HR'
        when up.profession in ('Finance','金融関係') then 'Finance'
        when up.profession in ('Medical/Health','医療関係','医療従事者') then 'Medical'
        when up.profession in ('Coach/Consulting','コンサルティング','Consulting','コンサルタント') then 'Consulting'
        when up.profession in ('情報システム・DX') then 'DX'
        when up.profession in ('エンジニアリング','IT/Engineering','IT','ITエンジニア','エンジニア','Engineering') then 'Engineering'
        else up.profession end AS profession
    ,COALESCE(up.use_case, 'unknown') AS role
    ,u.domain
    ,COALESCE(sd.same_domain_users,0) AS same_domain_users
    ,case when uw.ws_count is null then 0 else 1 end as is_create_ws
	,uw.create_first_ws_time
    ,case
    	when uw.first_ws_type=1 then 'SELF'
    	when uw.first_ws_type=2 then 'TEAM'
    	else 'unknown'
    end as first_ws_type
    ,COALESCE(uw.ws_count,0) AS create_ws_count
    ,case when ujw.joined_ws_plan_type is null then 0 else 1 end as is_joined_other_ws
    ,case
    	when ujw.joined_ws_plan_type is null then null
    	when ujw.joined_ws_plan_type=0 then 'Free'
    	when ujw.joined_ws_plan_type=1 then 'Pro'
    	when ujw.joined_ws_plan_type=2 then 'Biz'
    	when ujw.joined_ws_plan_type=3 then 'Enterprise'
    	else 'unknown'
    end as joined_ws_plan_type
	,case when r.first_transcription_create_time is null then 0 else 1 end as is_create_record
	,timestamp_seconds(cast(r.first_transcription_create_time as int)) as first_transcription_create_time
	,case
		when r.first_transcription_type is null then null
		when r.first_transcription_type=1 then 'File'
		when r.first_transcription_type=2 then 'Real Time'
		when r.first_transcription_type=3 then 'Multilingual Meeting'
		when r.first_transcription_type=4 then 'Meeting'
		when r.first_transcription_type=5 then 'Accurate'
		when r.first_transcription_type=6 then 'Screen'
		when r.first_transcription_type=7 then 'Media Download'
		when r.first_transcription_type=8 then 'Multilingual File Transcribe'
		when r.first_transcription_type=9 then 'Subtitle'
		when r.first_transcription_type=10 then 'Multilingual RealTime Transcribe'
		when r.first_transcription_type=11 then 'Calendar Events Auto Join Meeting'
		when r.first_transcription_type=12 then 'Youtube'
		else 'unknown'
	end as first_transcription_type
    ,COALESCE(r.first_transcription_duration,0) AS first_transcription_duration
    ,r.first_transcription_language
    ,case when p.first_trial_start_time is null then 0 else 1 end as is_trial
    ,p.first_trial_start_time
    ,case
    	when p.first_trial_plan_type is null then null
    	when p.first_trial_plan_type=0 then 'Free'
    	when p.first_trial_plan_type=1 then 'Pro'
    	when p.first_trial_plan_type=2 then 'Biz'
    	when p.first_trial_plan_type=3 then 'Enterprise'
    	else 'unknown'
    end as first_trial_plan_type
    ,case when p.first_pro_trial_create_time is null then 0 else 1 end as is_pro_trial
    ,p.first_pro_trial_create_time
    ,case when p.first_biz_trial_create_time is null then 0 else 1 end as is_biz_trial
    ,p.first_biz_trial_create_time
    ,case when p.first_paid_plan_start_time is null then 0 else 1 end as is_paid
    ,p.first_paid_plan_start_time
    ,p.first_paid_plan_type
	,p.first_pro_pay_create_time
	,p.first_biz_pay_create_time
	,case when ud.first_upsell_create_time is null then 0 else 1 end as is_upsell
	,ud.first_upsell_create_time
	,ud.upsell_type
	,COALESCE(round(p.pay_amount_usd,2),0) as pay_amount_usd
	,COALESCE(p.pay_order_count,0) as pay_order_count
	,case
		when ur.renewal_status is null then null
		when ur.renewal_status=0 then 'Subscribed'
		when ur.renewal_status=1 then 'Subscription Pending'
		when ur.renewal_status=2 then 'Subscription Canceled'
		when ur.renewal_status=3 then 'Subscription Canceled'
		else 'unknown' end as subscription_renewal_status
    ,COALESCE(r.total_record_count,0) AS total_record_count
    ,COALESCE(r.total_record_duration,0) AS total_record_duration
    ,COALESCE(r.import_file_record_count,0) AS import_file_record_count
    ,COALESCE(r.realtime_record_count,0) AS realtime_record_count
    ,COALESCE(r.meeting_record_count,0) AS meeting_record_count
    ,timestamp_seconds(cast(r.last_transcription_create_time as int)) as last_transcription_create_time
    ,COALESCE(r.last_7days_record_count,0) AS last_7days_record_count
    ,COALESCE(r.last_7days_weekend_record_count,0) AS last_7days_weekend_record_count
    ,COALESCE(r.last_30days_record_count,0) AS last_30days_record_count
    ,COALESCE(an.last_30days_ai_notes_count,0) AS last_30days_ai_notes_count
    ,case when date(u.signup_time)>date_add(date(current_timestamp()),interval -30 day) then 'New'
    	when r.last_30days_active_week_count is null or r.last_30days_active_week_count=0 then 'Dormant'
    	when r.last_30days_active_week_count=1 then 'Casual'
    	when r.last_30days_active_week_count in (2,3) then 'Core'
    	when r.last_30days_active_week_count>=4 then 'Power'
    	else null end as user_active_segment
	,COALESCE(iw.workspace_id, cast(uw.first_ws_id as string)) as current_ws_id
	,case
    	when iw.plan_type is null then null
    	when iw.plan_type=0 then 'Free'
    	when iw.plan_type=1 then 'Pro'
    	when iw.plan_type=2 then 'Biz'
    	when iw.plan_type=3 then 'Enterprise'
    	when iw.plan_type=4 then 'Starter'
    	else 'unknown'
    end as current_plan_type
	,iw.current_interest_seats as current_interest_seats
    ,iw.current_interest_start_time
    ,iw.current_interest_flush_time
	,iw.current_interest_import_total as current_interest_import_total
	,iw.current_interest_import_used as current_interest_import_used
	,iw.current_interest_duration_total as current_interest_duration_total
	,iw.current_interest_duration_used as current_interest_duration_used
	,iw.current_interest_ai_summary_used as current_interest_ai_summary_used
	,iwe.current_plan_end_time
	,case when ou.uid is not null then 1 else 0 end as is_offline_user
	,date_add(date(current_timestamp()),interval -1 day) as pt
from user_data u
left join user_source us on u.uid=us.uid
left join user_extra ue on u.uid=ue.uid
left join user_country uc on u.uid= CAST(uc.uid as int64)
left join user_profession up on up.uid=u.uid
left join same_domain_users sd on sd.domain=u.domain
left join user_ws uw on uw.uid=u.uid
left join user_joined_ws ujw on ujw.uid=u.uid
left join record r on r.uid=u.uid
left join ai_notes as an on an.uid=u.uid
left join order_table_user p on p.uid=u.uid
left join user_interest_ws iw on iw.uid=u.uid
left join user_interest_ws_end_time iwe on iwe.uid=iw.uid and iwe.workspace_id=iw.workspace_id
left join user_renewal ur on ur.workspace_id=iw.workspace_id
left join is_upsell_details ud on ud.uid=u.uid
left join offline_user ou on ou.uid=u.uid