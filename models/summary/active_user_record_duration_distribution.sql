--分国家、分套餐类型，分日、周、月，有套餐权益的，用户数，用户使用时长

with user_details as(
select
	uid
	,case when signup_country in ('Japan','United States','unknown','United Kingdom','France','Canada') then signup_country else 'Other' end as signup_country
from dbt_models_details.user_details
where pt=date_add(current_date(),interval -1 day)

)

,ws_member as (
-- 取ws的member
select
    CAST(workspace_id AS int) as workspace_id
    ,count(distinct uid) as user_count
from `notta-data-analytics.notta_aurora.langogo_user_space_member`
group by
    workspace_id
)

,ws_interest_plan as (
-- 计算每个starter以上 的ws在不同套餐下，按日、周、月拥有的付费天数
-- 匹配 ws owner的注册国家
-- 匹配 ws 的 member数

    -- 日粒度
    select
        a.workspace_id
        ,calendar_date as date
        ,case
            when goods_plan_type=0 then 'Starter'
            when goods_plan_type=1 then 'Pro'
            when goods_plan_type=2 then 'Biz'
            when goods_plan_type=3 then 'Enterprise'
            else 'else'
        end as plan_type
        ,'date' as segment
        ,signup_country
        ,1 as plan_date_count
        ,user_count as seats_count
    from dbt_models_details.workspace_daily_paid_plan a
    inner join user_details b on a.owner_uid=b.uid
    inner join ws_member c on c.workspace_id=a.workspace_id
    where is_trial = 0
        and calendar_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- 筛选从昨天（含）往前推365天的数据
    union all
    -- 周粒度
    select
        a.workspace_id
        ,date_trunc(calendar_date, isoweek) as date
        ,case
            when goods_plan_type=0 then 'Starter'
            when goods_plan_type=1 then 'Pro'
            when goods_plan_type=2 then 'Biz'
            when goods_plan_type=3 then 'Enterprise'
            else 'else'
        end as plan_type
        ,'week' as segment
        ,signup_country
        ,count(distinct calendar_date) as plan_date_count
        ,max(user_count) as seats_count
    from dbt_models_details.workspace_daily_paid_plan a
    inner join user_details b on a.owner_uid=b.uid
    inner join ws_member c on c.workspace_id=a.workspace_id
    where is_trial = 0
        and calendar_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), ISOWEEK), INTERVAL 52 WEEK)
        and calendar_date < DATE_TRUNC(CURRENT_DATE(), ISOWEEK)  -- 筛选从当前周的周一往前推52个完整周的数据
    group by 1,2,3,4,5
    union all
    -- 月粒度
    select
        a.workspace_id
        ,date_trunc(calendar_date, month) as date
        ,case
            when goods_plan_type=0 then 'Starter'
            when goods_plan_type=1 then 'Pro'
            when goods_plan_type=2 then 'Biz'
            when goods_plan_type=3 then 'Enterprise'
            else 'else'
        end as plan_type
        ,'month' as segment
        ,signup_country
        ,count(distinct calendar_date) as plan_date_count
        ,max(user_count) as seats_count
    from dbt_models_details.workspace_daily_paid_plan a
    inner join user_details b on a.owner_uid=b.uid
    inner join ws_member c on c.workspace_id=a.workspace_id
    where is_trial = 0
        and calendar_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH) -- 筛选从当前月的第一天往前推12个完整月的数据
        and calendar_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
    group by 1,2,3,4,5
)

-- 获取每个ws的创建日期，用于判断新老用户
,ws_min_paid_interest_date as (
    select
        workspace_id
        ,min(calendar_date) as min_paid_interest_date
    from dbt_models_details.workspace_daily_paid_plan
    where is_trial = 0
    group by
        workspace_id
)

-- cte 3: 将用户活跃数据按日、周、月三个粒度进行聚合，与权益数据对齐
,ws_activity_by_segment as (
    -- 日粒度
    select
        record_date as date
        ,'date' as segment
        ,workspace_id
        ,plan_type
        ,signup_country
        ,count(distinct uid) as user_count
        ,sum(record_duration) as total_duration
        ,sum(record_count) as total_records
    from dbt_models_details.stg_active_user_records_daily
    where
        record_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    group by
        1,2,3,4,5
    union all
    -- 周粒度
    select
        date_trunc(record_date, isoweek) as date
        ,'week' as segment
        ,workspace_id
        ,plan_type
        ,signup_country
        ,count(distinct uid) as user_count
        ,sum(record_duration) as total_duration
        ,sum(record_count) as total_records
    from dbt_models_details.stg_active_user_records_daily
    where -- 筛选从当前周的周一往前推52个完整周的数据
        record_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), ISOWEEK), INTERVAL 52 WEEK)
        and record_date < DATE_TRUNC(CURRENT_DATE(), ISOWEEK)
    group by
        1,2,3,4,5
    union all
    -- 月粒度
    select
        date_trunc(record_date, month) as date
        ,'month' as segment
        ,workspace_id
        ,plan_type
        ,signup_country
        ,count(distinct uid) as user_count
        ,sum(record_duration) as total_duration
        ,sum(record_count) as total_records
    from dbt_models_details.stg_active_user_records_daily
    where -- 筛选从当前月的第一天往前推12个完整月的数据
        record_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
        and record_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
    group by
        1,2,3,4,5
)

--每周每月每日，新老用户，不同国家，不同套餐，活跃user数，ws数，时长，
,paid_active_data as (
    select
        a.date
        ,a.segment
        ,a.plan_type
        ,a.signup_country
        ,case
            when DATE_DIFF(a.date, DATE(wm.min_paid_interest_date), DAY) BETWEEN 0 AND 30 then 'New User'
            else 'Old User'
        end as user_type
        ,case
            when a.segment = 'date' then 'Complete Plan'
            when a.segment = 'week' and a.plan_date_count >= 6 then 'Complete Plan'
            when a.segment = 'month' and a.plan_date_count >= 25 then 'Complete Plan'
            else 'Incomplete Plan'
        end as plan_completeness
        ,sum(a.seats_count) as entitled_user_count
        ,count(distinct a.workspace_id) as entitled_ws_count
        ,sum(ua.user_count) as active_user_count
        ,count(distinct ua.workspace_id) as active_ws_count
        ,count(case when ua.workspace_id is not null then 1 else null end) active_ws_check
        ,sum(ua.total_records) as record_count
        ,sum(ua.total_duration) as duration
        ,min(ua.total_duration) as min_duration
        ,max(ua.total_duration) as max_duration
        ,APPROX_QUANTILES(ua.total_duration, 100 IGNORE NULLS)[OFFSET(25)] as q25_duration
        ,APPROX_QUANTILES(ua.total_duration, 100 IGNORE NULLS)[OFFSET(50)] as q50_duration
        ,APPROX_QUANTILES(ua.total_duration, 100 IGNORE NULLS)[OFFSET(75)] as q75_duration
        ,min(ua.total_records) as min_record_count
        ,max(ua.total_records) as max_record_count
        ,APPROX_QUANTILES(ua.total_records, 100 IGNORE NULLS)[OFFSET(25)] as q25_record_count
        ,APPROX_QUANTILES(ua.total_records, 100 IGNORE NULLS)[OFFSET(50)] as q50_record_count
        ,APPROX_QUANTILES(ua.total_records, 100 IGNORE NULLS)[OFFSET(75)] as q75_record_count
    from ws_interest_plan a
    left join ws_activity_by_segment ua
        on a.workspace_id = ua.workspace_id
        and a.date = ua.date
        and a.segment = ua.segment
        and a.plan_type=ua.plan_type
        and a.signup_country=ua.signup_country
    left join ws_min_paid_interest_date as wm on a.workspace_id = wm.workspace_id
    group by 1,2,3,4,5,6
)

,free_active_data as (
    select
        ua.date
        ,ua.segment
        ,ua.plan_type
        ,ua.signup_country
        ,'Free' as user_type
        ,'Free' as plan_completeness
        ,sum(ua.user_count) as entitled_user_count
        ,count(distinct ua.workspace_id) as entitled_ws_count
        ,sum(ua.user_count) as active_user_count
        ,count(distinct ua.workspace_id) as active_ws_count
        ,count(case when ua.workspace_id is not null then 1 else null end) active_ws_check
        ,sum(ua.total_records) as record_count
        ,sum(ua.total_duration) as duration
        ,min(ua.total_duration) as min_duration
        ,max(ua.total_duration) as max_duration
        ,APPROX_QUANTILES(ua.total_duration, 100 IGNORE NULLS)[OFFSET(25)] as q25_duration
        ,APPROX_QUANTILES(ua.total_duration, 100 IGNORE NULLS)[OFFSET(50)] as q50_duration
        ,APPROX_QUANTILES(ua.total_duration, 100 IGNORE NULLS)[OFFSET(75)] as q75_duration
        ,min(ua.total_records) as min_record_count
        ,max(ua.total_records) as max_record_count
        ,APPROX_QUANTILES(ua.total_records, 100 IGNORE NULLS)[OFFSET(25)] as q25_record_count
        ,APPROX_QUANTILES(ua.total_records, 100 IGNORE NULLS)[OFFSET(50)] as q50_record_count
        ,APPROX_QUANTILES(ua.total_records, 100 IGNORE NULLS)[OFFSET(75)] as q75_record_count
    from ws_activity_by_segment ua
    where ua.plan_type='Free'
    group by
        1,2,3,4
)

,combined_table AS (
    SELECT * FROM paid_active_data
    UNION ALL
    SELECT * FROM free_active_data
)


select
    date
    ,segment
    ,plan_type
    ,signup_country
    ,user_type
    ,plan_completeness
    ,coalesce(entitled_user_count, 0) as entitled_user_count
    ,coalesce(entitled_ws_count, 0) as entitled_ws_count
    ,coalesce(active_user_count, 0) as active_user_count
    ,coalesce(active_ws_count, 0) as active_ws_count
    ,coalesce(record_count, 0) as record_count
    ,coalesce(duration/60, 0) as duration
    ,coalesce(min_duration/60, 0) as min_duration
    ,coalesce(max_duration/60, 0) as max_duration
    ,coalesce(q25_duration/60, 0) as q25_duration
    ,coalesce(q50_duration/60, 0) as q50_duration
    ,coalesce(q75_duration/60, 0) as q75_duration
    ,coalesce(min_record_count, 0) as min_record_count
    ,coalesce(max_record_count, 0) as max_record_count
    ,coalesce(q25_record_count, 0) as q25_record_count
    ,coalesce(q50_record_count, 0) as q50_record_count
    ,coalesce(q75_record_count, 0) as q75_record_count
from combined_table
order by
    date desc
    ,segment
    ,plan_type
    ,user_type