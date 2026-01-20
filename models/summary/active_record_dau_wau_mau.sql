with user as (

select
	uid
	,signup_date
from dbt_models_details.user_details
where pt=date_add(current_date(),interval -1 day)

),

workspace_info as (
    -- 获取workspace日期及其周/月起点
    select
        workspace_id,
        date(timestamp_millis(cast(create_time as int))) as create_date,
        date_trunc(date(timestamp_millis(cast(create_time as int))),ISOWEEK) as create_week,
        date_trunc(date(timestamp_millis(cast(create_time as int))),MONTH) as create_month
    from `notta-data-analytics.notta_aurora.langogo_user_space_workspace`
    where status!=2 --排除已删除
),

-- 计算用户在周的最高套餐等级
week_plans as (
    select
        workspace_id,
        date(timestamp_trunc(calendar_date, ISOWEEK)) as week,
        max(goods_plan_type) as goods_plan_type,
    from dbt_models_details.workspace_daily_paid_plan
    where is_trial=0 -- 排除试用
    group by
        1,2
),

-- 计算用户在月的最高套餐等级
month_plans as (
    select
        workspace_id,
        date(timestamp_trunc(calendar_date, MONTH)) as month,
        max(goods_plan_type) as goods_plan_type,
    from dbt_models_details.workspace_daily_paid_plan
    where is_trial=0 -- 排除试用
    group by
        1,2
),

base_daily as (
    -- 2. 日粒度预聚合：处理套餐权重与会议活跃判定
    select
        record_date,
        record_week,
        record_month,
        a.uid,
        a.workspace_id,
        case when signup_country in ('Japan','United States','unknown','United Kingdom','France','Canada')  then signup_country else 'Other' end as country_group,
        transcribe_source as device,
        plan_type as daily_plan_name,
	    case when u.uid is null then 'Guest/Deleted/Internal User'
	        when b.goods_plan_type is null then 'Free/Trial'
            when b.goods_plan_type=0 then 'Starter'
            when b.goods_plan_type=1 then 'Pro'
            when b.goods_plan_type=2 then 'Biz'
            when b.goods_plan_type=3 then 'Enterprise'
            else 'else'
            end as week_plan_type,
        case when u.uid is null then 'Guest/Deleted/Internal User'
	        when c.goods_plan_type is null then 'Free/Trial'
            when c.goods_plan_type=0 then 'Starter'
            when c.goods_plan_type=1 then 'Pro'
            when c.goods_plan_type=2 then 'Biz'
            when c.goods_plan_type=3 then 'Enterprise'
            else 'else'
            end as month_plan_type,
        w.create_date,
        w.create_week,
        w.create_month,
        sum(meeting_record_count) as meeting_record_count,
        sum(record_count) as record_count,
        sum(record_duration) as record_duration
    from dbt_models_details.stg_active_user_records_daily a
    inner join workspace_info w on a.workspace_id = w.workspace_id
    left join week_plans b on a.workspace_id=b.workspace_id and a.record_week=b.week
    left join month_plans c on a.workspace_id=c.workspace_id and a.record_month=c.month
    left join user u on a.uid=u.uid
    group by
        1,2,3,4,5,6,7,8,9,10,11,12,13
),

-- 2. 预聚合周粒度：计算周总指标并锁定维度
base_weekly_user_grain as (
    select
        record_week,
        uid,
        workspace_id,
        country_group,
        device,
        week_plan_type as plan,
        case when create_week = record_week then 'New' else 'Old' end as user_tag,
        sum(meeting_record_count) as meeting_record_count,
        sum(record_count) as record_count,
        sum(record_duration) as record_duration
    from base_daily
    group by 1,2,3,4,5,6,7
),

-- 3. 预聚合月粒度：计算月总指标并锁定维度
base_monthly_user_grain as (
    select
        record_month,
        uid,
        workspace_id,
        country_group,
        device,
        month_plan_type as plan,
        case when create_month = record_month then 'New' else 'Old' end as user_tag,
        sum(meeting_record_count) as meeting_record_count,
        sum(record_count) as record_count,
        sum(record_duration) as record_duration
    from base_daily
    group by 1,2,3,4,5,6,7
),

--------------------------------------------------------------------------------
-- 聚合块：Daily / Weekly / Monthly
--------------------------------------------------------------------------------

-- 1. 日度聚合 (DAU)
daily_agg as (
    -- 分端
    select
        'Daily' as time_type,
        b.record_date as report_date,
        case when b.record_date = b.create_date then 'New' else 'Old' end as user_tag,
        b.country_group,
        b.device,
        b.daily_plan_name as plan,
        count(distinct b.uid) as active_users,
        count(distinct b.workspace_id) as active_workspaces,
        count(distinct case when SAFE_DIVIDE(b.record_duration/60,b.record_count) >= 30 then b.uid else null end) as active_meeting_users
    from base_daily b
    group by 1,2,3,4,5,6

    union all

    -- 总端
    select
        'Daily' as time_type,
        b.record_date as report_date,
        case when b.record_date = b.create_date then 'New' else 'Old' end as user_tag,
        b.country_group,
        'Total' as device,
        b.daily_plan_name as plan,
        count(distinct b.uid) as active_users,
        count(distinct b.workspace_id) as active_workspaces,
        count(distinct case when SAFE_DIVIDE(b.record_duration/60,b.record_count) >= 30 then b.uid else null end) as active_meeting_users

    from base_daily b
    group by 1,2,3,4,5,6
),

-- 2. 周度聚合 (WAU)
weekly_agg as (
    -- 分端
    select
        'Weekly' as time_type,
        b.record_week as report_date,
        case when b.create_week = b.record_week then 'New' else 'Old' end as user_tag,
        b.country_group,
        b.device,
        b.week_plan_type as plan,
        count(distinct b.uid) as active_users,
        count(distinct b.workspace_id) as active_workspaces,
        count(distinct case when SAFE_DIVIDE(b.record_duration/60,b.record_count) >= 30 then b.uid else null end) as active_meeting_users

    from base_daily b
    group by 1,2,3,4,5,6

    union all

    -- 总端
    select
        'Weekly' as time_type,
        b.record_week as report_date,
        case when b.create_week = b.record_week then 'New' else 'Old' end as user_tag,
        b.country_group,
        'Total' as device,
        b.week_plan_type as plan,
        count(distinct b.uid) as active_users,
        count(distinct b.workspace_id) as active_workspaces,
        count(distinct case when SAFE_DIVIDE(b.record_duration/60,b.record_count) >= 30 then b.uid else null end) as active_meeting_users

    from base_daily b
    group by 1,2,3,4,5,6
),

-- 3. 月度聚合 (MAU)
monthly_agg as (
    -- 分端
    select
        'Monthly' as time_type,
        b.record_month as report_date,
        case when b.create_month = b.record_month then 'New' else 'Old' end as user_tag,
        b.country_group,
        b.device,
        b.month_plan_type as plan,
        count(distinct b.uid) as active_users,
        count(distinct b.workspace_id) as active_workspaces,
        count(distinct case when SAFE_DIVIDE(b.record_duration/60,b.record_count) >= 30 then b.uid else null end) as active_meeting_users

    from base_daily b
    group by 1,2,3,4,5,6

    union all

    -- 总端
    select
        'Monthly' as time_type,
        b.record_month as report_date,
        case when b.create_month = b.record_month then 'New' else 'Old' end as user_tag,
        b.country_group,
        'Total' as device,
        b.month_plan_type as plan,
        count(distinct b.uid) as active_users,
        count(distinct b.workspace_id) as active_workspaces,
        count(distinct case when SAFE_DIVIDE(b.record_duration/60,b.record_count) >= 30 then b.uid else null end) as active_meeting_users

    from base_daily b
    group by 1,2,3,4,5,6
)

--------------------------------------------------------------------------------
-- 最终合并结果集
--------------------------------------------------------------------------------
select * from daily_agg
union all
select * from weekly_agg
union all
select * from monthly_agg