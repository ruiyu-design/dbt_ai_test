-- 1. 基础用户表 (保持不变)
with user_base as (
    select
        uid
    from dbt_models_details.user_details
    where pt = date_add(current_date(), interval -1 day)
),

-- 2. 每月套餐字典
month_plans as (
    select
        workspace_id,
        date_trunc(calendar_date, MONTH) as month,
        max(goods_plan_type) as goods_plan_type,
        max_by(period_unit, goods_plan_type) as period_unit,
        max_by(period_count, goods_plan_type) as period_count
    from dbt_models_details.workspace_daily_paid_plan
    where is_trial = 0
    group by 1, 2
),

-- 3. 用户首次活跃月 (用于判定新老)
user_first_active as (
    select
        uid,
        min(record_month) as first_active_month
    from dbt_models_details.stg_active_user_records_daily
    group by 1
),

-- 4. 每月活跃流水 + 用户属性标注
monthly_active_status as (
    select
        a.uid,
        record_month,
        a.transcribe_source as device,
        -- 判定新老用户 (基于月)
        case when record_month = f.first_active_month then 'New' else 'Existing' end as user_type,
        -- 判定当月 Plan
        case when u.uid is null then 'Guest/Deleted/Internal User'
            when p.goods_plan_type is null then 'Free/Trial'
            else concat(
            case
                when p.goods_plan_type = 0 then 'Starter'
                when p.goods_plan_type = 1 then 'Pro'
                when p.goods_plan_type = 2 then 'Biz'
                when p.goods_plan_type = 3 then 'Enterprise'
                else 'Free'
            end, '-',
            case
                when p.period_unit = 2 and p.period_count = 12 then 'annually-1'
                when p.period_unit = 1 and p.period_count = 1 then 'annually-1'
                when p.period_unit = 2 then concat('monthly-', p.period_count)
                when p.period_unit = 1 then concat('annually-', p.period_count)
                else 'Free'
            end
        ) end as current_month_plan,
        a.signup_country as country_group,
        -- 判定是否为会议用户
        case when safe_divide(sum(record_duration)/60,sum(record_count))>30 then 'Meeting User'
        else 'Non-Meeting User' end as is_meeting_active_user,
        sum(record_count) as record_count,
        sum(meeting_record_count) as meeting_record_count,
        sum(record_duration) as record_duration
    from dbt_models_details.stg_active_user_records_daily a
    join user_first_active f on a.uid = f.uid
    left join user_base u on a.uid = u.uid
    left join month_plans p on a.workspace_id = p.workspace_id and a.record_month = p.month
    where a.record_month < date_trunc(current_date(), MONTH)
    group by 1, 2, 3, 4, 5, 6
),

-- 5. 汇总 Total 端逻辑 (将各端活跃上报至 Total)
all_activity_with_total as (
    select * from monthly_active_status
    union all
    select
        uid,
        record_month,
        'Total' as device,
        user_type,
        current_month_plan,
        country_group,
        case when safe_divide(sum(record_duration)/60,sum(record_count))>30 then 'Meeting User'
        else 'Non-Meeting User' end as is_meeting_active_user,
        sum(record_count) as record_count,
        sum(meeting_record_count) as meeting_record_count,
        sum(record_duration) as record_duration
    from monthly_active_status
    group by 1, 2, 3, 4, 5, 6
)

-- 6. 计算次月留存
select
    curr.record_month,
    curr.device,
    curr.user_type,
    curr.current_month_plan as plan_type,
    curr.country_group,
    curr.is_meeting_active_user,
    -- 当月活跃总人数 (分母)
    count(distinct curr.uid) as active_users,
    -- 下月留存人数 (分子)
    count(distinct nxt.uid) as retained_users_next_month
from all_activity_with_total curr
left join all_activity_with_total nxt
    on curr.uid = nxt.uid
    and curr.device = nxt.device  -- 保证同端对同端
    and nxt.record_month = date_add(curr.record_month, interval 1 MONTH)
group by 1, 2, 3, 4, 5, 6
order by 1 desc, 2, 3