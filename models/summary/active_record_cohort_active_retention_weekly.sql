WITH
-- =================================================
-- 1-5. 基础 Cohort 定义 (保持原逻辑)
-- =================================================
user as (
    select
        uid
        ,signup_date
    from dbt_models_details.user_details
    where pt=date_add(current_date(),interval -1 day)
),
week_plans as (
    select
        workspace_id,
        date(timestamp_trunc(calendar_date, ISOWEEK)) as week,
        max(goods_plan_type) as goods_plan_type,
        max_by(period_unit,goods_plan_type) as period_unit,
        max_by(period_count,goods_plan_type) as period_count
    from dbt_models_details.workspace_daily_paid_plan
    where is_trial = 0
    group by 1, 2
),
user_first_active as (
    select
        uid,
        min(record_week) as first_active_week,
        min_by(goods_plan_type,record_week) as first_week_plan_type,
        min_by(signup_country,record_week) as country_group
    from dbt_models_details.stg_active_user_records_daily a
    left join week_plans b on a.workspace_id=b.workspace_id and a.record_week=b.week
    group by 1
),
paid_user_first_active as (
    select
        a.uid,
        min(record_week) as first_paid_active_week,
        min_by(workspace_id,record_week) as workspace_id,
        min_by(signup_country,record_week) as country_group
    from dbt_models_details.stg_active_user_records_daily a
    where plan_type in ('Pro','Biz','Enterprise','Starter')
    group by 1
),
cohort_users as (
    -- Cohort 1: Free用户
    select
        a.uid,
        'Free/Trail Cohort' as cohort_type,
        first_active_week as cohort_week,
        country_group,
        'Free' as cohort_plan
    from user_first_active a
    left join user b on a.uid=b.uid
    where first_active_week >= '2024-01-01'
        and first_week_plan_type is null

    union all

    -- Cohort 2: 付费用户
    select
        a.uid,
        'Paid Cohort' as cohort_type,
        a.first_paid_active_week as cohort_week,
        a.country_group,
        concat(
        case
            when b.goods_plan_type=0 then 'Starter'
            when b.goods_plan_type=1 then 'Pro'
            when b.goods_plan_type=2 then 'Biz'
            when b.goods_plan_type=3 then 'Enterprise'
            else 'else'
        end,'-',
        case when period_unit =2 and period_count=12 then 'annually-1'
          when period_unit =1 and period_count=1 then 'annually-1'
          else concat(case when period_unit=2 then 'monthly' else 'annually' end,'-',period_count)
          end
        ) as cohort_plan
    from paid_user_first_active a
    join user_first_active fa on a.uid = fa.uid
    join week_plans b on a.workspace_id = b.workspace_id and a.first_paid_active_week = b.week
    where
        fa.first_active_week >= '2024-01-01' and a.first_paid_active_week>=fa.first_active_week
),

-- =================================================
-- 6. 用户每周活跃端流水 (保持原逻辑)
-- =================================================
all_activity as (
    select
        uid,
        record_week,
        transcribe_source as device,
        case when safe_divide(sum(record_duration)/60,sum(record_count))>30 then 'Meeting User'
        else 'Non-Meeting User' end as is_meeting_active_user
    from dbt_models_details.stg_active_user_records_daily
    where record_week < date_trunc(current_date(), ISOWEEK)
    group by 1, 2, 3
union all
    select
        uid,
        record_week,
        'Total' as device,
        case when safe_divide(sum(record_duration)/60,sum(record_count))>30 then 'Meeting User'
        else 'Non-Meeting User' end as is_meeting_active_user
    from dbt_models_details.stg_active_user_records_daily
    where record_week < date_trunc(current_date(), ISOWEEK)
    group by 1, 2
),

-- =================================================
-- 7. [新增] 确定用户 Week 0 的属性 (画像定格)
--    我们需要先知道每个用户在初始周是用什么设备、是不是Meeting User
-- =================================================
cohort_user_attributes as (
    select
        c.cohort_type,
        c.cohort_week,
        c.country_group,
        c.cohort_plan,
        c.uid,
        -- 获取 Week 0 的设备和 Meeting 状态，作为该用户后续留存的“分组标签”
        start_active.device,
        start_active.is_meeting_active_user as initial_meeting_status
    from cohort_users c
    -- 强制关联 Week 0 的活跃记录 (确定分母归属)
    join all_activity start_active
        on c.uid = start_active.uid
        and c.cohort_week = start_active.record_week
),

-- =================================================
-- 8. [新增] 计算【固定初始人数】(分母表)
-- =================================================
fixed_initial_stats as (
    select
        cohort_type,
        cohort_week,
        country_group,
        cohort_plan as plan_type,
        device,
        initial_meeting_status as is_meeting_active_user, -- 这里作为维度列
        count(distinct uid) as fixed_initial_users
    from cohort_user_attributes
    group by 1, 2, 3, 4, 5, 6
),

-- =================================================
-- 9. [新增] 计算【实际活跃人数】(分子表)
-- =================================================
retention_activity as (
    select
        c.cohort_type,
        c.cohort_week,
        c.country_group,
        c.cohort_plan as plan_type,
        c.device,
        c.initial_meeting_status as is_meeting_active_user, -- 保持维度一致
        
        date_diff(a.record_week, c.cohort_week, WEEK) as week_diff,
        
        -- 活跃人数 (端对端)
        count(distinct c.uid) as retained_active_users,
        
        -- 在回访周也是 Meeting User 的人数
        count(distinct case when a.is_meeting_active_user = 'Meeting User' then c.uid end) as retained_meeting_users

    from cohort_user_attributes c
    left join all_activity a
        on c.uid = a.uid
        and a.record_week >= c.cohort_week
        and c.device = a.device -- 保证端对端一致
    group by 1, 2, 3, 4, 5, 6, 7
),

-- =================================================
-- 10. [新增] 构建周度骨架 (Skeleton)
-- =================================================
week_series as (
    -- 生成 0 到 52 周的序列
    select * from unnest(generate_array(0, 52)) as week_diff
),
skeleton as (
    select
        i.*, -- 包含所有维度 + fixed_initial_users
        ws.week_diff
    from fixed_initial_stats i
    cross join week_series ws
    -- 过滤掉未来的时间
    where date_add(i.cohort_week, interval ws.week_diff week) <= current_date()
),

-- =================================================
-- 11. [新增] 骨架回填
-- =================================================
final_dataset as (
    select
        s.cohort_type,
        s.cohort_week,
        s.country_group,
        s.plan_type,
        s.device,
        s.is_meeting_active_user, -- 这里的含义是：该用户在 Week 0 时是 Meeting User
        s.week_diff,
        
        -- 1. 初始人数 (来自骨架，绝不丢失)
        s.fixed_initial_users as cohort_total_users,
        
        -- 2. 活跃人数 (关联不上则补0)
        coalesce(r.retained_active_users, 0) as retained_active_users,
        coalesce(r.retained_meeting_users, 0) as retained_meeting_users

    from skeleton s
    left join retention_activity r 
        on s.cohort_type = r.cohort_type
        and s.cohort_week = r.cohort_week
        and s.country_group = r.country_group
        and s.plan_type = r.plan_type
        and s.device = r.device
        and s.is_meeting_active_user = r.is_meeting_active_user
        and s.week_diff = r.week_diff
)

-- =================================================
-- 12. 最终输出
-- =================================================
select
    *,
    -- 补充次周留存 (Lead)
    lead(retained_active_users, 1, 0) over (
        partition by cohort_type, cohort_week, country_group, plan_type, device, is_meeting_active_user
        order by week_diff asc
    ) as next_week_active_users,
    
    lead(retained_meeting_users, 1, 0) over (
        partition by cohort_type, cohort_week, country_group, plan_type, device, is_meeting_active_user
        order by week_diff asc
    ) as next_week_meeting_active_users

from final_dataset
order by cohort_type, cohort_week, device, is_meeting_active_user, week_diff